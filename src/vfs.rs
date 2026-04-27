//! Single-file hybrid VFS for wasm.
//!
//! - Writes to the main database file are forwarded to a user-provided writer.
//! - Writes to sidecar files (`-wal`, `-shm`, `-journal`) stay in memory.
//! - This VFS intentionally does not validate filename intent.

use crate::{Gpkg, GpkgError, Result as CrateResult};
use sqlite_wasm_rs::utils::{
    OsCallback, RegisterVfsError, SQLiteIoMethods, SQLiteVfs, SQLiteVfsFile, VfsError, VfsFile,
    VfsResult, VfsStore,
    ffi::{
        SQLITE_IOERR, SQLITE_IOERR_DELETE, SQLITE_IOERR_READ, SQLITE_IOERR_WRITE, SQLITE_OK,
        sqlite3_file, sqlite3_vfs,
    },
    register_vfs,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

trait HybridWriter: Write + Seek {}
impl<T: Write + Seek> HybridWriter for T {}

type SharedWriter = Rc<RefCell<WriterState>>;
type SharedFiles = Rc<RefCell<HashMap<String, HybridFile>>>;
type HybridAppData = RefCell<HybridState>;

struct WriterState {
    writer: Box<dyn HybridWriter>,
    /// Last known cursor position. `None` means unknown — initial state,
    /// after a writer replacement, or after a failed seek/write that may
    /// have left the cursor at an indeterminate offset.
    pos: Option<u64>,
}

impl WriterState {
    fn new(writer: Box<dyn HybridWriter>) -> Self {
        Self { writer, pos: None }
    }

    /// Write `buf` at `offset`, skipping the seek when the cursor is already
    /// there. SQLite emits long runs of contiguous page writes; each `seek`
    /// otherwise forces `BufWriter::flush_buf` plus, on OPFS, a synchronous
    /// `get_size` JS round-trip.
    fn write_at(&mut self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        if self.pos != Some(offset) {
            self.pos = None;
            self.writer.seek(SeekFrom::Start(offset))?;
            self.pos = Some(offset);
        }
        match self.writer.write_all(buf) {
            Ok(()) => {
                self.pos = Some(offset + buf.len() as u64);
                Ok(())
            }
            Err(e) => {
                self.pos = None;
                Err(e)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    fn replace(&mut self, writer: Box<dyn HybridWriter>) {
        self.writer = writer;
        self.pos = None;
    }
}

thread_local! {
    static DEFAULT_HYBRID_VFS: RefCell<Option<HybridVfsHandle>> = const { RefCell::new(None) };
}

/// Builder that holds the writer used for main database file writes.
pub struct HybridVfsBuilder {
    writer: Box<dyn HybridWriter>,
}

#[derive(Clone)]
pub struct HybridVfsHandle {
    vfs_name: String,
    writer: SharedWriter,
    files: SharedFiles,
}

impl HybridVfsBuilder {
    /// Create a single-file hybrid VFS builder.
    pub fn new<W: Write + Seek + 'static>(writer: W) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }

    /// Register the VFS with sqlite.
    pub fn register(
        self,
        vfs_name: &str,
        default_vfs: bool,
    ) -> Result<*mut sqlite3_vfs, RegisterVfsError> {
        let state = HybridState {
            files: Rc::new(RefCell::new(HashMap::new())),
            writer: Rc::new(RefCell::new(WriterState::new(self.writer))),
        };
        register_vfs::<HybridIoMethods, HybridVfsImpl>(vfs_name, RefCell::new(state), default_vfs)
    }

    /// Register a reusable Hybrid VFS and return a handle that can replace writers.
    pub fn register_reusable(
        self,
        vfs_name: &str,
        default_vfs: bool,
    ) -> Result<HybridVfsHandle, RegisterVfsError> {
        let writer: SharedWriter = Rc::new(RefCell::new(WriterState::new(self.writer)));
        let files: SharedFiles = Rc::new(RefCell::new(HashMap::new()));
        let state = HybridState {
            files: files.clone(),
            writer: writer.clone(),
        };
        register_vfs::<HybridIoMethods, HybridVfsImpl>(vfs_name, RefCell::new(state), default_vfs)?;
        Ok(HybridVfsHandle {
            vfs_name: vfs_name.to_string(),
            writer,
            files,
        })
    }

    /// Convenience helper for wasm: register/reuse a default hybrid VFS and open a GeoPackage.
    ///
    /// On first use, this registers a process-local default VFS. On subsequent calls,
    /// it reuses the same registration, replaces the writer, and clears the in-memory
    /// file map so SQLite sees a fresh database. Any `Gpkg` instances from a previous
    /// call must be dropped before calling this again.
    pub fn open_gpkg<P: AsRef<Path>>(self, sqlite_filename: P) -> CrateResult<Gpkg> {
        let writer = self.writer;
        let handle = DEFAULT_HYBRID_VFS.with(|slot| -> CrateResult<HybridVfsHandle> {
            let mut slot = slot.borrow_mut();
            if let Some(handle) = slot.as_ref() {
                handle.replace_boxed_writer(writer);
                handle.clear_files();
                return Ok(handle.clone());
            }

            let vfs = HybridVfsBuilder { writer }
                .register_reusable("hybrid-opfs-default", false)
                .map_err(|e| GpkgError::Vfs(format!("{e}")))?;
            *slot = Some(vfs.clone());
            Ok(vfs)
        })?;

        handle.open_gpkg(sqlite_filename)
    }
}

impl HybridVfsHandle {
    /// Replace the writer used for main database file writes.
    pub fn replace_writer<W: Write + Seek + 'static>(&self, writer: W) {
        self.replace_boxed_writer(Box::new(writer));
    }

    fn replace_boxed_writer(&self, writer: Box<dyn HybridWriter>) {
        self.writer.borrow_mut().replace(writer);
    }

    /// Drop every in-memory file tracked by this VFS so the next `open_gpkg`
    /// starts from an empty database. Calling this while a `Gpkg` from a prior
    /// open is still alive will leave that connection with dangling references.
    fn clear_files(&self) {
        self.files.borrow_mut().clear();
    }

    /// Open a GeoPackage using this registered Hybrid VFS.
    pub fn open_gpkg<P: AsRef<Path>>(&self, sqlite_filename: P) -> CrateResult<Gpkg> {
        Gpkg::open_with_vfs(sqlite_filename, &self.vfs_name)
    }
}

// Adapted from sqlite-wasm-rs example code:
// https://github.com/Spxg/sqlite-wasm-rs/blob/master/examples/implement-a-vfs/src/lib.rs
#[derive(Default)]
struct MemFile(Vec<u8>);

impl VfsFile for MemFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        let end = offset.saturating_add(buf.len());
        if self.0.len() <= offset {
            buf.fill(0);
            return Ok(false);
        }

        let read_end = end.min(self.0.len());
        let read_size = read_end - offset;
        buf[..read_size].copy_from_slice(&self.0[offset..read_end]);
        if read_size < buf.len() {
            buf[read_size..].fill(0);
            return Ok(false);
        }
        Ok(true)
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        let end = offset.saturating_add(buf.len());
        if end > self.0.len() {
            self.0.resize(end, 0);
        }
        self.0[offset..end].copy_from_slice(buf);
        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.0.truncate(size);
        Ok(())
    }

    fn flush(&mut self) -> VfsResult<()> {
        Ok(())
    }

    fn size(&self) -> VfsResult<usize> {
        Ok(self.0.len())
    }
}

struct MainFile {
    data: Vec<u8>,
    writer: SharedWriter,
}

impl MainFile {
    fn new(writer: SharedWriter) -> Self {
        Self {
            data: Vec::new(),
            writer,
        }
    }
}

impl VfsFile for MainFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        let end = offset.saturating_add(buf.len());
        if self.data.len() <= offset {
            buf.fill(0);
            return Ok(false);
        }

        let read_end = end.min(self.data.len());
        let read_size = read_end - offset;
        buf[..read_size].copy_from_slice(&self.data[offset..read_end]);
        if read_size < buf.len() {
            buf[read_size..].fill(0);
            return Ok(false);
        }
        Ok(true)
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        let end = offset.saturating_add(buf.len());
        if end > self.data.len() {
            self.data.resize(end, 0);
        }
        self.data[offset..end].copy_from_slice(buf);

        self.writer
            .borrow_mut()
            .write_at(buf, offset as u64)
            .map_err(|e| VfsError::new(SQLITE_IOERR_WRITE, e.to_string()))?;
        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.data.truncate(size);
        Ok(())
    }

    fn flush(&mut self) -> VfsResult<()> {
        self.writer
            .borrow_mut()
            .flush()
            .map_err(|e| VfsError::new(SQLITE_IOERR, e.to_string()))
    }

    fn size(&self) -> VfsResult<usize> {
        Ok(self.data.len())
    }
}

enum HybridFile {
    Main(MainFile),
    Mem(MemFile),
}

impl VfsFile for HybridFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        match self {
            HybridFile::Main(file) => file.read(buf, offset),
            HybridFile::Mem(file) => file.read(buf, offset),
        }
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        match self {
            HybridFile::Main(file) => file.write(buf, offset),
            HybridFile::Mem(file) => file.write(buf, offset),
        }
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        match self {
            HybridFile::Main(file) => file.truncate(size),
            HybridFile::Mem(file) => file.truncate(size),
        }
    }

    fn flush(&mut self) -> VfsResult<()> {
        match self {
            HybridFile::Main(file) => file.flush(),
            HybridFile::Mem(file) => file.flush(),
        }
    }

    fn size(&self) -> VfsResult<usize> {
        match self {
            HybridFile::Main(file) => file.size(),
            HybridFile::Mem(file) => file.size(),
        }
    }
}

struct HybridState {
    files: SharedFiles,
    writer: SharedWriter,
}

fn is_main_sqlite_file(name: &str) -> bool {
    !name.ends_with("-wal") && !name.ends_with("-shm") && !name.ends_with("-journal")
}

struct HybridStore;

impl VfsStore<HybridFile, HybridAppData> for HybridStore {
    fn add_file(vfs: *mut sqlite3_vfs, file: &str, _flags: i32) -> VfsResult<()> {
        let app_data = unsafe { Self::app_data(vfs) };
        let state = app_data.borrow();
        let item = if is_main_sqlite_file(file) {
            HybridFile::Main(MainFile::new(state.writer.clone()))
        } else {
            HybridFile::Mem(MemFile::default())
        };
        state.files.borrow_mut().insert(file.to_string(), item);
        Ok(())
    }

    fn contains_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<bool> {
        let app_data = unsafe { Self::app_data(vfs) };
        let state = app_data.borrow();
        Ok(state.files.borrow().contains_key(file))
    }

    fn delete_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<()> {
        let app_data = unsafe { Self::app_data(vfs) };
        let state = app_data.borrow();
        if state.files.borrow_mut().remove(file).is_none() {
            return Err(VfsError::new(
                SQLITE_IOERR_DELETE,
                format!("{file} not found"),
            ));
        }
        Ok(())
    }

    fn with_file<F: Fn(&HybridFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let app_data = unsafe { Self::app_data(vfs_file.vfs) };
        let state = app_data.borrow();
        let files = state.files.borrow();
        let name = unsafe { vfs_file.name() };
        match files.get(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(
                SQLITE_IOERR_READ,
                format!("{name} not found"),
            )),
        }
    }

    fn with_file_mut<F: Fn(&mut HybridFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let app_data = unsafe { Self::app_data(vfs_file.vfs) };
        let state = app_data.borrow();
        let mut files = state.files.borrow_mut();
        let name = unsafe { vfs_file.name() };
        match files.get_mut(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(
                SQLITE_IOERR_WRITE,
                format!("{name} not found"),
            )),
        }
    }
}

struct HybridIoMethods;

impl SQLiteIoMethods for HybridIoMethods {
    type File = HybridFile;
    type AppData = HybridAppData;
    type Store = HybridStore;

    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xCheckReservedLock(
        _p_file: *mut sqlite3_file,
        p_res_out: *mut ::std::os::raw::c_int,
    ) -> ::std::os::raw::c_int {
        if !p_res_out.is_null() {
            unsafe {
                *p_res_out = 1;
            }
        }
        SQLITE_OK
    }
}

struct HybridVfsImpl;

impl SQLiteVfs<HybridIoMethods> for HybridVfsImpl {
    const VERSION: ::std::os::raw::c_int = 1;

    fn sleep(dur: Duration) {
        sqlite_wasm_rs::WasmOsCallback::sleep(dur);
    }

    fn random(buf: &mut [u8]) {
        sqlite_wasm_rs::WasmOsCallback::random(buf);
    }

    fn epoch_timestamp_in_ms() -> i64 {
        sqlite_wasm_rs::WasmOsCallback::epoch_timestamp_in_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor, Seek, Write};

    /// Test sink that delegates `Write + Seek` to a shared `Cursor<Vec<u8>>`,
    /// so the test can keep a clone to inspect the resulting bytes after the
    /// `Box<dyn HybridWriter>` has swallowed the concrete type.
    struct SharedCursor(Rc<RefCell<Cursor<Vec<u8>>>>);

    impl Write for SharedCursor {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.borrow_mut().write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.borrow_mut().flush()
        }
    }

    impl Seek for SharedCursor {
        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            self.0.borrow_mut().seek(pos)
        }
    }

    #[test]
    fn identifies_main_sqlite_file_by_suffix() {
        assert!(is_main_sqlite_file("data.sqlite"));
        assert!(!is_main_sqlite_file("data.sqlite-wal"));
        assert!(is_main_sqlite_file("data.gpkg"));
    }

    #[test]
    fn mem_file_read_pads_with_zero_when_beyond_end() {
        let mut file = MemFile::default();
        file.write(&[1, 2, 3], 0).expect("write should succeed");

        let mut buf = [9_u8; 5];
        let complete = file.read(&mut buf, 1).expect("read should succeed");

        assert!(!complete);
        assert_eq!(buf, [2, 3, 0, 0, 0]);
    }

    #[test]
    fn mem_file_supports_offset_write_and_truncate() {
        let mut file = MemFile::default();
        file.write(&[10, 20], 2).expect("write should succeed");
        assert_eq!(file.size().expect("size should succeed"), 4);

        let mut buf = [0_u8; 4];
        let complete = file.read(&mut buf, 0).expect("read should succeed");
        assert!(complete);
        assert_eq!(buf, [0, 0, 10, 20]);

        file.truncate(3).expect("truncate should succeed");
        assert_eq!(file.size().expect("size should succeed"), 3);
    }

    #[test]
    fn handle_clear_files_drops_entries_visible_to_state() {
        let writer: SharedWriter = Rc::new(RefCell::new(WriterState::new(Box::new(Cursor::new(
            Vec::<u8>::new(),
        )))));
        let files: SharedFiles = Rc::new(RefCell::new(HashMap::new()));
        let state = HybridState {
            files: files.clone(),
            writer: writer.clone(),
        };
        state
            .files
            .borrow_mut()
            .insert("main.gpkg".to_string(), HybridFile::Mem(MemFile::default()));
        state.files.borrow_mut().insert(
            "main.gpkg-journal".to_string(),
            HybridFile::Mem(MemFile::default()),
        );

        let handle = HybridVfsHandle {
            vfs_name: "test".to_string(),
            writer,
            files,
        };

        handle.clear_files();

        assert!(state.files.borrow().is_empty());
    }

    #[test]
    fn main_file_writes_forward_to_writer_at_offset() {
        let cursor = Rc::new(RefCell::new(Cursor::new(Vec::<u8>::new())));
        let writer: SharedWriter = Rc::new(RefCell::new(WriterState::new(Box::new(SharedCursor(
            cursor.clone(),
        )))));
        let mut file = MainFile::new(writer);

        // The second write lands at offset 1, not appended at offset 3.
        file.write(&[1, 2, 3], 0).expect("write should succeed");
        file.write(&[9], 1).expect("write should succeed");
        file.flush().expect("flush should succeed");

        let mut buf = [0_u8; 4];
        let complete = file.read(&mut buf, 0).expect("read should succeed");
        assert!(!complete);
        assert_eq!(buf, [1, 9, 3, 0]);
        assert_eq!(file.size().expect("size should succeed"), 3);
        assert_eq!(cursor.borrow().get_ref().as_slice(), &[1, 9, 3]);
    }

    struct CountingSeek {
        inner: Cursor<Vec<u8>>,
        seek_count: Rc<RefCell<usize>>,
    }

    impl Write for CountingSeek {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    impl Seek for CountingSeek {
        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            *self.seek_count.borrow_mut() += 1;
            self.inner.seek(pos)
        }
    }

    #[test]
    fn main_file_skips_seek_for_sequential_writes() {
        let seek_count = Rc::new(RefCell::new(0_usize));
        let writer: SharedWriter =
            Rc::new(RefCell::new(WriterState::new(Box::new(CountingSeek {
                inner: Cursor::new(Vec::<u8>::new()),
                seek_count: seek_count.clone(),
            }))));
        let mut file = MainFile::new(writer);

        file.write(&[1, 2, 3], 0).expect("first write");
        assert_eq!(*seek_count.borrow(), 1);

        // Sequential write at offset 3 should reuse the cursor position.
        file.write(&[4, 5, 6], 3).expect("sequential write");
        assert_eq!(*seek_count.borrow(), 1);

        // Non-sequential write must seek again.
        file.write(&[9], 0).expect("backward write");
        assert_eq!(*seek_count.borrow(), 2);
    }
}
