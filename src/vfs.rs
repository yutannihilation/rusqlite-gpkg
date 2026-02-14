//! Single-file hybrid VFS for wasm.
//!
//! - Writes to files ending with `.sqlite` are forwarded to a user-provided writer.
//! - Writes to all other files (for example `-wal`, `-shm`) stay in memory.
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
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

type SharedWriter = Rc<RefCell<Box<dyn Write>>>;
type HybridAppData = RefCell<HybridState>;

thread_local! {
    static DEFAULT_HYBRID_VFS: RefCell<Option<HybridVfsHandle>> = const { RefCell::new(None) };
}

/// Builder that holds the writer used for main `.sqlite` file writes.
pub struct HybridVfsBuilder {
    writer: Box<dyn Write>,
}

#[derive(Clone)]
pub struct HybridVfsHandle {
    vfs_name: String,
    writer: SharedWriter,
}

impl HybridVfsBuilder {
    /// Create a single-file hybrid VFS builder.
    pub fn new<W: Write + 'static>(writer: W) -> Self {
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
            files: HashMap::new(),
            writer: Rc::new(RefCell::new(self.writer)),
        };
        register_vfs::<HybridIoMethods, HybridVfsImpl>(vfs_name, RefCell::new(state), default_vfs)
    }

    /// Register a reusable Hybrid VFS and return a handle that can replace writers.
    pub fn register_reusable(
        self,
        vfs_name: &str,
        default_vfs: bool,
    ) -> Result<HybridVfsHandle, RegisterVfsError> {
        let writer: SharedWriter = Rc::new(RefCell::new(self.writer));
        let state = HybridState {
            files: HashMap::new(),
            writer: writer.clone(),
        };
        register_vfs::<HybridIoMethods, HybridVfsImpl>(vfs_name, RefCell::new(state), default_vfs)?;
        Ok(HybridVfsHandle {
            vfs_name: vfs_name.to_string(),
            writer,
        })
    }

    /// Convenience helper for wasm: register/reuse a default hybrid VFS and open a GeoPackage.
    ///
    /// On first use, this registers a process-local default VFS. On subsequent calls,
    /// it reuses the same registration and only replaces the writer.
    ///
    /// `sqlite_filename` must end with `.sqlite` so main DB writes are routed to
    /// the provided writer.
    pub fn open_gpkg<P: AsRef<Path>>(self, sqlite_filename: P) -> CrateResult<Gpkg> {
        let writer = self.writer;
        let handle = DEFAULT_HYBRID_VFS.with(|slot| -> CrateResult<HybridVfsHandle> {
            let mut slot = slot.borrow_mut();
            if let Some(handle) = slot.as_ref() {
                handle.replace_boxed_writer(writer);
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
    /// Replace the writer used for main `.sqlite` file writes.
    pub fn replace_writer<W: Write + 'static>(&self, writer: W) {
        self.replace_boxed_writer(Box::new(writer));
    }

    fn replace_boxed_writer(&self, writer: Box<dyn Write>) {
        *self.writer.borrow_mut() = writer;
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
            .write_all(buf)
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
    files: HashMap<String, HybridFile>,
    writer: SharedWriter,
}

fn is_main_sqlite_file(name: &str) -> bool {
    name.ends_with(".sqlite")
}

struct HybridStore;

impl VfsStore<HybridFile, HybridAppData> for HybridStore {
    fn add_file(vfs: *mut sqlite3_vfs, file: &str, _flags: i32) -> VfsResult<()> {
        let app_data = unsafe { Self::app_data(vfs) };
        let mut state = app_data.borrow_mut();
        let item = if is_main_sqlite_file(file) {
            HybridFile::Main(MainFile::new(state.writer.clone()))
        } else {
            HybridFile::Mem(MemFile::default())
        };
        state.files.insert(file.to_string(), item);
        Ok(())
    }

    fn contains_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<bool> {
        let app_data = unsafe { Self::app_data(vfs) };
        let state = app_data.borrow();
        Ok(state.files.contains_key(file))
    }

    fn delete_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<()> {
        let app_data = unsafe { Self::app_data(vfs) };
        let mut state = app_data.borrow_mut();
        if state.files.remove(file).is_none() {
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
        let name = unsafe { vfs_file.name() };
        match state.files.get(name) {
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
        let mut state = app_data.borrow_mut();
        let name = unsafe { vfs_file.name() };
        match state.files.get_mut(name) {
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
    use std::io::{self, Write};

    #[derive(Default, Clone)]
    struct RecordingState {
        writes: Vec<u8>,
        flush_count: usize,
    }

    struct RecordingWriter {
        state: Rc<RefCell<RecordingState>>,
    }

    impl RecordingWriter {
        fn new(state: Rc<RefCell<RecordingState>>) -> Self {
            Self { state }
        }
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.state.borrow_mut().writes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.state.borrow_mut().flush_count += 1;
            Ok(())
        }
    }

    #[test]
    fn identifies_main_sqlite_file_by_suffix() {
        assert!(is_main_sqlite_file("data.sqlite"));
        assert!(!is_main_sqlite_file("data.sqlite-wal"));
        assert!(!is_main_sqlite_file("data.gpkg"));
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
    fn main_file_writes_forward_to_writer_and_flushes() {
        let state = Rc::new(RefCell::new(RecordingState::default()));
        let writer: SharedWriter =
            Rc::new(RefCell::new(Box::new(RecordingWriter::new(state.clone()))));
        let mut file = MainFile::new(writer.clone());

        file.write(&[1, 2, 3], 0).expect("write should succeed");
        file.write(&[9], 1).expect("write should succeed");
        file.flush().expect("flush should succeed");

        let mut buf = [0_u8; 4];
        let complete = file.read(&mut buf, 0).expect("read should succeed");
        assert!(!complete);
        assert_eq!(buf, [1, 9, 3, 0]);
        assert_eq!(file.size().expect("size should succeed"), 3);

        let state = state.borrow();
        assert_eq!(state.writes, vec![1, 2, 3, 9]);
        assert_eq!(state.flush_count, 1);
    }
}
