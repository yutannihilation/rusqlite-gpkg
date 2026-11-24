//! Single-file hybrid VFS for wasm.
//!
//! - Writes to the main database file are forwarded to a user-provided writer.
//! - Writes to sidecar files (`-wal`, `-shm`, `-journal`) stay in memory.
//! - This VFS intentionally does not validate filename intent.

use crate::{Gpkg, GpkgError, Result as CrateResult};
use sqlite_wasm_rs::WasmOsCallback;
use sqlite_wasm_rs::vfs::{
    AccessMode, LockLevel, OpenOptions, OpenRequest, OpenedFile, RegisterVfsError, SQLiteIoMethods,
    SQLiteVfs, SyncOptions, VfsError, VfsErrorCode, VfsFile, VfsResult, VfsStore, ffi::sqlite3_vfs,
    register_vfs,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

trait HybridWriter: Write + Seek {}
impl<T: Write + Seek> HybridWriter for T {}

type SharedWriter = Rc<RefCell<WriterState>>;
/// File data shared by every open handle of the same name. It becomes `None`
/// when `HybridVfsHandle::clear_files()` drops the file.
type SharedFile = Rc<RefCell<Option<Box<dyn FileData>>>>;
type SharedFiles = Rc<RefCell<HashMap<String, SharedFile>>>;

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
        register_hybrid_vfs(vfs_name, state, default_vfs)
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
        register_hybrid_vfs(vfs_name, state, default_vfs)?;
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

fn register_hybrid_vfs(
    vfs_name: &str,
    state: HybridState,
    default_vfs: bool,
) -> Result<*mut sqlite3_vfs, RegisterVfsError> {
    // SAFETY: `HybridVfsImpl` and `HybridIoMethods` use the default VFS structure
    // and method table, so their versions, file layout, and app data type
    // (`HybridState`) agree. The app data is never freed (see `into_raw()` below),
    // so it outlives every callback. The `Rc`/`RefCell` in `HybridState` assume
    // SQLite is used from one thread (sqlite-wasm-rs builds it single-threaded).
    let registration =
        unsafe { register_vfs::<HybridIoMethods, HybridVfsImpl>(vfs_name, state, default_vfs) }?;
    // Keep the VFS registered for the lifetime of the process.
    Ok(registration.into_raw())
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
    /// starts from an empty database. If a `Gpkg` from a prior open is still
    /// alive, its file handles fail on their next I/O instead of writing to the
    /// new writer.
    fn clear_files(&self) {
        for (_, file) in self.files.borrow_mut().drain() {
            *file.borrow_mut() = None;
        }
    }

    /// Open a GeoPackage using this registered Hybrid VFS.
    pub fn open_gpkg<P: AsRef<Path>>(&self, sqlite_filename: P) -> CrateResult<Gpkg> {
        Gpkg::open_with_vfs(sqlite_filename, &self.vfs_name)
    }
}

fn shared_file(data: impl FileData + 'static) -> SharedFile {
    Rc::new(RefCell::new(Some(Box::new(data))))
}

fn offset_to_usize(offset: u64, code: VfsErrorCode) -> VfsResult<usize> {
    usize::try_from(offset)
        .map_err(|_| VfsError::new(code, "offset exceeds the address space".into()))
}

/// The contents of one file.
trait FileData {
    /// Copy the bytes available at `offset` into `buf`, zero-fill the rest, and
    /// return the number of bytes copied.
    fn read(&self, buf: &mut [u8], offset: u64) -> VfsResult<usize>;
    fn write(&mut self, buf: &[u8], offset: u64) -> VfsResult<()>;
    fn truncate(&mut self, size: u64) -> VfsResult<()>;
    fn flush(&mut self) -> VfsResult<()>;
    fn size(&self) -> VfsResult<u64>;
}

// Adapted from sqlite-wasm-rs example code:
// https://github.com/Spxg/sqlite-wasm-rs/blob/master/examples/implement-a-vfs/src/lib.rs
#[derive(Default)]
struct MemFile(Vec<u8>);

impl FileData for MemFile {
    fn read(&self, buf: &mut [u8], offset: u64) -> VfsResult<usize> {
        let offset = offset_to_usize(offset, VfsErrorCode::IoRead)?;
        let available = self.0.get(offset..).unwrap_or_default();
        let read_size = available.len().min(buf.len());
        buf[..read_size].copy_from_slice(&available[..read_size]);
        buf[read_size..].fill(0);
        Ok(read_size)
    }

    fn write(&mut self, buf: &[u8], offset: u64) -> VfsResult<()> {
        let offset = offset_to_usize(offset, VfsErrorCode::IoWrite)?;
        let end = offset.saturating_add(buf.len());
        if end > self.0.len() {
            self.0.resize(end, 0);
        }
        self.0[offset..end].copy_from_slice(buf);
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        self.0
            .truncate(offset_to_usize(size, VfsErrorCode::IoTruncate)?);
        Ok(())
    }

    fn flush(&mut self) -> VfsResult<()> {
        Ok(())
    }

    fn size(&self) -> VfsResult<u64> {
        Ok(self.0.len() as u64)
    }
}

/// An in-memory file that also forwards every write to the writer.
struct MainFile {
    mem: MemFile,
    writer: SharedWriter,
}

impl MainFile {
    fn new(writer: SharedWriter) -> Self {
        Self {
            mem: MemFile::default(),
            writer,
        }
    }
}

impl FileData for MainFile {
    fn read(&self, buf: &mut [u8], offset: u64) -> VfsResult<usize> {
        self.mem.read(buf, offset)
    }

    fn write(&mut self, buf: &[u8], offset: u64) -> VfsResult<()> {
        self.mem.write(buf, offset)?;

        self.writer
            .borrow_mut()
            .write_at(buf, offset)
            .map_err(|e| VfsError::new(VfsErrorCode::IoWrite, e.to_string().into()))?;
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        self.mem.truncate(size)
    }

    fn flush(&mut self) -> VfsResult<()> {
        self.writer
            .borrow_mut()
            .flush()
            .map_err(|e| VfsError::new(VfsErrorCode::Io, e.to_string().into()))
    }

    fn size(&self) -> VfsResult<u64> {
        self.mem.size()
    }
}

/// The handle SQLite holds from `xOpen` until `xClose`.
struct HybridFileHandle(SharedFile);

impl HybridFileHandle {
    /// Run `f` on the file data, or fail with `code` if the file was cleared.
    fn with_data<T>(
        &self,
        code: VfsErrorCode,
        f: impl FnOnce(&mut dyn FileData) -> VfsResult<T>,
    ) -> VfsResult<T> {
        match self.0.borrow_mut().as_deref_mut() {
            Some(data) => f(data),
            None => Err(VfsError::new(code, "file was removed from the VFS".into())),
        }
    }
}

impl VfsFile for HybridFileHandle {
    fn read(&mut self, buf: &mut [u8], offset: u64) -> VfsResult<usize> {
        self.with_data(VfsErrorCode::IoRead, |data| data.read(buf, offset))
    }

    fn write(&mut self, buf: &[u8], offset: u64) -> VfsResult<()> {
        self.with_data(VfsErrorCode::IoWrite, |data| data.write(buf, offset))
    }

    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        self.with_data(VfsErrorCode::IoTruncate, |data| data.truncate(size))
    }

    fn sync(&mut self, _options: SyncOptions) -> VfsResult<()> {
        self.with_data(VfsErrorCode::Io, |data| data.flush())
    }

    fn size(&self) -> VfsResult<u64> {
        self.with_data(VfsErrorCode::IoStat, |data| data.size())
    }

    // This VFS does not support file locking.
    fn lock(&mut self, _level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    fn unlock(&mut self, _level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    fn check_reserved_lock(&self) -> VfsResult<bool> {
        Ok(true)
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

impl VfsStore for HybridStore {
    type File = HybridFileHandle;
    type AppData = HybridState;

    fn open_file(
        state: &HybridState,
        request: OpenRequest<'_>,
    ) -> VfsResult<OpenedFile<HybridFileHandle>> {
        let options = request.options;
        let file = match request.filename {
            // An anonymous temporary file is never a part of the output.
            None => shared_file(MemFile::default()),
            Some(filename) => {
                let name = filename.path();
                let mut files = state.files.borrow_mut();
                match files.get(name) {
                    Some(_) if options.exclusive() => {
                        return Err(VfsError::new(
                            VfsErrorCode::CantOpen,
                            format!("file already exists: {name}").into(),
                        ));
                    }
                    Some(file) => file.clone(),
                    None if options.create() => {
                        let file = if is_main_sqlite_file(name) {
                            shared_file(MainFile::new(state.writer.clone()))
                        } else {
                            shared_file(MemFile::default())
                        };
                        files.insert(name.to_string(), file.clone());
                        file
                    }
                    None => {
                        return Err(VfsError::new(
                            VfsErrorCode::CantOpen,
                            format!("file not found: {name}").into(),
                        ));
                    }
                }
            }
        };

        Ok(OpenedFile {
            file: HybridFileHandle(file),
            access: options.access(),
        })
    }

    fn close_file(
        state: &HybridState,
        name: Option<&str>,
        file: HybridFileHandle,
        options: OpenOptions,
    ) -> VfsResult<()> {
        let Some(name) = name else {
            return Ok(());
        };
        if options.delete_on_close() {
            let mut files = state.files.borrow_mut();
            // Do not delete a replacement created under the same name.
            if files
                .get(name)
                .is_some_and(|current| Rc::ptr_eq(current, &file.0))
            {
                files.remove(name);
            }
        }
        Ok(())
    }

    fn access(state: &HybridState, name: &str, _mode: AccessMode) -> VfsResult<bool> {
        Ok(state.files.borrow().contains_key(name))
    }

    fn full_pathname(_state: &HybridState, name: &str) -> VfsResult<String> {
        Ok(name.to_string())
    }

    fn delete_file(state: &HybridState, name: &str, _sync_dir: bool) -> VfsResult<()> {
        if state.files.borrow_mut().remove(name).is_none() {
            return Err(VfsError::new(
                VfsErrorCode::IoDelete,
                format!("{name} not found").into(),
            ));
        }
        Ok(())
    }
}

struct HybridIoMethods;

impl SQLiteIoMethods for HybridIoMethods {
    type Store = HybridStore;
}

struct HybridVfsImpl;

impl SQLiteVfs<HybridIoMethods> for HybridVfsImpl {
    type Os = WasmOsCallback;

    fn os(_state: &HybridState) -> &Self::Os {
        &WasmOsCallback
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
        let read_size = file.read(&mut buf, 1).expect("read should succeed");

        assert_eq!(read_size, 2);
        assert_eq!(buf, [2, 3, 0, 0, 0]);
    }

    #[test]
    fn mem_file_supports_offset_write_and_truncate() {
        let mut file = MemFile::default();
        file.write(&[10, 20], 2).expect("write should succeed");
        assert_eq!(file.size().expect("size should succeed"), 4);

        let mut buf = [0_u8; 4];
        let read_size = file.read(&mut buf, 0).expect("read should succeed");
        assert_eq!(read_size, 4);
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
        for name in ["main.gpkg", "main.gpkg-journal"] {
            state
                .files
                .borrow_mut()
                .insert(name.to_string(), shared_file(MemFile::default()));
        }

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
        let read_size = file.read(&mut buf, 0).expect("read should succeed");
        assert_eq!(read_size, 3);
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
