use wasm_bindgen::JsValue;
use web_sys::FileSystemReadWriteOptions;

/// Adapter from OPFS sync handle to Rust `Read`/`Write`/`Seek`.
///
/// This is used as the sink/source for HybridVfs in browser workers.
pub struct OpfsFile {
    file: web_sys::FileSystemSyncAccessHandle,
    // Browser API stores offset inside a JS options object (`{ at: ... }`).
    offset: FileSystemReadWriteOptions,
}

// FileSystemSyncAccessHandle is not Send in a strict sense, but this helps
// with some writer wrappers as long as execution stays on one thread.
unsafe impl std::marker::Send for OpfsFile {}

impl OpfsFile {
    pub fn new(file: web_sys::FileSystemSyncAccessHandle) -> Result<Self, String> {
        // This demo always overwrites output from scratch.
        file.truncate_with_u32(0).map_err(|e| format!("{e:?}"))?;

        Ok(Self {
            file,
            offset: FileSystemReadWriteOptions::new(),
        })
    }
}

impl std::io::Write for OpfsFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Write at current logical offset and then advance it.
        let size = self
            .file
            .write_with_u8_array_and_options(buf, &self.offset)
            .map_err(convert_js_error_to_io_error)? as u64;

        self.offset
            .set_at(self.offset.get_at().unwrap_or(0.0) + size as f64);

        Ok(size as usize)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush().map_err(convert_js_error_to_io_error)?;
        Ok(())
    }
}

impl std::io::Read for OpfsFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Read at current logical offset and then advance it.
        let size = self
            .file
            .read_with_u8_array_and_options(buf, &self.offset)
            .map_err(convert_js_error_to_io_error)? as u64;

        self.offset
            .set_at(self.offset.get_at().unwrap_or(0.0) + size as f64);

        Ok(size as usize)
    }
}

impl std::io::Seek for OpfsFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let size = self.file.get_size().map_err(convert_js_error_to_io_error)? as i64;
        let new_offset = match pos {
            std::io::SeekFrom::Start(offset) => offset as i64,
            std::io::SeekFrom::End(offset) => size - offset,
            std::io::SeekFrom::Current(offset) => {
                self.offset.get_at().unwrap_or(0.0) as i64 + offset
            }
        };

        if new_offset < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid offset",
            ));
        }

        // Clamp to file size to keep behavior predictable in this demo.
        let new_offset = std::cmp::min(new_offset, size) as u64;
        self.offset.set_at(new_offset as f64);

        Ok(new_offset)
    }
}

impl Drop for OpfsFile {
    fn drop(&mut self) {
        // Safe to call repeatedly from JS+Rust boundaries; JS may already close.
        self.file.close();
    }
}

fn convert_js_error_to_io_error(e: JsValue) -> std::io::Error {
    std::io::Error::other(format!(
        "Some error happened on JS API: {}",
        e.as_string().unwrap_or("<undisplayable>".to_string())
    ))
}
