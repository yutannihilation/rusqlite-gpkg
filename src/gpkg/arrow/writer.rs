pub struct ArrowGpkgWriter<'a> {
    pub(super) stmt: rusqlite::Statement<'a>,
}
