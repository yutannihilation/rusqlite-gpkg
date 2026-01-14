use rusqlite_gpkg::read_gpkg;

fn main() {
    if let Err(err) = read_gpkg("src/test/test.gpkg") {
        eprintln!("read_gpkg failed: {err}");
        std::process::exit(1);
    }
}
