# rusqlite-gpkg

[![](https://img.shields.io/github/actions/workflow/status/yutannihilation/rusqlite-gpkg/test.yml?style=flat-square&logo=github)](https://github.com/yutannihilation/rusqlite-gpkg/actions/workflows/test.yml)
[![](https://img.shields.io/crates/v/rusqlite-gpkg.svg?style=flat-square&logo=rust)](https://crates.io/crates/rusqlite-gpkg)
[![](https://img.shields.io/docsrs/rusqlite-gpkg.svg?style=flat-square&logo=docsdotrs)](https://docs.rs/rusqlite-gpkg/latest/)
[![](https://img.shields.io/badge/%C2%AF%5C_(%E3%83%84)_%2F%C2%AF-green?style=flat-square&logo=docsdotrs&label=docs%20(dev)&labelColor=grey)](https://yutannihilation.github.io/rusqlite-gpkg/rusqlite_gpkg/)

Small GeoPackage reader built on top of [rusqlite](https://crates.io/crates/rusqlite).

## Example

```rs
use rusqlite_gpkg::Gpkg;
use wkt::to_wkt::write_geometry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gpkg = Gpkg::open("data.gpkg")?;
    for layer_name in gpkg.list_layers()? {
        let layer = gpkg.layer(&layer_name)?;
        for feature in layer.features()? {
            let geom = feature.geometry()?;
            let mut wkt = String::new();
            write_geometry(&mut wkt, &geom)?;
            println!("{layer_name}: {wkt}");
        }
    }
    Ok(())
}
```
