# rusqlite-gpkg

[![](https://img.shields.io/github/actions/workflow/status/yutannihilation/rusqlite-gpkg/test.yml?style=flat-square&logo=github)](https://github.com/yutannihilation/rusqlite-gpkg/actions/workflows/test.yml)
[![](https://img.shields.io/crates/v/rusqlite-gpkg.svg?style=flat-square&logo=rust)](https://crates.io/crates/rusqlite-gpkg)
[![](https://img.shields.io/docsrs/rusqlite-gpkg.svg?style=flat-square&logo=docsdotrs)](https://docs.rs/rusqlite-gpkg/latest/)
[![](<https://img.shields.io/badge/%C2%AF%5C_(%E3%83%84)_%2F%C2%AF-green?style=flat-square&logo=docsdotrs&label=docs%20(dev)&labelColor=grey>)](https://yutannihilation.github.io/rusqlite-gpkg/rusqlite_gpkg/)

Small GeoPackage reader/writer built on top of [rusqlite](https://crates.io/crates/rusqlite).

## Disclaimer

Most of the implementation is coded by Codex, while the primary idea is based on my own work in <https://github.com/yutannihilation/duckdb-ext-st-read-multi/pulls>. This probably requires more testing against real data; feedback is welcome!

## Example

### Reader

```rs
use rusqlite_gpkg::{Gpkg, Value};
use wkt::to_wkt::write_geometry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gpkg = Gpkg::open("data.gpkg")?;
    for layer_name in gpkg.list_layers()? {
        let layer = gpkg.layer(&layer_name)?;
        for feature in layer.features()? {
            let geom = feature.geometry()?;

            // Use wkt to show the context of the geometry
            let mut wkt = String::new();
            write_geometry(&mut wkt, &geom)?;
            println!("{layer_name}: {wkt}");

            for (idx, column) in layer.property_columns().iter().enumerate() {
                let value: Value = feature.property(idx)?;
                println!("  {} = {:?}", column.name, value);
            }
        }
    }
    Ok(())
}
```

### Writer

```rs
use geo_types::Point;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gpkg = Gpkg::new("data.gpkg")?;

    let columns = vec![
        ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        },
        ColumnSpec {
            name: "value".to_string(),
            column_type: ColumnType::Integer,
        },
    ];

    let layer = gpkg.new_layer(
        "points",
        "geom".to_string(),
        wkb::reader::GeometryType::Point,
        wkb::reader::Dimension::Xy,
        4326,
        &columns,
    )?;

     // You can pass whatever object that implements GeometryTrait
    layer.insert(
        Point::new(1.0, 2.0),
        vec![Value::Text("alpha".to_string()), Value::Integer(7)],
    )?;

    Ok(())
}
```
