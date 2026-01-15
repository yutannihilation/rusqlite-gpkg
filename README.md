# rusqlite-gpkg

[![](https://img.shields.io/github/actions/workflow/status/yutannihilation/rusqlite-gpkg/test.yml?style=flat-square&logo=github)](https://github.com/yutannihilation/rusqlite-gpkg/actions/workflows/test.yml)
[![](https://img.shields.io/crates/v/rusqlite-gpkg.svg?style=flat-square&logo=rust)](https://crates.io/crates/rusqlite-gpkg)
[![](https://img.shields.io/docsrs/rusqlite-gpkg.svg?style=flat-square&logo=docsdotrs)](https://docs.rs/rusqlite-gpkg/latest/)
[![](<https://img.shields.io/badge/%C2%AF%5C_(%E3%83%84)_%2F%C2%AF-green?style=flat-square&logo=docsdotrs&label=docs%20(dev)&labelColor=grey>)](https://yutannihilation.github.io/rusqlite-gpkg/rusqlite_gpkg/)

Small GeoPackage reader/writer built on top of [rusqlite](https://crates.io/crates/rusqlite).

## Overview

`rusqlite-gpkg` provides a small API around the main GeoPackage concepts:

- `Gpkg`: open or create a GeoPackage, list layers, and create new layers.
- `GpkgLayer`: access feature rows, insert geometries with properties, or truncate data.
- `GpkgFeature`: read geometry and properties from a single row.

The library focuses on simple, explicit flows. You control how layers are created
and which property columns are present.

## Short usage

### Gpkg

`Gpkg` represents the GeoPackage connection and is the entry point for almost all
operations. There are multiple ways to open it:

- `Gpkg::open_read_only(path)`: open an existing file without write access.
- `Gpkg::open(path)`: open an existing file for read/write (fails if missing).
- `Gpkg::new(path)`: create a new GeoPackage file (fails if the file exists).
- `Gpkg::new_in_memory()`: create a transient in-memory GeoPackage.

From a `Gpkg`, you can discover or create layers:

- `list_layers()` returns the layer/table names.
- `open_layer(name)` loads a `GpkgLayer` by name.
- `new_layer(...)` creates a new feature layer and returns a `GpkgLayer`.

```rs
use rusqlite_gpkg::Gpkg;

let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
let layers = gpkg.list_layers()?;
let layer = gpkg.open_layer(&layers[0])?;
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

### GpkgLayer

`GpkgLayer` represents a single feature table. You typically get it from
`Gpkg::open_layer` (for existing data) or `Gpkg::new_layer` (for new data).
It exposes the layer schema (geometry column name, property columns) and
methods to iterate, insert, or update features. Insertions and updates accept
any geometry that implements `geo_traits::GeometryTrait<T = f64>`, including
common types from `geo_types` and parsed `wkt::Wkt`.

```rs
use geo_types::Point;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, params};

let gpkg = Gpkg::new("data/new.gpkg")?;
let columns = vec![
    ColumnSpec { name: "name".to_string(), column_type: ColumnType::Varchar },
    ColumnSpec { name: "value".to_string(), column_type: ColumnType::Integer },
];
let layer = gpkg.new_layer(
    "points",
    "geom".to_string(),
    wkb::reader::GeometryType::Point,
    wkb::reader::Dimension::Xy,
    4326,
    &columns,
)?;

layer.insert(Point::new(1.0, 2.0), params!["alpha", 7_i64])?;
let count = layer.features()?.count();
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

### GpkgFeature

`GpkgFeature` represents one row in a layer. You usually obtain it by iterating
`GpkgLayer::features()`. It provides the primary key (`id()`), geometry (`geometry()`),
and property access via `property(name)` returning an owned `Value`. The geometry is returned as a
`wkb::reader::Wkb`, which you can inspect or convert to WKT for display.

```rs
use rusqlite_gpkg::Gpkg;
use wkt::to_wkt::write_geometry;

let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
let layer = gpkg.open_layer("points")?;
let feature = layer.features()?.next().expect("feature");
let id = feature.id();
let geom = feature.geometry()?;
let mut wkt = String::new();
write_geometry(&mut wkt, &geom)?;
let name: String = feature
    .property("name")
    .ok_or("missing name")?
    .try_into()?;
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

## Disclaimer

Most of the implementation is coded by Codex, while the primary idea is based on my own work in <https://github.com/yutannihilation/duckdb-ext-st-read-multi/pulls>. This probably requires more testing against real data; feedback is welcome!

## Prior Work

- https://github.com/cjriley9/gpkg-rs

## Example

### Reader

```rs
use rusqlite_gpkg::{Gpkg, Value};
use wkt::to_wkt::write_geometry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gpkg = Gpkg::open("data.gpkg")?;
    for layer_name in gpkg.list_layers()? {
        let layer = gpkg.open_layer(&layer_name)?;
        for feature in layer.features()? {
            let geom = feature.geometry()?;

            // Use wkt to show the context of the geometry
            let mut wkt = String::new();
            write_geometry(&mut wkt, &geom)?;
            println!("{layer_name}: {wkt}");

            for column in &layer.property_columns {
                let value = feature.property(&column.name).unwrap_or(Value::Null);
                println!("  {} = {:?}", column.name, value);
            }
        }
    }
    Ok(())
}
```

## Value

`Value` is the crate's owned dynamic value used for feature properties. It
mirrors SQLite's dynamic typing (null, integer, real, text, blob) and is
returned by `GpkgFeature::property` as `Option<Value>`. Convert using
`try_into()` or match directly.

```rs
use rusqlite_gpkg::Gpkg;

let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
let layer = gpkg.open_layer("points")?;
let feature = layer.features()?.next().expect("feature");

let name: String = feature.property("name").ok_or("missing name")?.try_into()?;
let active: bool = feature.property("active").ok_or("missing active")?.try_into()?;
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

### Writer

```rs
use geo_types::Point;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, params};

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

    layer.insert(
        Point::new(1.0, 2.0),    // geometry: You can pass whatever object that implements GeometryTrait
        params!["alpha", 7_i64]  // other properties: Use params! macro to create &[&dyn ToSQL]
    )?;

    Ok(())
}
```
