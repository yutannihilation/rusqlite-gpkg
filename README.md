# rusqlite-gpkg

[![](https://img.shields.io/github/actions/workflow/status/yutannihilation/rusqlite-gpkg/test.yml?style=flat-square&logo=github)](https://github.com/yutannihilation/rusqlite-gpkg/actions/workflows/test.yml)
[![](https://img.shields.io/crates/v/rusqlite-gpkg.svg?style=flat-square&logo=rust)](https://crates.io/crates/rusqlite-gpkg)
[![](https://img.shields.io/docsrs/rusqlite-gpkg.svg?style=flat-square&logo=docsdotrs)](https://docs.rs/rusqlite-gpkg/latest/)

GeoPackage reader/writer built on top of [rusqlite](https://crates.io/crates/rusqlite).

## Web Demo

A simple GitHub Pages demo is available with a button to generate
and download a `.gpkg` file in the browser using a Web Worker + OPFS.

https://yutannihilation.github.io/rusqlite-gpkg/

See `web/README.md` for implementation details and design notes.

## Overview

`rusqlite-gpkg` provides a small API around the main GeoPackage concepts:

- `Gpkg` represents the whole data of GeoPackage data.
- `GpkgLayer` represents a single layer in the data.
- `GpkgFeature` represents a single feature in the layer.
- `Value` represents a single property value related to the feature.

Apache Arrow support is available behind the `arrow` feature flag.
You can find some example codes in the bottom of this README.

The library focuses on simple, explicit flows. You control how layers are created
and which property columns are present.

### Browser usage (to_bytes / from_bytes)

Web environments often cannot access files directly (OPFS can be used by
`rusqlite`, but this crate does not currently expose a way to enable it). In
those cases, the recommended workflow is to serialize a GeoPackage to bytes.

```rs
use rusqlite_gpkg::Gpkg;

let gpkg = Gpkg::open_in_memory()?;
// ... write layers/features ...
let bytes = gpkg.to_bytes()?; // store bytes in IndexedDB, send over network, etc.

let restored = Gpkg::from_bytes(&bytes)?;
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

### Gpkg

`Gpkg` represents the GeoPackage connection and is the entry point for almost all
operations. There are multiple ways to open it:

- `Gpkg::open_read_only(path)`: open an existing file without write access.
- `Gpkg::open(path)`: open a new or existing file for read/write.
- `Gpkg::open_in_memory()`: create a transient in-memory GeoPackage.

From a `Gpkg`, you can discover or create layers:

- `list_layers()` returns the layer/table names.
- `get_layer(name)` loads a `GpkgLayer` by name.
- `create_layer(...)` creates a new feature layer and returns a `GpkgLayer`.

```rs
use rusqlite_gpkg::Gpkg;

let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
let layers = gpkg.list_layers()?;
let layer = gpkg.get_layer(&layers[0])?;
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

### GpkgLayer

`GpkgLayer` represents a single feature table. You typically get it from
`Gpkg::get_layer` (for existing data) or `Gpkg::create_layer` (for new data).
It exposes the layer schema (geometry column name, property columns) and
methods to iterate, insert, or update features. Insertions and updates accept
any geometry that implements `geo_traits::GeometryTrait<T = f64>`, including
common types from `geo_types` and parsed `wkt::Wkt`.

`GpkgLayer::features()` always allocates a `Vec<GpkgFeature>` for the whole
layer. For large datasets, use `features_batch(batch_size)` to stream features
in chunks and limit peak memory.

```rs
use geo_types::Point;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, params};

let gpkg = Gpkg::new("data/new.gpkg")?;
let columns = vec![
    ColumnSpec { name: "name".to_string(), column_type: ColumnType::Varchar },
    ColumnSpec { name: "value".to_string(), column_type: ColumnType::Integer },
];
let layer = gpkg.create_layer(
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

Batch iteration example:

```rs
use rusqlite_gpkg::Gpkg;

let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
let layer = gpkg.get_layer("points")?;
for batch in layer.features_batch(100)? {
    let features = batch?;
    for feature in features {
        let _id = feature.id();
        let _geom = feature.geometry()?;
    }
}
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

You might notice the `params!` macro in the example above. It is useful when
you want to pass a fixed list of values.

`params!` accepts `Option<T>` and converts `None` to SQL `NULL`. Because `None`
has no inherent type, you may need to annotate it:

```rs
layer.insert(
    Point::new(0.0, 0.0),
    params![Some(1.0_f64), Option::<i64>::None],
)?;
```

When programmatically constructing parameters, build an iterator of `&Value`
from owned values:

```rs
use rusqlite_gpkg::Value;

fn convert_to_value(input: &str) -> Value {
    Value::from(input)
}

let raw = vec!["alpha", "beta"];
let values: Vec<Value> = raw.iter().map(|v| convert_to_value(v)).collect();
layer.insert(Point::new(1.0, 2.0), values.iter())?;
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
let layer = gpkg.get_layer("points")?;
let features = layer.features()?;
let feature = features.first().expect("feature");
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

### Value

`Value` is the crate's owned dynamic value used for feature properties. It
mirrors SQLite's dynamic typing (null, integer, real, text, blob) and is
returned by `GpkgFeature::property` as `Option<Value>`. Convert using
`try_into()` or match directly.

```rs
use rusqlite_gpkg::Gpkg;

let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
let layer = gpkg.get_layer("points")?;
let features = layer.features()?;
let feature = features.first().expect("feature");

let name: String = feature.property("name").ok_or("missing name")?.try_into()?;
let active: bool = feature.property("active").ok_or("missing active")?.try_into()?;
# Ok::<(), rusqlite_gpkg::GpkgError>(())
```

The conversion above returns an error if the value is `NULL`. If you want to
handle `NULL`, convert to `Option<T>`; `NULL` becomes `None` and non-null values
become `Some(T)`:

```rs
use rusqlite_gpkg::Value;

let value = Value::Null;
let maybe_i64: Option<i64> = value.try_into()?;
assert_eq!(maybe_i64, None);
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
        let layer = gpkg.get_layer(&layer_name)?;
        for feature in layer.features()? {
            let geom = feature.geometry()?;

            // Convert geometry to WKT for display.
            let mut wkt = String::new();
            write_geometry(&mut wkt, &geom)?;
            println!("{layer_name}: {wkt}");

            for column in &layer.property_columns {
                // Property values are returned as `Value`.
                let value = feature.property(&column.name).unwrap_or(Value::Null);
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

    let layer = gpkg.create_layer(
        "points",
        "geom".to_string(),
        wkb::reader::GeometryType::Point,
        wkb::reader::Dimension::Xy,
        4326,
        &columns,
    )?;

    // Insert a feature with geometry and property values.
    layer.insert(
        Point::new(1.0, 2.0),    // geometry: You can pass whatever object that implements GeometryTrait
        params!["alpha", 7_i64], // other properties: pass references to Value
    )?;

    Ok(())
}
```

### Arrow reader

```rs
use rusqlite_gpkg::{ArrowGpkgReader, Gpkg};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open an existing GeoPackage and stream Arrow batches.
    let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    let mut reader = ArrowGpkgReader::new(&gpkg, "points", 1024)?;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        // Use the Arrow RecordBatch API.
        println!("rows = {}", batch.num_rows());
    }
    Ok(())
}
```

### Arrow geometry handling

```rs
use geoarrow_array::array::WkbArray;
use geoarrow_array::GeoArrowArrayAccessor;
use rusqlite_gpkg::{ArrowGpkgReader, Gpkg};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    let mut reader = ArrowGpkgReader::new(&gpkg, "points", 256)?;
    if let Some(batch) = reader.next() {
        let batch = batch?;
        let geom_index = batch.num_columns() - 1;
        let schema = batch.schema();
        let geom_field = schema.field(geom_index).as_ref();
        let geom_array =
            WkbArray::try_from((batch.column(geom_index).as_ref(), geom_field))?;

        // Access raw WKB bytes from the geometry column.
        let wkb = geom_array.value(0)?;
        let _bytes: &[u8] = wkb.buf();
    }
    Ok(())
}
```
