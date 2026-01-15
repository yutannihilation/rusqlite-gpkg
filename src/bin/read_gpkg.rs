use rusqlite::types::Value;
use rusqlite_gpkg::Gpkg;
use wkt::to_wkt::write_geometry;

fn main() {
    if let Err(err) = run() {
        eprintln!("read_gpkg failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Usage: read_gpkg <path-to-gpkg>")?;
    let gpkg = Gpkg::open_read_only(path)?;
    let layers = gpkg.list_layers()?;

    for layer_name in layers {
        let layer = gpkg.open_layer(&layer_name)?;
        println!("layer: {layer_name}");

        for (row_idx, feature) in layer.features()?.enumerate() {
            let mut values = Vec::with_capacity(layer.property_columns.len() + 1);
            let wkb = feature.geometry()?;
            let mut wkt = String::new();
            write_geometry(&mut wkt, &wkb)?;
            values.push(format!("{}={wkt}", layer.geometry_column));

            for column in &layer.property_columns {
                let value = feature.property::<Value>(&column.name)?;
                values.push(format!("{}={}", column.name, format_value(&value)));
            }

            println!("  row {}: {}", row_idx, values.join(", "));
        }
    }

    Ok(())
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Blob(value) => format!("{value:?}"),
    }
}
