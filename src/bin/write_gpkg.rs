use rusqlite::params;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg};
use std::f64::consts::PI;
use std::str::FromStr;
use wkt::Wkt;

fn main() {
    if let Err(err) = run() {
        eprintln!("write_gpkg failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Usage: write_gpkg <output.gpkg>")?;

    let gpkg = Gpkg::open(path)?;

    let columns = vec![
        ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        },
        ColumnSpec {
            name: "region".to_string(),
            column_type: ColumnType::Varchar,
        },
        ColumnSpec {
            name: "center_lat".to_string(),
            column_type: ColumnType::Double,
        },
        ColumnSpec {
            name: "center_lon".to_string(),
            column_type: ColumnType::Double,
        },
        ColumnSpec {
            name: "points".to_string(),
            column_type: ColumnType::Integer,
        },
        ColumnSpec {
            name: "note".to_string(),
            column_type: ColumnType::Varchar,
        },
    ];

    let layer = gpkg.create_layer(
        "stars",
        "geom".to_string(),
        wkb::reader::GeometryType::Polygon,
        wkb::reader::Dimension::Xy,
        4326,
        &columns,
    )?;

    let tokyo_center = (139.767, 35.681);
    let tokyo_star = star_polygon_wkt(tokyo_center.0, tokyo_center.1, 1.4, 0.6, 5)?;
    let tokyo_name = "Tokyo Star".to_string();
    let tokyo_region = "Tokyo".to_string();
    let tokyo_points = 5_i64;
    let tokyo_note = "Star polygon around Tokyo".to_string();
    layer.insert(
        tokyo_star,
        params![
            tokyo_name,
            tokyo_region,
            tokyo_center.1,
            tokyo_center.0,
            tokyo_points,
            tokyo_note,
        ],
    )?;

    let hokkaido_center = (141.3468, 43.0642);
    let hokkaido_star = star_polygon_wkt(hokkaido_center.0, hokkaido_center.1, 2.2, 0.9, 5)?;
    let hokkaido_name = "Hokkaido Star".to_string();
    let hokkaido_region = "Hokkaido".to_string();
    let hokkaido_points = 5_i64;
    let hokkaido_note = "Star polygon around Hokkaido".to_string();
    layer.insert(
        hokkaido_star,
        params![
            hokkaido_name,
            hokkaido_region,
            hokkaido_center.1,
            hokkaido_center.0,
            hokkaido_points,
            hokkaido_note,
        ],
    )?;

    Ok(())
}

fn star_polygon_wkt(
    center_lon: f64,
    center_lat: f64,
    outer_radius: f64,
    inner_radius: f64,
    points: usize,
) -> Result<Wkt<f64>, Box<dyn std::error::Error>> {
    let mut coords = Vec::with_capacity(points * 2 + 1);
    let total_vertices = points * 2;
    let start_angle = -PI / 2.0;

    for i in 0..total_vertices {
        let radius = if i % 2 == 0 {
            outer_radius
        } else {
            inner_radius
        };
        let angle = start_angle + (i as f64) * (2.0 * PI / total_vertices as f64);
        let lon = center_lon + radius * angle.cos();
        let lat = center_lat + radius * angle.sin();
        coords.push((lon, lat));
    }

    if let Some(first) = coords.first().copied() {
        coords.push(first);
    }

    let mut ring = String::new();
    for (idx, (lon, lat)) in coords.iter().copied().enumerate() {
        if idx > 0 {
            ring.push_str(", ");
        }
        ring.push_str(&format!("{lon} {lat}"));
    }

    let wkt = format!("POLYGON (({ring}))");
    Ok(Wkt::from_str(&wkt)?)
}
