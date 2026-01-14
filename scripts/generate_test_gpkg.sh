#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixtures_dir="${root_dir}/src/test/fixtures"
output_gpkg="${root_dir}/src/test/test_generated.gpkg"

mkdir -p "${fixtures_dir}"

cat > "${fixtures_dir}/points.geojson" <<'EOF'
{
  "type": "FeatureCollection",
  "name": "points",
  "features": [
    {
      "type": "Feature",
      "properties": { "id": 1, "name": "alpha", "elevation": 12.5, "active": true, "category": "A", "note": "first" },
      "geometry": { "type": "Point", "coordinates": [139.7000, 35.6895] }
    },
    {
      "type": "Feature",
      "properties": { "id": 2, "name": "beta", "elevation": 44.2, "active": false, "category": "B", "note": null },
      "geometry": { "type": "Point", "coordinates": [139.7100, 35.6890] }
    },
    {
      "type": "Feature",
      "properties": { "id": 3, "name": "gamma", "elevation": 3.0, "active": true, "category": "A", "note": "near river" },
      "geometry": { "type": "Point", "coordinates": [139.6950, 35.6850] }
    },
    {
      "type": "Feature",
      "properties": { "id": 4, "name": "delta", "elevation": 88.9, "active": true, "category": "C", "note": "tower" },
      "geometry": { "type": "Point", "coordinates": [139.7200, 35.6900] }
    },
    {
      "type": "Feature",
      "properties": { "id": 5, "name": "epsilon", "elevation": 0.5, "active": false, "category": "B", "note": "" },
      "geometry": { "type": "Point", "coordinates": [139.7050, 35.6920] }
    }
  ]
}
EOF

cat > "${fixtures_dir}/lines.geojson" <<'EOF'
{
  "type": "FeatureCollection",
  "name": "lines",
  "features": [
    {
      "type": "Feature",
      "properties": { "id": 101, "route": "north", "speed": 30.0, "paved": true, "lanes": 2, "note": "main" },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [139.6900, 35.6800],
          [139.7000, 35.6850],
          [139.7100, 35.6900]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": { "id": 102, "route": "east", "speed": 50.0, "paved": false, "lanes": 1, "note": null },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [139.7000, 35.6850],
          [139.7150, 35.6850],
          [139.7300, 35.6850]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": { "id": 103, "route": "south", "speed": 40.0, "paved": true, "lanes": 3, "note": "express" },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [139.7050, 35.7000],
          [139.7000, 35.6900],
          [139.6950, 35.6800]
        ]
      }
    }
  ]
}
EOF

cat > "${fixtures_dir}/polygons.geojson" <<'EOF'
{
  "type": "FeatureCollection",
  "name": "polygons",
  "features": [
    {
      "type": "Feature",
      "properties": { "id": 201, "zone": "residential", "population": 1200, "density": 350.5, "flag": true, "note": "quiet" },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [139.6900, 35.6950],
            [139.7000, 35.6950],
            [139.7000, 35.7050],
            [139.6900, 35.7050],
            [139.6900, 35.6950]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": { "id": 202, "zone": "industrial", "population": 300, "density": 120.0, "flag": false, "note": null },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [139.7100, 35.6750],
            [139.7250, 35.6750],
            [139.7250, 35.6900],
            [139.7100, 35.6900],
            [139.7100, 35.6750]
          ]
        ]
      }
    }
  ]
}
EOF

rm -f "${output_gpkg}"

gdal vector convert \
  --output-format GPKG \
  --overwrite \
  --output-layer points \
  "${fixtures_dir}/points.geojson" \
  "${output_gpkg}"

gdal vector convert \
  --update \
  --append \
  --output-layer lines \
  "${fixtures_dir}/lines.geojson" \
  "${output_gpkg}"

gdal vector convert \
  --update \
  --append \
  --output-layer polygons \
  "${fixtures_dir}/polygons.geojson" \
  "${output_gpkg}"

echo "Wrote ${output_gpkg}"
