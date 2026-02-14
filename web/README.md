# Web Demo

This demo creates a small GeoPackage in a Web Worker, writes it to OPFS, then downloads it.

## Local build

```sh
wasm-pack build web/wasm --target web --out-dir ../pkg --out-name rusqlite_gpkg_web
```

Then serve the `web/` directory over HTTP (for example with `python -m http.server`).

## GitHub Pages

The workflow `.github/workflows/pages-demo.yml` builds the wasm package and deploys the static files in `web/`.
