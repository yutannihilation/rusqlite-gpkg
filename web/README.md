# Web Demo

This demo creates a small GeoPackage in a Web Worker, writes it to OPFS, then downloads it.

## Local build

```sh
wasm-pack build web/wasm --target web --out-dir ../pkg --out-name rusqlite_gpkg_web
```

Then serve the `web/` directory over HTTP (for example with `python -m http.server`).

## GitHub Pages

The workflow `.github/workflows/pages-demo.yml` builds the wasm package and deploys the static files in `web/`.

## Web Design Notes

The demo under `web/` is intentionally technical and shows one practical way to
integrate `rusqlite-gpkg` in a browser app.

Architecture used by the demo:

- `web/main.js`: main-thread UI only (button/select/status + download trigger).
- `web/worker.js`: runs heavy work off the UI thread and owns OPFS handles.
- `web/wasm/src/lib.rs`: generates data through `rusqlite-gpkg`.
- `web/wasm/src/io.rs`: bridges OPFS `FileSystemSyncAccessHandle` to Rust
  `Read/Write/Seek`.

Why this demo uses Hybrid VFS:

- `HybridVfsBuilder` lets you route the main SQLite file to a custom writer
  (`OpfsFile` here) while keeping sidecar files in memory.
- This is useful when you want explicit control of where bytes go during
  generation, and when you want to connect custom browser storage flows.
- In this demo, we register a named VFS and open the database with
  `Gpkg::open_with_vfs("demo.sqlite", vfs_name)` so writes to the `.sqlite`
  file go to OPFS.

Alternative design: memory-only then serialize:

- Create data with `Gpkg::open_in_memory()`.
- Export bytes with `Gpkg::to_bytes()`.
- Handle persistence/download in JS.

Tradeoff summary:

- Hybrid VFS route:
  - Pros: direct streaming path to custom storage target; explicit VFS control.
  - Cons: more moving parts (custom VFS registration + worker/OPFS integration).
- Memory-only route:
  - Pros: simplest integration for many apps.
  - Cons: peak memory can be larger because output is materialized as a full
    byte vector before persistence.

Implementation hints for your own app:

1. Keep OPFS sync handles in a Worker (not main thread).
2. Keep UI and generation separated; pass only small control messages in, and
   transfer `ArrayBuffer` out.
3. Start with `open_in_memory + to_bytes` for simplicity; move to Hybrid VFS
   if you need custom write routing or tighter control over storage behavior.
4. Use small test sizes first, then scale up (100 -> 10k -> 1M) and monitor
   elapsed time in the UI.
