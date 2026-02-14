// Worker-side data generation pipeline.
//
// Why a worker:
// - OPFS sync access handles are intended to be used in workers.
// - GeoPackage generation can be expensive and would otherwise block UI.
import init, { generate_gpkg_to_opfs } from './pkg/rusqlite_gpkg_web.js';

let ready = false;

async function initialize() {
  // wasm-bindgen init loads/instantiates the .wasm module.
  await init();
  ready = true;
  // Tell main thread that generate button can be enabled.
  postMessage({ type: 'ready' });
}

initialize().catch((error) => {
  postMessage({ type: 'error', message: String(error) });
});

self.onmessage = async (event) => {
  const data = event.data;
  if (data?.type !== 'generate') {
    return;
  }
  const pointCount = Number(data?.pointCount ?? 100000);
  const safePointCount =
    Number.isFinite(pointCount) && pointCount > 0 ? Math.floor(pointCount) : 100000;

  if (!ready) {
    postMessage({ type: 'error', message: 'Worker is not ready yet.' });
    return;
  }

  try {
    // UI can start elapsed-time tracking immediately.
    postMessage({ type: 'started', pointCount: safePointCount });

    // OPFS root for this origin.
    const opfsRoot = await navigator.storage.getDirectory();
    const filename = `example_${safePointCount}.gpkg`;
    const fileHandle = await opfsRoot.getFileHandle(filename, { create: true });
    // Design note:
    // We intentionally create a fresh sync handle per generation run.
    // Reusing one handle can be brittle because handle lifecycle/closure can happen
    // across JS/Rust boundaries. The VFS registration is reused, but file handles are not.
    const accessHandle = await fileHandle.createSyncAccessHandle();

    try {
      // Rust creates the GeoPackage and writes through HybridVfs -> OpfsFile.
      const insertedCount = generate_gpkg_to_opfs(accessHandle, safePointCount);
      postMessage({ type: 'progress', insertedCount });
    } finally {
      try {
        // The Rust side may have already closed the handle on drop.
        accessHandle.close();
      } catch (_) {
        // The handle may already be closed by Rust code.
      }
    }

    // Read final bytes from the OPFS file handle for download.
    const outputFile = await fileHandle.getFile();
    const out = await outputFile.arrayBuffer();
    // Transfer buffer ownership to avoid a structured clone copy.
    postMessage({ type: 'done', filename, bytes: out, pointCount: safePointCount }, [out]);
  } catch (error) {
    postMessage({ type: 'error', message: String(error) });
  }
};
