import init, { generate_gpkg_to_opfs } from './pkg/rusqlite_gpkg_web.js';

let ready = false;

async function initialize() {
  await init();
  ready = true;
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
    postMessage({ type: 'started', pointCount: safePointCount });

    const opfsRoot = await navigator.storage.getDirectory();
    const filename = `example_${safePointCount}.gpkg`;
    const fileHandle = await opfsRoot.getFileHandle(filename, { create: true });
    const accessHandle = await fileHandle.createSyncAccessHandle();

    try {
      const insertedCount = generate_gpkg_to_opfs(accessHandle, safePointCount);
      postMessage({ type: 'progress', insertedCount });
    } finally {
      try {
        accessHandle.close();
      } catch (_) {
        // The handle may already be closed by Rust code.
      }
    }

    const outputFile = await fileHandle.getFile();
    const out = await outputFile.arrayBuffer();
    postMessage({ type: 'done', filename, bytes: out, pointCount: safePointCount }, [out]);
  } catch (error) {
    postMessage({ type: 'error', message: String(error) });
  }
};
