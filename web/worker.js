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

  if (!ready) {
    postMessage({ type: 'error', message: 'Worker is not ready yet.' });
    return;
  }

  try {
    postMessage({ type: 'started' });

    const opfsRoot = await navigator.storage.getDirectory();
    const filename = 'example.gpkg';
    const fileHandle = await opfsRoot.getFileHandle(filename, { create: true });
    const accessHandle = await fileHandle.createSyncAccessHandle();

    try {
      generate_gpkg_to_opfs(accessHandle);

      const size = accessHandle.getSize();
      const bytes = new Uint8Array(size);
      const readSize = accessHandle.read(bytes, { at: 0 });
      const out = bytes.slice(0, readSize);

      postMessage({ type: 'done', filename, bytes: out.buffer }, [out.buffer]);
    } finally {
      accessHandle.close();
    }
  } catch (error) {
    postMessage({ type: 'error', message: String(error) });
  }
};
