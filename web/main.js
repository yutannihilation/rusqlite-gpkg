const statusEl = document.getElementById('status');
const button = document.getElementById('generate');

const worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });

button.disabled = true;

worker.onmessage = (event) => {
  const data = event.data;

  if (data.type === 'ready') {
    statusEl.textContent = 'Ready.';
    button.disabled = false;
    return;
  }

  if (data.type === 'started') {
    statusEl.textContent = 'Generating .gpkg in OPFS...';
    button.disabled = true;
    return;
  }

  if (data.type === 'done') {
    const bytes = new Uint8Array(data.bytes);
    const blob = new Blob([bytes], { type: 'application/geopackage+sqlite3' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = data.filename;
    a.click();

    URL.revokeObjectURL(url);
    statusEl.textContent = `Downloaded ${data.filename} (${bytes.length} bytes).`;
    button.disabled = false;
    return;
  }

  if (data.type === 'error') {
    statusEl.textContent = `Error: ${data.message}`;
    button.disabled = false;
  }
};

button.addEventListener('click', () => {
  worker.postMessage({ type: 'generate' });
});
