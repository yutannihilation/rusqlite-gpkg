const statusEl = document.getElementById('status');
const button = document.getElementById('generate');
const pointCountSelect = document.getElementById('point-count');
let startedAtMs = 0;

function elapsedSeconds() {
  if (!startedAtMs) return 0;
  return (Date.now() - startedAtMs) / 1000;
}

const worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });

button.disabled = true;
pointCountSelect.disabled = true;

worker.onmessage = (event) => {
  const data = event.data;

  if (data.type === 'ready') {
    statusEl.textContent = 'Ready.';
    button.disabled = false;
    pointCountSelect.disabled = false;
    return;
  }

  if (data.type === 'started') {
    startedAtMs = Date.now();
    statusEl.textContent = `Generating ${data.pointCount.toLocaleString()} points in OPFS...`;
    button.disabled = true;
    pointCountSelect.disabled = true;
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
    statusEl.textContent = `Downloaded ${data.filename} with ${Number(data.pointCount).toLocaleString()} points (${bytes.length} bytes) in ${elapsedSeconds().toFixed(1)}s.`;
    button.disabled = false;
    pointCountSelect.disabled = false;
    startedAtMs = 0;
    return;
  }

  if (data.type === 'progress') {
    statusEl.textContent = `Inserted ${Number(data.insertedCount).toLocaleString()} points in ${elapsedSeconds().toFixed(1)}s. Preparing download...`;
    return;
  }

  if (data.type === 'error') {
    const elapsed = startedAtMs ? ` after ${elapsedSeconds().toFixed(1)}s` : '';
    statusEl.textContent = `Error${elapsed}: ${data.message}`;
    button.disabled = false;
    pointCountSelect.disabled = false;
    startedAtMs = 0;
  }
};

button.addEventListener('click', () => {
  const pointCount = Number(pointCountSelect.value);
  worker.postMessage({ type: 'generate', pointCount });
});
