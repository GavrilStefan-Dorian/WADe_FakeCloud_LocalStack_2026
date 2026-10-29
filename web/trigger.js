(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const el = (tag, cls, text) => {
    const item = document.createElement(tag);
    if (cls) item.className = cls;
    if (text != null) item.textContent = text;
    return item;
  };
  const emulator = () => document.querySelector('input[name="tl-emu"]:checked').value;
  const buttons = ['tl-disconnect', 'tl-upload', 'tl-reconnect', 'tl-replay'];
  const next = { ready: 'tl-disconnect', disconnected: 'tl-upload', uploaded: 'tl-reconnect',
                 reconnected: 'tl-replay' };
  const finished = { disconnected: 1, uploaded: 2, reconnected: 3, replayed: 3, complete: 4 };
  let current = null, busy = false, timer = null, pending = false, timedOut = false;
  const nodes = {};

  async function request(url, options) {
    const response = await fetch(url, options);
    const text = await response.text();
    let body;
    try { body = JSON.parse(text); } catch (_) { throw new Error(`HTTP ${response.status}: invalid server response`); }
    if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
    return body;
  }
  const post = (action, extra) => request('/api/trigger/' + action, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(Object.assign({ emulator: emulator() }, extra || {}))
  });
  const missing = state => state.files.filter(file => !file.report);

  function setError(message) {
    const panel = $('tl-error');
    panel.textContent = message || '';
    panel.hidden = !message;
  }
  function resetDiagram() {
    for (const key in nodes) {
      nodes[key].node.classList.remove('tl-off');
      nodes[key].small.textContent = nodes[key].original;
    }
  }
  function diagram(state) {
    const n = nodes;
    n.notify.node.classList.toggle('tl-off', !state.connected);
    n.notify.small.textContent = state.connected ? 'S3 notification connected' : 'NOTIFICATION DISCONNECTED';
    n.store.small.textContent = state.files.length ? 'CSV stored in input bucket' : 'wade-graduates-in';
    const report = state.files.find(file => file.report);
    n.read.small.textContent = report ? 'Report generated in output bucket' :
      state.files.length ? 'No report yet' : 'wade-graduates-out';
  }
  function paint(state) {
    current = state;
    diagram(state);
    const phase = state.phase;
    const line = $('tl-trigger');
    line.className = 'tl-line ' + (state.connected ? 'on' : 'off');
    if (phase === 'unsafe') {
      line.textContent = 'The saved experiment state no longer matches the emulator. Use Reset to restore the notification.';
    } else {
      line.textContent = state.connected ? 'S3 notification: connected' : 'S3 notification: disconnected';
    }
    $('tl-progress').textContent = phase === 'complete' ? 'Completed: the retry produced a report.' :
      phase === 'replayed' ? 'Retry sent. Checking S3 for the generated report…' :
      phase === 'unsafe' ? 'Configuration needs recovery' :
      `Step ${(finished[phase] || 0) + 1} of 4`;

    const box = $('tl-files');
    box.replaceChildren();
    if (!state.files.length) {
      box.append(el('p', 'tl-none', 'No file uploaded for this experiment.'));
    } else {
      const table = el('table', 'tl-files');
      for (const file of state.files) {
        const row = el('tr');
        row.append(el('td', '', file.key.replace('incoming/', '')));
        row.append(el('td', file.report ? 'has' : 'none', file.report ?
          `Report generated · ${file.report.accepted} accepted · ${file.report.rejected} rejected` :
          'Stored in S3 · NO REPORT'));
        table.append(row);
      }
      box.append(table);
    }

    const highlighted = next[phase] || (phase === 'replayed' && timedOut ? 'tl-replay' : '');
    for (const [index, id] of buttons.entries()) {
      const button = $(id);
      button.classList.toggle('next', id === highlighted && !busy);
      button.classList.toggle('completed', (finished[phase] || 0) > index);
      button.disabled = busy || !(id === next[phase] || (id === 'tl-replay' && phase === 'replayed' && timedOut));
    }
    $('tl-reset').disabled = busy;
    // An invocation record is meaningful here only after the retry produced a report.
    // Earlier records may be unrelated, so do not invite inspection during outage.
    $('tl-evidence').disabled = busy || phase !== 'complete';
    $('tl-evidence').title = phase === 'complete' ? 'Inspect the recorded Lambda event for the recovered upload' : 'Available after Step 4 generates a report';
    $('tl-replay-cap').textContent = state.key ? 'Upload the stored CSV again to generate a NEW event.' :
      'Available after reconnecting the trigger.';
    const note = $('tl-note');
    const noReport = state.files.some(file => !file.report);
    let message = '';
    if (phase === 'uploaded') message = 'S3 stored the file, but there was no notification to invoke Lambda. No report was generated.';
    else if (phase === 'reconnected') message = 'The notification is connected again. The earlier upload still has no report; restoring a trigger does not replay missed events.';
    else if (phase === 'complete') message = 'Recovery confirmed: re-uploading created a NEW S3 event. Lambda processed it and wrote the report. Now you can inspect that invocation.';
    else if (phase === 'replayed' && timedOut && noReport) message = 'Still no report. Inspect execution evidence, or click Replay again. Do not claim success yet.';
    else if (phase === 'disconnected') message = 'S3 will store the next CSV, but the missing notification means Lambda will not be started by that upload.';
    else if (phase === 'unsafe') message = 'An unexpected configuration change was detected. Reset restores from the saved configuration before deleting test objects.';
    note.textContent = message;
    note.hidden = !message;
  }

  async function refresh() {
    const chosen = emulator();
    const response = await request('/api/trigger/state?emulator=' + encodeURIComponent(chosen));
    if (chosen === emulator()) paint(response);
    return response;
  }
  function stopPolling() { clearInterval(timer); timer = null; pending = false; }
  function watchForReport() {
    stopPolling(); timedOut = false;
    const deadline = Date.now() + 12000;
    const selected = emulator();
    timer = setInterval(async () => {
      if (pending || selected !== emulator()) return;
      pending = true;
      try {
        const state = await refresh();
        if (state.phase === 'complete' || Date.now() >= deadline) {
          stopPolling();
          timedOut = state.phase !== 'complete';
          paint(state);
        }
      } catch (error) { stopPolling(); setError(error.message); }
      finally { pending = false; }
    }, 700);
  }

  async function step(action, options) {
    if (busy) return;
    busy = true;
    stopPolling();
    if (current) paint(current);
    setError('');
    try {
      await action();
      $('tl-evbox').hidden = true;
      timedOut = false;
      const state = await refresh();
      if (options && options.poll && state.phase === 'replayed') watchForReport();
    } catch (error) {
      setError(error.message);
      try { await refresh(); } catch (_) { /* Show original error. */ }
    } finally {
      busy = false;
      if (current) paint(current);
    }
  }

  function init() {
    for (const [key, id] of Object.entries({ notify: 'fn-notify', store: 'fn-store', read: 'fn-read' })) {
      const node = $(id);
      const small = node.querySelector('small');
      nodes[key] = { node, small, original: small.textContent };
    }
    $('tl-disconnect').onclick = () => step(() => post('disconnect'));
    $('tl-upload').onclick = () => step(() => {
      const csv = $('csv-editor').value;
      if (!csv.trim()) throw new Error('Select a sample CSV under Data first.');
      return post('upload', { csv });
    });
    $('tl-reconnect').onclick = () => step(() => post('reconnect'));
    $('tl-replay').onclick = () => step(() => post('replay', { key: current && current.key }), { poll: true });
    $('tl-reset').onclick = () => step(() => post('reset'));
    $('tl-evidence').onclick = async () => {
      if (!current || current.phase !== 'complete') return;
      setError('');
      try {
        const result = await request('/api/trigger/evidence?emulator=' + encodeURIComponent(emulator()));
        $('tl-source').textContent = result.source;
        $('tl-pre').textContent = result.text || '(No execution details returned)';
        $('tl-evbox').hidden = false;
      } catch (error) { setError(error.message); }
    };
    document.querySelectorAll('input[name="tl-emu"]').forEach(input => {
      input.onchange = () => {
        stopPolling(); timedOut = false; current = null;
        $('tl-evbox').hidden = true;
        setError('');
        refresh().then(state => { if (state.phase === 'replayed') watchForReport(); }).catch(error => setError(error.message));
      };
    });
    $('trigger-lab').addEventListener('toggle', () => {
      if ($('trigger-lab').open) {
        setError('');
        refresh().then(state => { if (state.phase === 'replayed') watchForReport(); }).catch(error => setError(error.message));
      } else {
        stopPolling(); resetDiagram();
      }
    });
    buttons.forEach(id => { $(id).disabled = true; });
    $('tl-reset').disabled = true;
    $('tl-evidence').disabled = true;
  }
  document.addEventListener('DOMContentLoaded', init);
})();
