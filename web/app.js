'use strict';

const editor = document.querySelector('#csv-editor');
const runButton = document.querySelector('#run-button');
const runHint = document.querySelector('#run-hint');
const results = document.querySelector('#results');
const comparison = document.querySelector('#comparison');
const csvPreview = document.querySelector('#csv-preview');
const activity = document.querySelector('#activity');
const sampleButtons = [...document.querySelectorAll('[data-sample]')];
let activeRun = null;
let latestRequestId = 0;
let showAllRows = false;

function el(tag, className = '', text = null) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== null) node.textContent = String(text);
  return node;
}
function paragraph(text, klass='') { return el('p', klass, text); }
function nodeWithText(tag, klass, text) { return el(tag, klass, text); }
function clear(node) { node.replaceChildren(); }

async function fetchJSON(url, options) {
  const response = await fetch(url, options);
  const json = await response.json();
  if (!response.ok) throw new Error(json.error || `HTTP ${response.status}`);
  return json;
}

async function refreshHealth() {
  try {
    const data = await fetchJSON('/api/status');
    for (const name of ['FakeCloud', 'LocalStack']) {
      const id = name === 'FakeCloud' ? 'engine-fakecloud' : 'engine-localstack';
      const element = document.getElementById(id);
      element.classList.toggle('ready', !!data.emulators[name]?.reachable);
      element.classList.toggle('offline', !data.emulators[name]?.reachable);
      element.querySelector('.status-word').textContent = data.emulators[name]?.reachable ? 'Ready' : 'Not reachable';
    }
  } catch {
    for (const id of ['engine-fakecloud', 'engine-localstack']) {
      const element = document.getElementById(id);
      element.classList.add('offline');
      element.querySelector('.status-word').textContent = 'Unknown';
    }
  }
}

async function loadSample(name) {
  try {
    const data = await fetchJSON(`/api/samples/${name}`);
    editor.value = data.csv;
    sampleButtons.forEach(button => {
      const selected = button.dataset.sample === name;
      button.classList.toggle('active', selected);
      button.setAttribute('aria-pressed', String(selected));
    });
    runHint.textContent = '';
    renderPreview();
  } catch (err) {
    runHint.textContent = `Could not load the sample: ${err.message}`;
  }
}

// Compact, read-only preview. The editable original remains in the CSV field.
function csvRows(text) {
  const rows = [];
  let row = [], value = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === '"') {
      if (quoted && text[i+1] === '"') { value += '"'; i++; }
      else quoted = !quoted;
    } else if (char === ',' && !quoted) {
      row.push(value); value = '';
    } else if ((char === '\n' || char === '\r') && !quoted) {
      if (char === '\r' && text[i+1] === '\n') i++;
      row.push(value); value = '';
      if (row.some(cell => cell.trim())) rows.push(row);
      row = [];
    } else value += char;
  }
  row.push(value);
  if (row.some(cell => cell.trim())) rows.push(row);
  return rows;
}
function renderPreview() {
  clear(csvPreview);
  const rows = csvRows(editor.value);
  if (!rows.length) {
    csvPreview.append(paragraph('No CSV loaded.'));
    return;
  }
  const headers = rows[0].map(h=>h.trim().toLowerCase());
  const columns = [
    ['graduate_id','ID'],
    ['graduation_year','Year'],
    ['program','Program'],
    ['status','Status']
  ].map(([key,label])=>[headers.indexOf(key), label]).filter(([idx])=>idx>=0);
  if (!columns.length) {
    csvPreview.append(paragraph('Open Edit CSV to check the column headers.'));
    return;
  }
  const table = el('table','preview-table');
  const thead = el('thead'), headerRow = el('tr');
  for (const [,label] of columns) headerRow.append(el('th','',label));
  thead.append(headerRow); table.append(thead);
  const tbody = el('tbody');
  for (const values of rows.slice(1,showAllRows ? undefined : 7)) {
    const tr = el('tr');
    for (const [idx] of columns) tr.append(el('td','',values[idx] || '—'));
    tbody.append(tr);
  }
  table.append(tbody);
  csvPreview.append(table);
  const count = rows.length - 1;
  const foot = el('div','preview-foot');
  foot.append(el('span','',`${count} data row${count===1?'':'s'} · showing ${showAllRows?count:Math.min(count,6)}`));
  if (count > 6) {
    const expand = el('button','preview-expand',showAllRows ? 'Show fewer' : `Show all ${count}`);
    expand.type='button';
    expand.addEventListener('click',()=>{showAllRows=!showAllRows;renderPreview();});
    foot.append(expand);
  }
  csvPreview.append(foot);
}

function drawBreakdown(parent, title, data) {
  if (!data || !Object.keys(data).length) return;
  const section = el('section', 'breakdown');
  section.append(el('h4', '', title));
  const max = Math.max(1, ...Object.values(data).map(Number));
  Object.entries(data).sort((a,b) => b[1]-a[1] || a[0].localeCompare(b[0])).forEach(([label, value]) => {
    const row = el('div','breakdown-row');
    row.append(el('span','breakdown-name',label));
    const track = el('div','bar-track');
    track.setAttribute('role','img');
    track.setAttribute('aria-label',`${label}: ${value} records`);
    const bar = el('div','bar-fill');
    bar.style.width = `${Math.max(0,Math.min(100,100*Number(value)/max))}%`;
    track.append(bar);
    row.append(track, el('span','breakdown-count', value));
    section.append(row);
  });
  parent.append(section);
}

function reportCard(name, entry, data) {
  const card = el('article','report');
  const heading = el('header','report-top');
  heading.append(el('h3','',name));
  if (entry.state === 'complete') {
    const status = data.status || 'unknown';
    heading.append(el('span',`report-status ${status}`,status.toUpperCase()));
    card.append(heading);
    const content = el('div','report-body');
    const metrics = el('div','metrics');
    for (const [value,label] of [[data.accepted,'Accepted'],[data.rejected,'Rejected'],[`${data.employmentRatePct}%`,'Employment rate']]) {
      const metric = el('div');
      metric.append(el('div','metric-number',value),el('div','metric-label',label));
      metrics.append(metric);
    }
    content.append(metrics);

    drawBreakdown(content,'Employment status',data.byStatus);
    drawBreakdown(content,'Study program',data.byProgram);
    drawBreakdown(content,'Industry (employed)',data.byIndustry);
    drawBreakdown(content,'Graduation year',data.byGraduationYear);
    drawBreakdown(content,'City',data.byCity);
    if (data.cohortEmployment && Object.keys(data.cohortEmployment).length) {
      const details = el('details','cohorts');
      details.append(el('summary','','Employment by graduation year'));
      const table = el('table','cohort-table');
      const head = el('tr');
      for (const title of ['Year','Graduates','Employed','Rate']) head.append(el('th','',title));
      table.append(head);
      for (const [year, value] of Object.entries(data.cohortEmployment)) {
        const tr = el('tr');
        for (const text of [year, value.total, value.employed, `${value.employmentRatePct}%`]) tr.append(el('td','',text));
        table.append(tr);
      }
      details.append(table);
      content.append(details);
    }
    if (data.errors?.length) {
      const details = el('details');
      details.open = data.rejected > 0;
      details.append(el('summary','',`Validation errors (${data.errors.length})`));
      const list = el('ul','errors-list');
      for (const err of data.errors) {
        const item = el('li');
        item.append(el('strong','',`Line ${err.line ?? 'unknown'} · ${err.graduateId || 'CSV format'}`));
        item.append(el('span','',Array.isArray(err.reasons) ? err.reasons.join('; ') : 'Invalid record'));
        list.append(item);
      }
      details.append(list); content.append(details);
    }
    if (data.acceptedRecords?.length) {
      const details = el('details');
      details.append(el('summary','',`Accepted records (${data.acceptedRecords.length})`));
      const pre = el('pre','raw-json');
      pre.textContent = data.acceptedRecords.map(row => `${row.graduateId} | ${row.graduationYear} | ${row.program} | ${row.status}${row.industry ? ' | '+row.industry : ''}`).join('\n');
      details.append(pre); content.append(details);
    }
    const inspect = el('details');
    inspect.append(el('summary','','View report JSON'));
    inspect.append(el('pre','raw-json',JSON.stringify(data,null,2)));
    content.append(inspect);
    const meta = el('details','metadata');
    meta.append(el('summary','','S3 object'));
    meta.append(el('code','', 'wade-graduates-out / '+(entry.outputKey || '')));
    content.append(meta);
    const actions = el('div','report-actions');
    const inspectButton = el('button','inspect-button','Inspect S3 / Lambda');
    inspectButton.type='button';
    inspectButton.addEventListener('click', () => showInspection(name, card));
    const download = el('button','export-button','Save report JSON');
    download.type='button';
    download.addEventListener('click', () => {
      const blob = new Blob([JSON.stringify(data,null,2)],{type:'application/json'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href=url; a.download=`${name.toLowerCase()}-graduate-report.json`; a.click();
      setTimeout(()=>URL.revokeObjectURL(url),3000);
    });
    actions.append(inspectButton, download);
    content.append(actions);
    const inspection = el('div','inspection');
    inspection.hidden=true;
    card.append(content,inspection);
    return card;
  }
  card.append(heading);
  if (entry.state === 'waiting') {
    const pending = el('div','pending');
    pending.append(el('strong','','Processing…'));
    pending.append(paragraph('Waiting for the Lambda report.'));
    card.append(pending);
  } else {
    const error = el('div','error-panel');
    error.append(el('strong','',entry.state === 'timeout' ? 'No report produced' : 'An error occurred'));
    error.append(paragraph(entry.error || 'No report was returned.'));
    card.append(error);
  }
  return card;
}


// Live observations are taken from actual S3 PUT/HEAD/GET requests in ui_server.py.
// The activity view does not pretend to know the internal Lambda progress.
function drawActivity(run) {
  clear(activity);
  const title = el('div','activity-title');
  title.append(el('strong','','Observed activity'),el('span','','Updated from local S3 requests'));
  activity.append(title);
  const columns=el('div','activity-columns');
  for (const [name, entry] of Object.entries(run.targets)) {
    const panel=el('section','activity-emulator');
    const label=el('div','activity-emulator-head');
    label.append(el('strong','',name),el('span',`activity-state ${entry.state}`, entry.state==='complete'?'Report ready':entry.state==='waiting'?'Waiting for report':entry.state));
    panel.append(label);
    const timeline=el('ol','event-trace');
    for (const event of entry.events || []) {
      const li=el('li',`event ${event.kind.includes('error')?'failed':''}`);
      const top=el('div','event-label');
      top.append(el('strong','',event.label));
      const d=new Date(event.at);
      top.append(el('time','',Number.isNaN(d.getTime())?'':d.toLocaleTimeString([], {hour12:false})));
      li.append(top);
      if (event.detail) li.append(el('div','event-detail',event.detail));
      timeline.append(li);
    }
    if (entry.state==='waiting') {
      const pending=el('li','event waiting');
      pending.append(el('div','event-label','Awaiting the output object'));
      timeline.append(pending);
    }
    panel.append(timeline);
    columns.append(panel);
  }
  activity.append(columns);
}

async function showInspection(name, card) {
  const box=card.querySelector('.inspection');
  const button=card.querySelector('.inspect-button');
  if (!box.hidden) {box.hidden=true;return;}
  box.hidden=false;
  box.replaceChildren(el('p','metadata','Reading current resource configuration…'));
  button.disabled=true;
  try {
    const evidence=await fetchJSON(`/api/inspect/${activeRun}/${encodeURIComponent(name)}`);
    clear(box);
    box.append(el('h4','','Backend evidence'));
    const table=el('dl','inspection-list');
    function row(k,v) {
      const dt=el('dt','',k), dd=el('dd','',v ?? 'Not observed');
      table.append(dt,dd);
    }
    row('Endpoint',evidence.endpoint);
    row('Input object',evidence.input? `${evidence.input.bucket}/${evidence.input.key} (${evidence.input.sizeBytes} bytes)`:'Not found');
    row('Output object',evidence.output? `${evidence.output.bucket}/${evidence.output.key} (${evidence.output.sizeBytes} bytes)`:'Not found');
    row('Function',evidence.function?.FunctionArn);
    row('Runtime',evidence.function?.Runtime);
    row('State',evidence.function?.State);
    row('S3 notification', (evidence.notification||[]).map(x=>`${(x.events||[]).join(', ')} → ${x.arn}`).join(' | ') || 'Not returned');
    box.append(table);
    if (evidence.logSource) {
      box.append(el('h5','','Execution log evidence'),el('p','source-detail',evidence.logSource));
      const logs=el('pre','real-logs',evidence.logs.length?evidence.logs.join('\n\n'):'No matching events returned.');
      box.append(logs);
    }
    if (evidence.notes?.length) box.append(el('p','inspect-notes',evidence.notes.join(' ')));
  } catch(error) {
    box.replaceChildren(el('p','error-panel',`Could not inspect: ${error.message}`));
  } finally {button.disabled=false;}
}

function drawRun(run) {
  if (!run) return;
  drawActivity(run);
  clear(results);
  results.className = 'result-grid';
  for (const [name, entry] of Object.entries(run.targets)) {
    results.append(reportCard(name,{...entry,outputKey:run.outputKey},entry.report));
  }
  comparison.hidden = false;
  comparison.className = 'comparison-note';
  if (run.reportsMatch === true) {
    comparison.classList.add('match');
    comparison.textContent = 'Reports match.';
  } else if (run.reportsMatch === false) {
    comparison.classList.add('differ');
    comparison.textContent = 'Reports differ. Open the JSON results to compare.';
  } else {
    const states = Object.values(run.targets).map(entry => entry.state);
    comparison.textContent = states.every(x => x === 'complete')
      ? 'Report ready.'
      : `Processing · ${states.filter(x=>x==='complete').length}/${states.length} ready`;
  }
}

async function watchRun(id, requestId) {
  for (let i = 0; i < 90; i++) {
    if (latestRequestId !== requestId) return;
    const run = await fetchJSON(`/api/runs/${id}`);
    if (latestRequestId !== requestId) return;
    drawRun(run);
    const states = Object.values(run.targets).map(entry=>entry.state);
    if (states.every(state=>state !== 'waiting')) return;
    await new Promise(resolve => setTimeout(resolve,700));
  }
  throw new Error('Report polling expired. Check the local server output.');
}

async function runExperiment() {
  const targets = [];
  if (document.querySelector('#select-fakecloud').checked) targets.push('FakeCloud');
  if (document.querySelector('#select-localstack').checked) targets.push('LocalStack');
  if (!targets.length) { runHint.textContent='Select at least one emulator.'; return; }
  if (!editor.value.trim()) { runHint.textContent='Enter or load CSV data first.'; return; }
  const requestId=++latestRequestId;
  runButton.disabled = true;
  runHint.textContent = 'Uploading…';
  try {
    const run = await fetchJSON('/api/runs',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({csv:editor.value,targets})
    });
    activeRun=run.id;
    drawRun(run);
    runHint.textContent = 'Processing…';
    await watchRun(run.id,requestId);
    runHint.textContent = 'Report ready.';
    document.querySelector('.full-results')?.scrollIntoView({behavior:'smooth', block:'start'});
  } catch (error) {
    runHint.textContent = `Error: ${error.message}`;
    comparison.hidden = false;
    comparison.className = 'comparison-note differ';
    comparison.textContent = `Experiment stopped: ${error.message}`;
  } finally {
    if (latestRequestId===requestId) runButton.disabled = false;
    refreshHealth();
  }
}

sampleButtons.forEach(button => button.addEventListener('click',()=>loadSample(button.dataset.sample)));
document.querySelector('#csv-file').addEventListener('change',async event => {
  const file=event.target.files[0]; if(!file) return;
  if (file.size>120000) { runHint.textContent='That file is too large for this local demonstration.'; return; }
  editor.value=await file.text();
  sampleButtons.forEach(button=>{button.classList.remove('active');button.setAttribute('aria-pressed','false');});
  runHint.textContent=`Loaded ${file.name}.`;
  renderPreview();
});
editor.addEventListener('input',()=>{
  renderPreview();
  sampleButtons.forEach(button=>{button.classList.remove('active');button.setAttribute('aria-pressed','false');});
});
runButton.addEventListener('click',runExperiment);
loadSample('valid');
refreshHealth();
