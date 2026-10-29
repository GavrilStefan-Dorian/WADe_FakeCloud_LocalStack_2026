'use strict';
const $ = q => document.querySelector(q);
let csvText = '';
const ids = {FakeCloud:'fc', LocalStack:'ls'};
function csvRows(content) {
  const rows=[]; let row=[],value='',quoted=false;
  for(let i=0;i<content.length;i++) {
    const ch=content[i];
    if(ch==='"') {if(quoted&&content[i+1]==='"'){value+='"';i++} else quoted=!quoted}
    else if(ch===','&&!quoted){row.push(value);value=''}
    else if((ch==='\n'||ch==='\r')&&!quoted){if(ch==='\r'&&content[i+1]==='\n')i++;row.push(value);value='';if(row.some(x=>x.trim()))rows.push(row);row=[]}
    else value+=ch;
  }
  row.push(value);if(row.some(x=>x.trim()))rows.push(row);
  return rows;
}
function renderPreview() {
  const root=$('#csv-preview');root.replaceChildren();
  const rows=csvRows(csvText);
  if(!rows.length){root.textContent='No CSV loaded.';return}
  const headers=rows[0].map(h=>h.trim().toLowerCase());
  const columns=[['graduate_id','ID'],['graduation_year','Year'],['program','Program'],['status','Status']].map(([key,label])=>[headers.indexOf(key),label]).filter(([i])=>i>=0);
  if(!columns.length){root.textContent='Edit CSV to check column headers.';return}
  const table=document.createElement('table');table.className='preview-table';
  const head=document.createElement('thead');const tr=document.createElement('tr');
  for(const [,label] of columns){const th=document.createElement('th');th.textContent=label;tr.append(th)}
  head.append(tr);table.append(head);
  const body=document.createElement('tbody');
  for(const row of rows.slice(1,7)) {
    const r=document.createElement('tr');for(const [index] of columns){const td=document.createElement('td');td.textContent=row[index]||'—';r.append(td)}body.append(r)
  }
  table.append(body);root.append(table);
  const foot=document.createElement('div');foot.className='preview-foot';foot.textContent=`${rows.length-1} data rows · showing ${Math.min(rows.length-1,6)}`;root.append(foot);
}
function markSelected(name) {
  for (const id of ['valid','problems']) {
    const active=id===name;$('#'+id).classList.toggle('active',active);$('#'+id).setAttribute('aria-pressed',String(active));
  }
}
function updateHealth(name, ok) {
 const el=document.querySelector('#engine-'+name.toLowerCase());
 el.classList.toggle('ready',ok);el.classList.toggle('offline',!ok);
 el.querySelector('.status-word').textContent=ok?'Ready':'Not reachable';
}

async function api(path, body) {
  const res = await fetch(path, body ? {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)} : {});
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}
function text(selector, value) {$(selector).textContent = value;}
async function sample(name) {
  const response=await api('/api/samples/'+name);
  csvText=response.csv;
  $('#csv').value=csvText;
  $('#source-note').textContent = 'CSV data';
  markSelected(name);renderPreview();
}
async function refresh(name) {
  const id=ids[name];
  try {
    const data=await api('/api/jobs/status?emulator='+encodeURIComponent(name));
    text(`#${id}-visible`,data.queue.visibleApprox);
    text(`#${id}-flight`,data.queue.inFlightApprox);
    const box=$(`#${id}-history`);box.replaceChildren();
    if (!data.jobs.length) box.textContent='No jobs yet.';
    for (const job of data.jobs) {
      const row=document.createElement('div');row.className='job';
      const a=document.createElement('span');a.textContent=job.jobId;
      const b=document.createElement('b');b.textContent=job.status;
      const c=document.createElement('span');c.textContent=job.note||'';
      row.append(a,b,c);box.append(row);
    }
    const latest = data.jobs[0];
    const summary = $(`#${id}-latest`);
    if (!latest) {
      summary.textContent = 'No jobs submitted yet.';
      summary.className = 'current-job';
    } else {
      const status = String(latest.status || 'unknown').toLowerCase();
      const notes = {
        queued: 'Waiting to be picked up. Click Process next job.',
        interrupted: 'The worker stopped before processing. The SQS message was NOT deleted. It should become available for retry after the visibility timeout. Click Process next job to try again.',
        failed: 'Processing failed; the message was not acknowledged. You can attempt a retry after the visibility timeout.',
        completed: 'The report was generated and the SQS message was deleted.'
      };
      summary.textContent = `Latest job ${latest.jobId || ''} · ${status.toUpperCase()} — ${notes[status] || latest.note || ''}`;
      summary.className = 'current-job job-' + status;
    }
    text(`#${id}-state`,'Ready');updateHealth(name,true);
  } catch (err) {text(`#${id}-state`,'Unavailable');text(`#${id}-response`,err.message);updateHealth(name,false)}
}
async function refreshBoth(){await Promise.all(['FakeCloud','LocalStack'].map(refresh))}
function setResponse(name, message, kind=''){
  const el=$(`#${ids[name]}-response`);el.className='response '+kind;el.textContent=message;
}
$('#valid').addEventListener('click',()=>sample('valid'));
$('#problems').addEventListener('click',()=>sample('problems'));
$('#csv').addEventListener('input',()=>{csvText=$('#csv').value;text('#source-note','Edited CSV');markSelected(null);renderPreview()});
$('#csv-file').addEventListener('change', async event=>{const file=event.target.files[0];if(!file)return;if(file.size>120000){text('#source-note','File too large');return;}csvText=await file.text();$('#csv').value=csvText;text('#source-note',file.name);markSelected(null);renderPreview()});
$('#submit').addEventListener('click',async()=>{
  const targets=['FakeCloud','LocalStack'].filter(n=>$('#'+n.toLowerCase()).checked);
  if(!targets.length){alert('Select at least one emulator');return}
  $('#submit').disabled=true;
  targets.forEach(n=>setResponse(n,'Saving CSV and sending an SQS message…'));
  try{const data=await api('/api/jobs/submit',{csv:csvText, targets});for(const [name,job] of Object.entries(data.jobs))setResponse(name,`Queued job ${job.jobId}. S3 object stored; message sent to SQS; DynamoDB status: queued.`,'ok')}
  catch(err){targets.forEach(n=>setResponse(n,'Could not queue: '+err.message,'problem'))}
  finally{$('#submit').disabled=false;await refreshBoth()}
});
for(const button of document.querySelectorAll('[data-action]')) button.addEventListener('click',async()=>{
  const name=button.closest('[data-emulator]').dataset.emulator;
  button.disabled=true;
  setResponse(name,button.dataset.action==='interrupt'?'Receiving one job without deleting it…':'Worker is receiving a job and invoking Lambda…');
  try{
    const response=await api('/api/jobs/'+button.dataset.action,{emulator:name});
    if(response.state==='completed')setResponse(name,`Job ${response.jobId}: Lambda completed the graduate report (${response.accepted} accepted, ${response.rejected} rejected). DynamoDB updated and SQS message deleted. Delivery #${response.receiveCount}.`,'ok');
    else if(response.state==='interrupted')setResponse(name,`Job ${response.jobId}: worker interrupted before Lambda ran. Message not deleted. After about 8 seconds, click Process next job to retry (delivery #${response.receiveCount}).`,'problem');
    else if(response.state==='failed')setResponse(name,`Job ${response.jobId} failed: ${response.error}. SQS message was NOT deleted. Retry after visibility timeout.`,'problem');
    else setResponse(name,'SQS returned no visible message. If you interrupted a job, wait a moment and try again; the approximate counts may lag.');
  }catch(err){setResponse(name,err.message,'problem')}finally{button.disabled=false;await refresh(name)}
});
$('#refresh').addEventListener('click',refreshBoth);
sample('valid').then(refreshBoth).catch(err=>text('#source-note',err.message));
