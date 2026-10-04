const sourceData={
  source_a:{icon:'▤',label:'source_a',count:'4 rows',columns:['record_id','lookup_id','group_id','value','event_date'],rows:[['R-001','L-12','G-01',2,'2026-09-01'],['R-002','L-08','G-02',1,'2026-09-01'],['R-003','L-12','G-01',3,'2026-09-02'],['R-004','L-04','G-03',2,'2026-09-02']]},
  source_b:{icon:'◫',label:'source_b',count:'4 rows',columns:['lookup_id','descriptor','multiplier','type'],rows:[['L-12','Item A',18.5,'Type 1'],['L-08','Item B',32,'Type 2'],['L-04','Item C',12,'Type 1'],['L-21','Item D',15.25,'Type 3']]},
  source_c:{icon:'⌂',label:'source_c',count:'4 rows',columns:['group_id','group_label','segment'],rows:[['G-01','Group One','Segment A'],['G-02','Group Two','Segment B'],['G-03','Group Three','Segment A'],['G-04','Group Four','Segment C']]}
};
const transformedRows=[['Group One','Segment A',2,92.5],['Group Two','Segment B',1,32],['Group Three','Segment A',1,24]];
const outputColumns=['group_label','segment','record_count','total_value'];
const outputFileName='transformed_output.csv';
const outputPath='/workspace/output/';
const languageSamples={sql:`SELECT
    c.group_label,
    c.segment,
    COUNT(*) AS record_count,
    ROUND(SUM(a.value * b.multiplier), 2) AS total_value
FROM source_a AS a
JOIN source_b AS b ON a.lookup_id = b.lookup_id
JOIN source_c AS c ON a.group_id = c.group_id
GROUP BY c.group_label, c.segment
ORDER BY total_value DESC;`,python:`from pyspark.sql import functions as F

# Join the input tables and calculate a derived value
source_a = spark.table("source_a")
source_b = spark.table("source_b")
source_c = spark.table("source_c")

transformed = (source_a
    .join(source_b, "lookup_id")
    .join(source_c, "group_id")
    .groupBy("group_label", "segment")
    .agg(
        F.count("*").alias("record_count"),
        F.round(F.sum(F.col("value") * F.col("multiplier")), 2)
          .alias("total_value")
    ))

display(transformed)`,pyspark:`from pyspark.sql import functions as F

transformed = (source_a_df
    .join(source_b_df, "lookup_id")
    .join(source_c_df, "group_id")
    .groupBy("group_label", "segment")
    .agg(
        F.count("*").alias("record_count"),
        F.round(F.sum(F.col("value") * F.col("multiplier")), 2)
          .alias("total_value")
    )
)
display(transformed)`};
const $=s=>document.querySelector(s),$$=s=>[...document.querySelectorAll(s)];let completed=false,outputReady=false,toastTimer,nextCellNumber=2;
function toast(msg){const el=$('#toast');el.textContent=msg;el.classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>el.classList.remove('show'),2500)}
function renderTables(){
  const entries=Object.entries(sourceData);
  $('#tableCount').textContent=entries.length;
  $('#tableList').innerHTML=entries.map(([key,t])=>`<div class="table-item" data-table="${key}"><div class="table-row"><button class="table-main" type="button" aria-label="Preview ${key}"><span class="table-icon ${key}">${t.icon||'▤'}</span><b>${t.label}</b><span class="row-count">${t.rows.length} rows</span><span class="table-chevron">›</span></button><span class="table-controls"><button class="table-action edit-table" type="button" title="Edit ${key}" aria-label="Edit ${key}">✎</button><button class="table-action delete-table" type="button" title="Delete ${key}" aria-label="Delete ${key}">×</button></span></div><div class="table-schema" hidden>${t.columns.map(c=>`<code>${escapeHtml(c)}</code>`).join('')}</div></div>`).join('');
  $$('.table-main').forEach(btn=>btn.addEventListener('click',()=>{const el=btn.closest('.table-item'),key=el.dataset.table,was=el.classList.contains('open');$$('.table-item').forEach(x=>{x.classList.remove('open');x.querySelector('.table-schema').hidden=true});if(!was){el.classList.add('open');el.querySelector('.table-schema').hidden=false}showTable(key)}));
  $$('.edit-table').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();openTableEditor(btn.closest('.table-item').dataset.table)}));
  $$('.delete-table').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();const key=btn.closest('.table-item').dataset.table;if(!confirm(`Delete the table “${key}”?`))return;delete sourceData[key];renderTables();toast(`Deleted table: ${key}`)}));
}
function showTable(key){const t=sourceData[key];$('#modalTitle').textContent=t.label;$('#modalTable').innerHTML=tableHtml(t.columns,t.rows);$('#tableModal').hidden=false}
function tableHtml(cols,rows){return `<table class="result-table"><thead><tr>${cols.map(c=>`<th>${escapeHtml(c)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${cols.map((_,i)=>`<td>${escapeHtml(row[i]??'')}</td>`).join('')}</tr>`).join('')}</tbody></table>`}
function openTableEditor(key=null){const form=$('#tableForm');form.dataset.originalKey=key||'';$('#editModalTitle').textContent=key?'Edit table':'Add a table';$('#tableNameInput').value=key?sourceData[key].label:'';$('#tableColumnsInput').value=key?sourceData[key].columns.join(', '):'';$('#tableRowsInput').value=key?sourceData[key].rows.map(row=>row.map(csvValue).join(', ')).join('\n'):'';$('#tableFormError').textContent='';$('#editTableModal').hidden=false;$('#tableNameInput').focus()}
function csvValue(value){const text=String(value);return /[",\n]/.test(text)?`"${text.replaceAll('"','""')}"`:text}
function parseCsvLine(line){const out=[];let value='',quoted=false;for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"'&&quoted&&line[i+1]==='"'){value+='"';i++}else if(ch==='"')quoted=!quoted;else if(ch===','&&!quoted){out.push(value.trim());value=''}else value+=ch}out.push(value.trim());return out}
function saveTable(event){event.preventDefault();const original=$('#tableForm').dataset.originalKey,name=$('#tableNameInput').value.trim().toLowerCase().replace(/\s+/g,'_'),columns=$('#tableColumnsInput').value.split(',').map(x=>x.trim()).filter(Boolean),lines=$('#tableRowsInput').value.split(/\r?\n/).filter(x=>x.trim()),error=$('#tableFormError');if(!/^[a-z][a-z0-9_]*$/.test(name)){error.textContent='Use a name starting with a letter; use only letters, numbers, and underscores.';return}if(!columns.length||new Set(columns).size!==columns.length){error.textContent='Add at least one column and make each column name unique.';return}if(!lines.length){error.textContent='Add at least one row.';return}const rows=lines.map(parseCsvLine);if(rows.some(r=>r.length!==columns.length)){error.textContent=`Every row must have exactly ${columns.length} values to match the columns.`;return}if(name!==original&&sourceData[name]){error.textContent='A table with that name already exists.';return}if(original&&name!==original)delete sourceData[original];sourceData[name]={label:name,icon:'▤',columns,rows};renderTables();$('#editTableModal').hidden=true;toast(original?'Table updated':'Table added')}
function resultHtml(){return `<div class="result-table-wrap">${tableHtml(outputColumns,transformedRows)}</div><div class="result-caption">${transformedRows.length} rows returned · Completed in 0.8s · Generic training data</div>`}
function runNotebook(){const b=$('#runNotebook'),ind=$('#cell1Indicator');b.disabled=true;b.innerHTML='<span>◌</span> Running…';ind.className='run-indicator running';$('#cellOutput').innerHTML='<div class="output-empty">Running cell against generic training data…</div>';setTimeout(()=>{b.disabled=false;b.innerHTML='<span>▶</span> Run all';ind.className='run-indicator success';$('#cellOutput').innerHTML=resultHtml();completed=true;updateProgress(55);toast(`Notebook ran successfully · ${transformedRows.length} rows returned`)},850)}
function updateProgress(val){$('#progressPercent').textContent=val+'%';$('#progressBar').style.width=val+'%';$$('.path-item').forEach((el,i)=>{if(i<Math.floor(val/20)){el.classList.add('done');el.classList.remove('current');el.querySelector('.path-bullet').textContent='✓'}else if(i===Math.floor(val/20)){el.classList.add('current')}})}
function switchTab(name){$$('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===name));$$('.tab-panel').forEach(p=>{const active=p.id===name+'Panel';p.classList.toggle('active',active);p.hidden=!active})}
function renderRunLog(){const log=$('#runLog');log.innerHTML=`<div class="log-title">DEBUG RUN · ${new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}</div><div class="log-step"><span>✓</span> Read input tables <b class="status success">Succeeded</b></div><div class="log-step"><span>✓</span> Run Databricks notebook <b class="status success">Succeeded</b></div><div class="log-step"><span>✓</span> Validate output rows <b class="status success">Succeeded · ${transformedRows.length} rows</b></div><div class="log-step"><span>✓</span> Write ${outputFileName} <b class="status success">Succeeded</b></div>`}
function createOutput(){outputReady=true;$('#fileBadge').textContent='1';$('#outputContent').innerHTML=`<div class="file-card"><div class="file-icon">CSV</div><div class="file-meta"><b>${outputFileName}</b><small>CSV · ${transformedRows.length} rows · ${outputPath}</small></div><div class="file-actions"><button id="previewFile">Preview</button><button id="downloadFile">↓ Download</button></div></div><div class="result-table-wrap" style="margin:15px">${tableHtml(outputColumns,transformedRows)}</div>`;$('#previewFile').onclick=()=>{switchTab('notebook');$('#cellOutput').innerHTML=resultHtml();toast('Showing the generated file preview in the notebook')};$('#downloadFile').onclick=downloadCsv}
function downloadCsv(){const csv=[outputColumns,...transformedRows].map(r=>r.map(v=>`"${String(v).replaceAll('"','""')}"`).join(',')).join('\n');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));a.download=outputFileName;a.click();URL.revokeObjectURL(a.href);toast('Downloaded generated output CSV')}
function runPipeline(){switchTab('pipeline');const b=$('#runPipeline');b.disabled=true;b.innerHTML='<span>◌</span> Running…';const nodes=$$('.pipe-node');nodes.forEach(n=>n.classList.remove('pipeline-active'));$('#runLog').innerHTML='<div class="log-empty">Starting debug run…</div>';const steps=['Read input tables','Run Databricks notebook','Validate output rows',`Write ${outputFileName}`];let i=0;function next(){if(i<nodes.length)nodes[i].classList.add('pipeline-active');if(i<steps.length){$('#runLog').innerHTML=`<div class="log-title">DEBUG RUN · IN PROGRESS</div>${steps.map((s,j)=>`<div class="log-step"><span>${j<i?'✓':j===i?'◌':'○'}</span> ${s}<b class="status ${j<i?'success':j===i?'running':'pending'}">${j<i?'Succeeded':j===i?'Running':'Waiting'}</b></div>`).join('')}`;i++;setTimeout(next,650)}else{nodes.forEach(n=>n.classList.remove('pipeline-active'));renderRunLog();createOutput();b.disabled=false;b.innerHTML='<span>▶</span> Debug pipeline';updateProgress(100);toast(`Pipeline succeeded · ${outputFileName} is ready`)}}next()}
function addActivity(){const flow=$('#pipelineFlow');const node=document.createElement('div');node.className='pipe-node additional-node';node.innerHTML='<div class="node-icon" style="background:#fff4e5;color:#d49436">✓</div><div><small>ACTIVITY</small><b>Validate rows</b><span>Row count > 0</span></div><span class="node-menu">···</span>';const connector=document.createElement('div');connector.className='pipe-connector additional-node';flow.insertBefore(connector,flow.lastElementChild);flow.insertBefore(node,flow.lastElementChild);toast('Added a validation activity to your pipeline')}
function openAssistant(){const d=$('#assistantDrawer');d.classList.add('open');d.setAttribute('aria-hidden','false');setTimeout(()=>$('#chatInput').focus(),200)}
function assistantReply(prompt){const p=prompt.toLowerCase();let answer;if(p.includes('hint'))answer='Start with <code>source_a</code>. Look for columns that match across the tables, then use those as join keys. Which fields should appear in your output?';else if(p.includes('join'))answer='A join combines rows when key values match. In this exercise, <code>lookup_id</code> links source_a and source_b, while <code>group_id</code> links source_a and source_c. Try tracing one record across the tables.';else if(p.includes('pipeline')||p.includes('factory')||p.includes('adf'))answer='This simulated pipeline has three stages: it reads the input tables, runs your notebook transformation, and writes the result as a CSV file. In a real ADF workflow, activities connect these steps and a debug run helps you inspect their status.';else if(p.includes('pyspark')||p.includes('python'))answer='In PySpark, you describe transformations with DataFrame operations such as <code>join</code>, <code>groupBy</code>, and <code>agg</code>. The notebook cell language menu lets you compare SQL, Python, and PySpark for this same task.';else if(p.includes('file')||p.includes('csv'))answer=`The final step writes the transformed rows to <code>${outputPath}${outputFileName}</code>. Run the simulated pipeline, then open Output files to preview or download the generated CSV.`;else answer='For this task, combine the available tables, calculate a derived value, and group the result by an attribute. You can ask me for a hint, an explanation, or help understanding an error. Examples use generic training data.';addMessage(answer,'assistant-message',true)}
function addMessage(text,cls,html=false){const d=document.createElement('div');d.className='chat-message '+cls;d.innerHTML=html?text:escapeHtml(text);$('#chatMessages').appendChild(d);$('#chatMessages').scrollTop=$('#chatMessages').scrollHeight}
function escapeHtml(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML}
$('#codeEditor').value=languageSamples.sql;renderTables();
$('#addTableButton').addEventListener('click',()=>openTableEditor());$('#tableForm').addEventListener('submit',saveTable);$('#closeEditModal').addEventListener('click',()=>$('#editTableModal').hidden=true);$('#cancelEditTable').addEventListener('click',()=>$('#editTableModal').hidden=true);$('#editTableModal').addEventListener('click',e=>{if(e.target===$('#editTableModal'))$('#editTableModal').hidden=true});
$('#languageSelect').addEventListener('change',e=>{$('#codeEditor').value=languageSamples[e.target.value];toast('Showing a '+e.target.options[e.target.selectedIndex].text+' example')});
$('#runNotebook').addEventListener('click',runNotebook);$('#runCell').addEventListener('click',runNotebook);$('#runPipeline').addEventListener('click',runPipeline);
$$('.tab').forEach(t=>t.addEventListener('click',()=>switchTab(t.dataset.tab)));
$$('[data-goto]').forEach(b=>b.addEventListener('click',()=>switchTab(b.dataset.goto)));
$('#askAssistant').addEventListener('click',openAssistant);$('#closeAssistant').addEventListener('click',()=>{$('#assistantDrawer').classList.remove('open');$('#assistantDrawer').setAttribute('aria-hidden','true')});
$('#chatForm').addEventListener('submit',e=>{e.preventDefault();const input=$('#chatInput'),v=input.value.trim();if(!v)return;addMessage(v,'user-message');input.value='';setTimeout(()=>assistantReply(v),350)});
$$('[data-prompt]').forEach(b=>b.addEventListener('click',()=>{const v=b.dataset.prompt;addMessage(v,'user-message');setTimeout(()=>assistantReply(v),300)}));
$('#dismissChallenge').addEventListener('click',()=>$('.mission-banner').remove());$('#addActivity').addEventListener('click',addActivity);$('#fitCanvas').addEventListener('click',()=>toast('Pipeline canvas centered'));
$('#addCell').addEventListener('click',()=>{
  const cellNumber=nextCellNumber++;
  const cell=document.createElement('div');
  cell.className='cell code-cell added-cell';
  cell.innerHTML=`<div class="cell-gutter"><span>${cellNumber}</span><span class="run-indicator"></span></div><div class="cell-body"><div class="cell-toolbar"><div class="language-picker"><select aria-label="Cell ${cellNumber} language"><option>SQL</option><option>Python</option><option>PySpark</option></select><span class="dropdown-arrow">⌄</span></div><span class="cell-toolbar-right">Cell ${cellNumber} <button class="mini-run" title="Run cell">▶</button></span></div><textarea spellcheck="false" placeholder="Write another transformation…"></textarea><div class="cell-output"><div class="output-empty">Run this cell to see the result.</div></div><div class="added-cell-actions"><button type="button" class="skip-cell">Skip cell</button><button type="button" class="discard-cell">Discard cell</button></div></div>`;
  $('.notebook-column').insertBefore(cell,$('#addCell'));
  cell.querySelector('.mini-run').onclick=()=>toast('This practice cell is ready to run.');
  cell.querySelector('.skip-cell').onclick=event=>{
    const skipped=cell.classList.toggle('skipped');
    cell.querySelector('textarea').disabled=skipped;
    cell.querySelector('select').disabled=skipped;
    cell.querySelector('.mini-run').disabled=skipped;
    event.currentTarget.textContent=skipped?'Resume cell':'Skip cell';
    event.currentTarget.setAttribute('aria-pressed',String(skipped));
    toast(skipped?`Cell ${cellNumber} skipped`:`Cell ${cellNumber} resumed`);
  };
  cell.querySelector('.discard-cell').onclick=()=>{cell.remove();toast(`Cell ${cellNumber} discarded`)};
  cell.querySelector('textarea').focus();
  toast(`Added cell ${cellNumber}`);
});
$('#resetButton').addEventListener('click',()=>{completed=false;outputReady=false;$('#codeEditor').value=languageSamples[$('#languageSelect').value];$('#cellOutput').innerHTML='<div class="output-empty">Run this cell to preview your transformed data.</div>';$('#cell1Indicator').className='run-indicator';$('#runLog').innerHTML='<div class="log-empty">Pipeline run details will appear here after you start a debug run.</div>';$('#outputContent').innerHTML='<div class="empty-state"><span>▧</span><b>No files yet</b><p>Run your pipeline to generate a fictional output file.</p><button class="text-link" data-goto="pipeline">Go to pipeline →</button></div>';$('#fileBadge').textContent='0';$$('.additional-node').forEach(n=>n.remove());updateProgress(20);$$('.tab-panel .text-link').forEach(b=>b.onclick=()=>switchTab('pipeline'));toast('Mission reset')});
$('#closeModal').addEventListener('click',()=>$('#tableModal').hidden=true);$('#tableModal').addEventListener('click',e=>{if(e.target===$('#tableModal'))$('#tableModal').hidden=true});document.addEventListener('keydown',e=>{if(e.key==='Escape'){$('#tableModal').hidden=true;$('#assistantDrawer').classList.remove('open')}});
$$('[data-toast]').forEach(b=>b.addEventListener('click',()=>toast(b.dataset.toast)));
$$('.text-link').forEach(b=>b.addEventListener('click',()=>switchTab('pipeline')));
