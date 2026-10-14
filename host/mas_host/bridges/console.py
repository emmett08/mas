"""Static same-origin console; all application data uses authenticated APIs."""
import base64
import hashlib


STYLE = """
:root{font:16px system-ui;color:#18212b;background:#f5f7fa}*{box-sizing:border-box}
body{max-width:1100px;margin:auto;padding:24px}h1{margin-bottom:4px}p{line-height:1.5}
section{background:white;border:1px solid #d7dfe8;border-radius:10px;padding:18px;margin:16px 0}
label{display:block;margin:12px 0 5px;font-weight:600}input,textarea,select{width:100%;padding:9px;border:1px solid #9ba9b8;border-radius:4px;font:inherit}
textarea{font-family:monospace;min-height:85px}button{padding:9px 15px;margin:10px 6px 0 0;border:1px solid #314e71;border-radius:4px;background:#173f6b;color:white;font:inherit;cursor:pointer}
button:disabled{opacity:.5;cursor:default}button.secondary{color:#173f6b;background:white}
pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:420px;overflow:auto;background:#f5f7fa;padding:12px;font-size:13px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.hint{color:#4b5d70;font-size:14px}
#status{white-space:pre-wrap;min-height:24px}#tasks button{display:block;width:100%;text-align:left;background:white;color:#173f6b;border-color:#c3ccd8}
@media(max-width:720px){body{padding:12px}.grid{grid-template-columns:1fr}}
"""

SCRIPT = """
'use strict';
const $=id=>document.getElementById(id); let token='',cursor=0,eventTask='',events=[],nextPage=null,selection=null,generation=0;
const requests=new Set();
const status=text=>{$('status').textContent=text;};
function parse(text){return JSON.parse(text,(_key,value)=>{if(typeof value==='number'&&!Number.isSafeInteger(value))throw Error('The browser requires safe integers. Use the Python client for larger integers or artefacts for scientific data.');return value;});}
function object(id){const value=parse($(id).value);if(!value||Array.isArray(value)||typeof value!=='object')throw Error('Input must be a JSON object.');return value;}
function reset(){generation++;for(const request of requests)request.abort();requests.clear();token='';selection=null;events=[];cursor=0;eventTask='';nextPage=null;
 for(const id of ['tasks','workflows'])$(id).replaceChildren();for(const id of ['detail','leaf-detail','events','definitions'])$(id).textContent='';
 for(const id of ['token','task','path','key','reply-key'])$(id).value='';for(const id of ['input','reply'])$(id).value='{}';$('more').disabled=true;
}
async function api(operation,input){
 if(!token)throw Error('Connect with your service token first.');
 const current=generation,controller=new AbortController();requests.add(controller);
 try{const response=await fetch('/service/'+operation,{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify(input),credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
 const text=await response.text();if(current!==generation)throw Error('Session changed.');const body=parse(text);if(!response.ok)throw Error(body.error?.code||'Request failed');return body.result;
 }finally{requests.delete(controller);}
}
async function action(work){const current=generation;try{status('Working…');await work();if(current===generation)status('Ready.');}catch(error){if(current===generation)status(String(error.message||'Operation failed'));}}
function show(value){selection=value;$('task').value=value.task_id;$('detail').textContent=JSON.stringify(value,null,2);}
async function refresh(){const value=await api('inspect',{task_id:$('task').value});show(value);}
async function list(more=false){
 const query={page_size:30};if(more&&nextPage)query.after=nextPage;if($('state').value)query.state=$('state').value;
 const page=await api('page',query);if(!more)$('tasks').replaceChildren();
 for(const task of page.tasks){const button=document.createElement('button');button.textContent=task.workflow+' · '+task.state+' · '+task.task_id;button.addEventListener('click',()=>action(async()=>{show(task);await refresh();}));$('tasks').append(button);}
 nextPage=page.next;$('more').disabled=!nextPage;
}
async function discover(){const data=await api('discover',{});$('workflows').replaceChildren();
 for(const item of data.registrations){const option=document.createElement('option');option.value=item.name;option.textContent=item.name+(item.available?'':' (unavailable)');option.disabled=!item.available;$('workflows').append(option);}
 $('definitions').textContent=JSON.stringify(data,null,2);
}
async function progress(){const task=$('task').value;if(eventTask!==task){cursor=0;events=[];eventTask=task;}
 const after=cursor,page=await api('events_page',{task_id:task,after,limit:100});if(task!==$('task').value||eventTask!==task||cursor!==after)return;cursor=page.next_after;events.push(...page.events);events=events.slice(-500);$('events').textContent=events.map(x=>JSON.stringify(x)).join('\\n');
}
$('connect').onclick=()=>{const supplied=$('token').value.trim();reset();token=supplied;return action(async()=>{await discover();await list();});};
$('disconnect').onclick=()=>{reset();status('Disconnected. Token and displayed data cleared.');};
$('refresh-list').onclick=()=>action(()=>list());$('more').onclick=()=>action(()=>list(true));
$('submit').onclick=()=>action(async()=>{show(await api('submit',{workflow:$('workflows').value,request_key:$('key').value,input:object('input')}));await list();});
$('inspect').onclick=()=>action(refresh);
$('cancel').onclick=()=>action(async()=>{show(await api('cancel',{task_id:$('task').value}));});
$('recover').onclick=()=>action(async()=>{show(await api('recover',{task_id:$('task').value}));});
$('continue').onclick=()=>action(async()=>{let input=object('reply');if(selection?.kind==='plan')input={path:$('path').value,input};show(await api('update',{task_id:$('task').value,request_key:$('reply-key').value,input}));});
$('leaf').onclick=()=>action(async()=>{$('leaf-detail').textContent=JSON.stringify(await api('task',{task_id:$('task').value,path:$('path').value}),null,2);});
$('progress').onclick=()=>action(progress);
$('new-key').onclick=()=>{$('key').value=crypto.randomUUID();};
window.addEventListener('pagehide',reset);
status('Connect to inspect your authorised tasks.');
"""

BODY = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAS operations</title><style>STYLE</style><body>
<h1>MAS operations</h1><p class="hint">Submit work, follow progress and complete human input. The service enforces your permissions on every action.</p>
<section><label for="token">Service token</label><input id="token" type="password" autocomplete="off" spellcheck="false">
<button id="connect">Connect</button><button id="disconnect" class="secondary">Disconnect</button>
<p class="hint">The token stays in this page's memory. Refreshing or closing the page clears it.</p><p id="status" role="status" aria-live="polite"></p></section>
<div class="grid"><section><h2>Your tasks</h2><label for="state">State</label><select id="state"><option value="">All states</option><option>queued</option><option>running</option><option>input_required</option><option>auth_required</option><option>blocked</option><option>unknown</option><option>completed</option><option>failed</option><option>cancelled</option></select>
<button id="refresh-list">Refresh list</button><div id="tasks"></div><button id="more" disabled>Load more</button></section>
<section><h2>Submit work</h2><label for="workflows">Registration</label><select id="workflows"></select>
<label for="key">Request key</label><input id="key" maxlength="200"><button id="new-key" class="secondary">New key</button>
<p class="hint">Keep the same key when checking an interrupted submission. A new key creates new work.</p>
<label for="input">Input JSON</label><textarea id="input" maxlength="64000">{}</textarea><button id="submit">Submit</button>
<details><summary>Authorised registrations</summary><pre id="definitions"></pre></details></section></div>
<section><h2>Task detail</h2><label for="task">Task ID</label><input id="task" maxlength="200"><button id="inspect">Inspect</button><button id="cancel" class="secondary">Request cancellation</button><button id="recover" class="secondary">Recover after resolution</button>
<pre id="detail"></pre><p class="hint">Unknown effects require evidence-backed resolution. Cancellation intent does not prove a remote effect stopped.</p>
<label for="path">Composed leaf path</label><input id="path" maxlength="500" placeholder="root/0"><button id="leaf" class="secondary">Inspect leaf</button><pre id="leaf-detail"></pre>
<label for="reply-key">Continuation request key</label><input id="reply-key" maxlength="200"><label for="reply">Human input JSON</label><textarea id="reply" maxlength="64000">{}</textarea><button id="continue">Continue paused work</button></section>
<section><h2>Progress</h2><button id="progress">Read next events</button><p class="hint">Retained events resume from the last received ID. Archived history may require inspecting the compact receipt.</p><pre id="events"></pre></section>
<script>SCRIPT</script></body></html>"""


def console_response():
    digest = lambda value: base64.b64encode(hashlib.sha256(value.encode()).digest()).decode()
    content = BODY.replace('STYLE', STYLE).replace('SCRIPT', SCRIPT).encode()
    return content, {
        'Content-Security-Policy': "default-src 'none'; connect-src 'self'; script-src 'sha256-" + digest(SCRIPT)
            + "'; style-src 'sha256-" + digest(STYLE) + "'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        'Referrer-Policy': 'no-referrer',
        'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
    }
