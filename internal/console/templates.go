package console

const loginPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Sign in · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{min-height:100vh;margin:0;display:grid;place-items:center;padding:24px;background:radial-gradient(circle at 50% 0,#161f2d 0,#0d1117 42%)}.shell{width:min(340px,100%);text-align:center}.mark{display:grid;place-items:center;width:52px;height:52px;margin:0 auto 24px;border:1px solid #30363d;border-radius:12px;background:#161b22;color:#58a6ff;font-size:24px;font-weight:800;box-shadow:0 8px 24px #01040980}h1{margin:0 0 20px;font-size:24px;font-weight:300;letter-spacing:-.01em}.card{padding:20px;text-align:left;border:1px solid #30363d;border-radius:6px;background:#161b22;box-shadow:0 8px 24px #01040966}.hint{margin:0 0 16px;color:#8b949e;font-size:14px;line-height:1.5}label{display:block;margin:0 0 8px;font-size:14px;font-weight:600}input,button{width:100%;border-radius:6px;font:inherit}input{height:34px;padding:5px 12px;border:1px solid #30363d;outline:none;background:#0d1117;color:#f0f6fc;box-shadow:inset 0 0 0 1px transparent}input:focus{border-color:#2f81f7;box-shadow:0 0 0 3px #2f81f74d}button{height:34px;margin-top:16px;border:1px solid #2ea043;background:#238636;color:#fff;font-size:14px;font-weight:600;cursor:pointer;box-shadow:0 1px 0 #1f6f32 inset}button:hover{background:#2ea043}button:disabled{opacity:.6;cursor:wait}#error{min-height:20px;margin:12px 0 0;color:#f85149;font-size:13px;text-align:center}.footer{margin:20px 0 0;color:#6e7681;font-size:12px}
  </style>
</head>
<body>
  <main class="shell">
    <div class="mark" aria-hidden="true">OA</div>
    <h1>Sign in to Open Actions</h1>
    <form class="card" id="login">
      <p class="hint">Use the administrator token configured for this Console.</p>
      <label for="token">Administrator token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" required autofocus>
      <button type="submit">Sign in</button>
      <p id="error" role="alert"></p>
    </form>
    <p class="footer">Workflow runs and live runner logs</p>
  </main>
  <script>
    const form=document.getElementById('login');const error=document.getElementById('error');const button=form.querySelector('button');
    form.addEventListener('submit',async event=>{event.preventDefault();error.textContent='';button.disabled=true;
      try{const response=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:form.token.value})});
        if(!response.ok){error.textContent='The token is invalid.';return}
        const next=new URLSearchParams(location.search).get('next');location.assign(next&&next.startsWith('/runs/')?next:'/');
      }catch(_){error.textContent='The Console is unavailable.'}finally{button.disabled=false}
    });
  </script>
</body>
</html>`

const mainPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Workflow runs · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1180px,100%);margin:0 auto;padding:32px 32px 56px}.page-heading{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;margin-bottom:20px}.page-heading h1{margin:0 0 6px;font-size:24px;font-weight:600;letter-spacing:-.01em}.page-heading p{margin:0;color:#8b949e}.count{min-width:20px;padding:2px 8px;border-radius:20px;background:#30363d;color:#c9d1d9;font-size:12px;text-align:center}.runs{overflow:hidden;border:1px solid #30363d;border-radius:6px}.run{display:grid;grid-template-columns:minmax(280px,1fr) minmax(150px,.55fr) 130px 170px;align-items:center;gap:20px;min-height:76px;padding:14px 16px;border-bottom:1px solid #21262d}.run:last-child{border-bottom:0}.run:hover{background:#161b2266}.run-link{display:flex;align-items:center;gap:12px;min-width:0;color:#f0f6fc}.run-link:hover{color:#58a6ff;text-decoration:none}.status-mark{display:grid;place-items:center;flex:0 0 auto;width:20px;height:20px;border:2px solid currentColor;border-radius:50%;font-size:11px;font-weight:800}.status-mark.succeeded{color:#3fb950}.status-mark.failed{color:#f85149}.status-mark.running{color:#d29922;border-style:dashed}.status-mark.queued{color:#8b949e}.status-mark.succeeded::after{content:"✓"}.status-mark.failed::after{content:"×"}.status-mark.queued::after{content:"·"}.run-name{min-width:0}.run-name strong,.run-name small{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.run-name small{margin-top:4px;color:#8b949e;font-weight:400}.revision{min-width:0;color:#c9d1d9}.revision span{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.revision code{display:inline-block;margin-top:4px;color:#8b949e;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.detail{color:#8b949e;font-size:13px}.status{color:#c9d1d9}.status small{display:block;margin-top:4px;color:#8b949e}.time{display:block;margin-top:4px}.empty{padding:52px 24px;text-align:center}.empty strong{display:block;margin-bottom:6px;font-size:16px}.empty span{color:#8b949e}@media(max-width:800px){.topbar{padding:0 16px}.page{padding:24px 16px 40px}.page-heading{align-items:flex-start}.run{grid-template-columns:minmax(0,1fr) auto}.run .revision,.run .event{display:none}.run .status{text-align:right}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a></nav>
  <main class="page">
    <header class="page-heading"><div><h1>Workflow runs</h1><p>Workflow activity across all namespaces{{if .Truncated}} · Showing {{.Limit}} most recent runs{{end}}</p></div><span class="count" aria-label="Workflow run count">{{len .Runs}}</span></header>
    <section class="runs" aria-label="Workflow runs">
      {{range .Runs}}<article class="run">
        <a class="run-link" href="{{.URL}}"><span class="status-mark {{.StatusClass}}" aria-hidden="true"></span><span class="run-name"><strong>{{.WorkflowName}}</strong><small>{{.Repository}} · {{.Namespace}}/{{.Project}}</small></span></a>
        <div class="revision"><span>{{if .RefName}}{{.RefName}}{{else}}—{{end}}</span><code title="{{.Revision}}">{{.ShortRevision}}</code></div>
        <div class="detail event">{{.Event}}</div>
        <div class="detail status"><span>{{.Status}}</span>{{if .Duration}}<small>{{.Duration}}</small>{{end}}{{if .Created}}<time class="time" data-time="{{.Created}}">{{.Created}}</time>{{end}}</div>
      </article>{{else}}<div class="empty"><strong>No workflow runs</strong><span>Runs will appear here when a configured project receives a supported event.</span></div>{{end}}
    </section>
  </main>
  <script>for(const element of document.querySelectorAll('[data-time]')){const date=new Date(element.dataset.time);if(!Number.isNaN(date.valueOf()))element.textContent=new Intl.DateTimeFormat(undefined,{dateStyle:'medium',timeStyle:'short'}).format(date)}</script>
</body>
</html>`

const runPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{.WorkflowName}} · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1180px,100%);margin:0 auto;padding:28px 32px 56px}.breadcrumbs{display:flex;align-items:center;gap:8px;margin-bottom:22px;color:#8b949e;font-size:14px}.breadcrumbs strong{color:#f0f6fc}.run-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:22px}.run-heading h1{margin:0 0 8px;font-size:24px;font-weight:600;line-height:1.25;letter-spacing:-.01em}.muted{color:#8b949e}.workflow-path{overflow-wrap:anywhere}.summary{display:grid;grid-template-columns:auto 1fr;gap:14px;padding:20px;border:1px solid #30363d;border-radius:6px;background:#0d1117}.status-mark{display:grid;place-items:center;flex:0 0 auto;width:24px;height:24px;border:2px solid currentColor;border-radius:50%;font-size:14px;font-weight:800}.status-mark.succeeded{color:#3fb950}.status-mark.failed{color:#f85149}.status-mark.running{color:#d29922;border-style:dashed}.status-mark.queued{color:#8b949e}.status-mark.succeeded::after{content:"✓"}.status-mark.failed::after{content:"×"}.status-mark.running::after{content:""}.status-mark.queued::after{content:"·"}.summary-title{margin:1px 0 5px;font-size:16px;font-weight:600}.summary-meta{display:flex;flex-wrap:wrap;gap:6px 14px;color:#8b949e;font-size:13px}.summary-meta code{padding:2px 5px;border:1px solid #30363d;border-radius:5px;background:#161b22;color:#c9d1d9;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.section-title{display:flex;align-items:center;gap:8px;margin:30px 0 12px;font-size:16px;font-weight:600}.count{min-width:20px;padding:1px 6px;border-radius:20px;background:#30363d;color:#c9d1d9;font-size:12px;text-align:center}.jobs{overflow:hidden;border:1px solid #30363d;border-radius:6px}.job{display:grid;grid-template-columns:minmax(220px,1fr) 140px 180px 150px;align-items:center;min-height:68px;padding:12px 16px;border-bottom:1px solid #21262d}.job:last-child{border-bottom:0}.job:hover{background:#161b2266}.job-link{display:flex;align-items:center;gap:12px;min-width:0;color:#f0f6fc;font-weight:600}.job-link:hover{color:#58a6ff;text-decoration:none}.job-status{width:18px;height:18px;font-size:11px}.job-name{min-width:0}.job-name span{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.job-name small{display:block;margin-top:3px;color:#8b949e;font-weight:400}.cell{color:#8b949e;font-size:13px}.status-text{color:#c9d1d9}.empty{padding:32px;text-align:center;color:#8b949e}@media(max-width:760px){.topbar{padding:0 16px}.page{padding:22px 16px 40px}.run-heading{display:block}.summary{padding:16px}.job{grid-template-columns:1fr auto;gap:8px}.job .runner,.job .started{display:none}.job .status-text{text-align:right}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a></nav>
  <main class="page">
    <div class="breadcrumbs"><strong>{{.Repository}}</strong><span>/</span><span>Actions</span><span>/</span><span>{{.WorkflowName}}</span></div>
    <div class="run-heading">
      <div><h1>{{.WorkflowName}}</h1><div class="muted workflow-path">{{.WorkflowPath}}</div></div>
    </div>
    <section class="summary" aria-label="Workflow run summary">
      <span class="status-mark {{.StatusClass}}" aria-hidden="true"></span>
      <div>
        <div class="summary-title">Workflow run {{.Status}}</div>
        <div class="summary-meta"><span>{{.RefName}}</span><code title="{{.Revision}}">{{.ShortRevision}}</code>{{if .Started}}<time data-time="{{.Started}}">{{.Started}}</time>{{end}}{{if .Duration}}<span>{{.Duration}}</span>{{end}}</div>
      </div>
    </section>
    <h2 class="section-title">Jobs <span class="count">{{len .Jobs}}</span></h2>
    <section class="jobs" aria-label="Jobs">
      {{range .Jobs}}<div class="job">
        <a class="job-link" href="{{.URL}}"><span class="status-mark job-status {{.StatusClass}}" aria-hidden="true"></span><span class="job-name"><span>{{.DisplayName}}</span>{{if ne .DisplayName .ID}}<small>{{.ID}}</small>{{end}}</span></a>
        <span class="cell status-text">{{.Status}}</span><span class="cell runner">{{if .Runner}}{{.Runner}}{{else}}—{{end}}</span><span class="cell started">{{if .Duration}}{{.Duration}}{{else if .Started}}In progress{{else}}Not started{{end}}</span>
      </div>{{else}}<div class="empty">No jobs have been created.</div>{{end}}
    </section>
  </main>
  <script>for(const element of document.querySelectorAll('[data-time]')){const date=new Date(element.dataset.time);if(!Number.isNaN(date.valueOf()))element.textContent=new Intl.DateTimeFormat(undefined,{dateStyle:'medium',timeStyle:'short'}).format(date)}</script>
</body>
</html>`

const logPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{.JobName}} · {{.WorkflowName}} · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1440px,100%);margin:0 auto;padding:24px 32px 56px}.breadcrumbs{display:flex;align-items:center;gap:8px;margin-bottom:22px;color:#8b949e}.breadcrumbs strong{color:#f0f6fc}.layout{display:grid;grid-template-columns:260px minmax(0,1fr);gap:24px}.sidebar{min-width:0}.sidebar-title{margin:0 0 8px;padding:0 8px;color:#8b949e;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.05em}.summary-link,.side-job{display:flex;align-items:center;gap:9px;min-height:40px;padding:8px;border-radius:6px;color:#c9d1d9}.summary-link:hover,.side-job:hover{background:#161b22;text-decoration:none}.side-job.selected{background:#1f6feb26;color:#f0f6fc;font-weight:600}.side-dot{display:grid;place-items:center;width:17px;height:17px;flex:0 0 auto;border:2px solid currentColor;border-radius:50%;font-size:10px}.side-dot.succeeded{color:#3fb950}.side-dot.failed{color:#f85149}.side-dot.running{color:#d29922;border-style:dashed}.side-dot.queued{color:#8b949e}.side-dot.succeeded::after{content:"✓"}.side-dot.failed::after{content:"×"}.side-dot.queued::after{content:"·"}.side-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.job-header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:16px}.job-header h1{margin:0 0 6px;font-size:20px;line-height:1.3}.job-meta{display:flex;flex-wrap:wrap;gap:6px 14px;color:#8b949e;font-size:13px}.job-meta code{color:#c9d1d9;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.state-pill{display:flex;align-items:center;gap:7px;white-space:nowrap;color:#8b949e;font-size:13px}.pulse{width:8px;height:8px;border-radius:50%;background:#8b949e}.state-pill.live .pulse{background:#3fb950;box-shadow:0 0 0 4px #23863633}.toolbar{display:flex;align-items:center;justify-content:space-between;gap:14px;min-height:44px;padding:7px 12px;border:1px solid #30363d;border-bottom:0;border-radius:6px 6px 0 0;background:#161b22}.toolbar-title{font-size:13px;font-weight:600}.controls{display:flex;align-items:center;gap:16px;color:#8b949e;font-size:12px}.control{display:flex;align-items:center;gap:6px;cursor:pointer}.control input{accent-color:#2f81f7}.badge{display:none;min-width:18px;padding:0 5px;border-radius:10px;background:#30363d;color:#c9d1d9;text-align:center}.badge.visible{display:inline-block}.log-shell{min-height:340px;overflow:hidden;border:1px solid #30363d;border-radius:0 0 6px 6px;background:#0d1117}.log{padding:8px 0 16px;counter-reset:line;color:#c9d1d9;font:12px/1.65 ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace}.placeholder{padding:28px 20px;color:#8b949e;text-align:center;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.log-line{display:grid;grid-template-columns:auto auto minmax(0,1fr);min-height:20px;padding:0 16px;white-space:pre-wrap;overflow-wrap:anywhere}.log-line::before{counter-increment:line;content:counter(line);width:42px;padding-right:14px;color:#484f58;text-align:right;user-select:none}.line-time{display:none;width:178px;padding-right:12px;color:#6e7681;white-space:nowrap}.show-time .line-time{display:block}.line-icon{display:inline-block;width:16px;margin-right:2px;color:#8b949e;text-align:center}.line-text{min-width:0}.log-line.runner{color:#8b949e}.log-line.command{color:#79c0ff}.log-line.input,.log-line.step-output{color:#8b949e;background:#161b224d}.metadata-key{color:#79c0ff}.log-line.debug{display:none;color:#8b949e}.show-debug .log-line.debug{display:grid}.log-line.notice{color:#58a6ff;background:#388bfd1a}.log-line.warning{color:#d29922;background:#bb800926}.log-line.error{color:#ff7b72;background:#f8514926}.annotation{display:block;color:#8b949e;font-size:11px}.log-group{border-top:1px solid #21262d;border-bottom:1px solid #21262d;background:#0d1117}.log-group+.log-group{border-top:0}.log-group>summary{display:flex;align-items:center;min-height:42px;padding:8px 16px;cursor:pointer;background:#161b22;color:#f0f6fc;font:600 13px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;list-style:none}.log-group>summary::-webkit-details-marker{display:none}.log-group>summary::before{content:"›";width:18px;color:#8b949e;font-size:18px;transform:rotate(0);transition:transform .1s}.log-group[open]>summary::before{transform:rotate(90deg)}.group-time{display:none;margin-left:auto;color:#6e7681;font:11px ui-monospace,SFMono-Regular,Consolas,monospace}.show-time .group-time{display:block}.log-group .log-group{margin:4px 16px;border:1px solid #30363d;border-radius:5px}@media(max-width:800px){.topbar{padding:0 16px}.page{padding:20px 12px 40px}.layout{grid-template-columns:1fr}.sidebar{display:none}.job-header{padding:0 4px}.job-meta .optional{display:none}.toolbar{align-items:flex-start}.controls{gap:10px;flex-wrap:wrap;justify-content:flex-end}.log-line{padding:0 8px}.log-line::before{width:34px;padding-right:9px}.show-time .line-time{display:none}.show-time .group-time{display:none}}
  </style>
</head>
<body data-stream-url="{{.StreamURL}}">
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a></nav>
  <main class="page">
    <div class="breadcrumbs"><strong>{{.Repository}}</strong><span>/</span><span>Actions</span><span>/</span><a href="{{.RunURL}}">{{.WorkflowName}}</a></div>
    <div class="layout">
      <aside class="sidebar" aria-label="Workflow jobs">
        <a class="summary-link" href="{{.RunURL}}">← Run summary</a>
        <h2 class="sidebar-title">Jobs</h2>
        {{range .Jobs}}<a class="side-job{{if .Selected}} selected{{end}}" href="{{.URL}}"><span class="side-dot {{.StatusClass}}" aria-hidden="true"></span><span class="side-name">{{.DisplayName}}</span></a>{{end}}
      </aside>
      <section>
        <header class="job-header">
          <div><h1>{{.JobName}}</h1><div class="job-meta"><span>{{.Status}}</span><span>{{.Runner}}</span>{{if .Duration}}<span>{{.Duration}}</span>{{end}}<code class="optional">{{.ShortRevision}}</code></div></div>
          <div class="state-pill live" id="state-pill"><span class="pulse" aria-hidden="true"></span><span id="state">Connecting…</span></div>
        </header>
        <div class="toolbar">
          <span class="toolbar-title">Runner logs</span>
          <div class="controls">
            <label class="control"><input id="debug-toggle" type="checkbox"> Show debug <span class="badge" id="debug-count">0</span></label>
            <label class="control"><input id="time-toggle" type="checkbox"> Show timestamps</label>
          </div>
        </div>
        <div class="log-shell"><div class="placeholder" id="placeholder">Waiting for log output…</div><div class="log" id="logs" role="log" aria-live="polite"></div></div>
      </section>
    </div>
  </main>
  <script>
    const output=document.getElementById('logs');const placeholder=document.getElementById('placeholder');const state=document.getElementById('state');const statePill=document.getElementById('state-pill');const debugToggle=document.getElementById('debug-toggle');const timeToggle=document.getElementById('time-toggle');const debugCount=document.getElementById('debug-count');
    const containers=[{body:output,scope:'root'}];let debugLines=0;let received=false;
    const current=()=>containers[containers.length-1].body;
    const timestamp=value=>{if(!value)return '';const date=new Date(value);return Number.isNaN(date.valueOf())?value:date.toLocaleTimeString(undefined,{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit',fractionalSecondDigits:3})};
    const iconFor=kind=>({debug:'◆',notice:'●',warning:'▲',error:'✕',runner:'›',command:'$',input:'↳','step-output':'↳'})[kind]||'';
    function addLine(entry){
      if(entry.kind==='group'){
        if(entry.scope==='workflow'||entry.scope==='post')while(containers.length>1)containers.pop();
        const details=document.createElement('details');details.className='log-group';details.open=true;
        const summary=document.createElement('summary');const label=document.createElement('span');label.textContent=entry.text||'Log group';summary.append(label);
        if(entry.time){const time=document.createElement('span');time.className='group-time';time.textContent=timestamp(entry.time);summary.append(time)}
        const body=document.createElement('div');details.append(summary,body);current().append(details);containers.push({body,details,scope:entry.scope||'command'});return;
      }
      if(entry.kind==='endgroup'){for(let index=containers.length-1;index>0;index--){if(containers[index].scope===entry.scope){const container=containers[index];containers.length=index;if(entry.scope==='workflow')container.details.open=false;break}}return}
      const line=document.createElement('div');line.className='log-line '+entry.kind;
      const time=document.createElement('span');time.className='line-time';time.textContent=timestamp(entry.time);
      const icon=document.createElement('span');icon.className='line-icon';icon.textContent=iconFor(entry.kind);
      const text=document.createElement('span');text.className='line-text';
      if(entry.kind==='input'||entry.kind==='step-output'){const key=document.createElement('span');key.className='metadata-key';key.textContent=(entry.kind==='input'?'with ':'output ')+(entry.text||'');text.append(key)}else{text.textContent=entry.text||''}
      if(entry.properties){const detail=[];if(entry.properties.title)detail.push(entry.properties.title);let location=entry.properties.file||'';if(entry.properties.line)location+=(location?':':'line ')+entry.properties.line;if(entry.properties.col)location+=':'+entry.properties.col;if(entry.properties.endline){location+='-'+entry.properties.endline;if(entry.properties.endcolumn)location+=':'+entry.properties.endcolumn}else if(entry.properties.endcolumn){location+='-'+entry.properties.endcolumn}if(location)detail.push(location);if(detail.length){const annotation=document.createElement('small');annotation.className='annotation';annotation.textContent=detail.join(' · ');text.append(document.createElement('br'),annotation)}}
      line.append(time,icon,text);current().append(line);
      if(entry.kind==='debug'){debugLines++;debugCount.textContent=String(debugLines);debugCount.classList.add('visible')}
    }
    function receive(entry){const pinned=window.innerHeight+window.scrollY>=document.documentElement.scrollHeight-80;if(!received){placeholder.hidden=true;received=true}addLine(entry);if(pinned)requestAnimationFrame(()=>window.scrollTo(0,document.documentElement.scrollHeight))}
    debugToggle.addEventListener('change',()=>output.classList.toggle('show-debug',debugToggle.checked));timeToggle.addEventListener('change',()=>output.classList.toggle('show-time',timeToggle.checked));
    const source=new EventSource(document.body.dataset.streamUrl);
    source.addEventListener('status',event=>{state.textContent=JSON.parse(event.data)});
    source.addEventListener('log',event=>receive(JSON.parse(event.data)));
    source.addEventListener('end',event=>{state.textContent=JSON.parse(event.data);statePill.classList.remove('live');source.close()});
    source.addEventListener('error',event=>{if(event.data){state.textContent=JSON.parse(event.data);statePill.classList.remove('live');source.close()}else{state.textContent='Connection lost; retrying…'}});
  </script>
</body>
</html>`
