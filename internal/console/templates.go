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
    <h1>Administrator sign in</h1>
    <form class="card" id="login">
      <p class="hint">Use the administrator token configured for this Console to run workflows and manage Project Secrets.</p>
      <label for="token">Administrator token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" required autofocus>
      <button type="submit">Sign in</button>
      <p id="error" role="alert"></p>
    </form>
    <p class="footer">Workflow and Project administration</p>
  </main>
  <script>
    const form=document.getElementById('login');const error=document.getElementById('error');const button=form.querySelector('button');
    form.addEventListener('submit',async event=>{event.preventDefault();error.textContent='';button.disabled=true;
      try{const response=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:form.token.value})});
        if(!response.ok){error.textContent='The token is invalid.';return}
        const next=new URLSearchParams(location.search).get('next');location.assign(next&&(next==='/dispatch'||next.startsWith('/dispatch?')||next==='/projects'||next.startsWith('/projects/')||next.startsWith('/runs/'))?next:'/projects');
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
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;gap:24px;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1180px,100%);margin:0 auto;padding:32px 32px 56px}.page-heading{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;margin-bottom:20px}.page-heading h1{margin:0 0 6px;font-size:24px;font-weight:600;letter-spacing:-.01em}.page-heading p{margin:0;color:#8b949e}.count{min-width:20px;padding:2px 8px;border-radius:20px;background:#30363d;color:#c9d1d9;font-size:12px;text-align:center}.runs{overflow:hidden;border:1px solid #30363d;border-radius:6px}.run{display:grid;grid-template-columns:minmax(280px,1fr) minmax(150px,.55fr) 130px 170px;align-items:center;gap:20px;min-height:76px;padding:14px 16px;border-bottom:1px solid #21262d}.run:last-child{border-bottom:0}.run:hover{background:#161b2266}.run-link{display:flex;align-items:center;gap:12px;min-width:0;color:#f0f6fc}.run-link:hover{color:#58a6ff;text-decoration:none}.status-mark{display:grid;place-items:center;flex:0 0 auto;width:20px;height:20px;border:2px solid currentColor;border-radius:50%;font-size:11px;font-weight:800}.status-mark.succeeded{color:#3fb950}.status-mark.failed{color:#f85149}.status-mark.running,.status-mark.cancelling{color:#d29922;border-style:dashed}.status-mark.queued{color:#8b949e}.status-mark.succeeded::after{content:"✓"}.status-mark.failed::after{content:"×"}.status-mark.queued::after{content:"·"}.run-name{min-width:0}.run-name strong,.run-name small{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.run-name small{margin-top:4px;color:#8b949e;font-weight:400}.revision{min-width:0;color:#c9d1d9}.revision span{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.revision code{display:inline-block;margin-top:4px;color:#8b949e;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.detail{color:#8b949e;font-size:13px}.status{color:#c9d1d9}.status small{display:block;margin-top:4px;color:#8b949e}.time{display:block;margin-top:4px}.empty{padding:52px 24px;text-align:center}.empty strong{display:block;margin-bottom:6px;font-size:16px}.empty span{color:#8b949e}@media(max-width:800px){.topbar{padding:0 16px}.page{padding:24px 16px 40px}.page-heading{align-items:flex-start}.run{grid-template-columns:minmax(0,1fr) auto}.run .revision,.run .event{display:none}.run .status{text-align:right}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a><a href="/projects">Projects</a><a href="/dispatch">Run workflow</a></nav>
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

const projectsPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Projects · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;gap:24px;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1000px,100%);margin:0 auto;padding:32px 32px 56px}.page-heading{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;margin-bottom:20px}.page-heading h1{margin:0 0 6px;font-size:24px}.page-heading p{margin:0;color:#8b949e}.count{padding:2px 8px;border-radius:20px;background:#30363d;color:#c9d1d9;font-size:12px}.projects{overflow:hidden;border:1px solid #30363d;border-radius:6px}.project{display:grid;grid-template-columns:minmax(240px,1fr) 170px 160px;align-items:center;gap:20px;min-height:72px;padding:14px 16px;border-bottom:1px solid #21262d}.project:last-child{border-bottom:0}.project:hover{background:#161b2266}.project-name{color:#f0f6fc}.project-name strong,.project-name small{display:block}.project-name small,.detail{margin-top:4px;color:#8b949e}.status{display:flex;align-items:center;gap:8px}.status-mark{width:12px;height:12px;border:2px solid currentColor;border-radius:50%}.status-mark.succeeded{color:#3fb950}.status-mark.failed{color:#f85149}.status-mark.queued{color:#8b949e}.empty{padding:48px;text-align:center;color:#8b949e}@media(max-width:800px){.topbar{padding:0 16px}}@media(max-width:680px){.page{padding:24px 16px}.project{grid-template-columns:1fr auto}.project .secret{display:none}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a><a href="/projects">Projects</a><a href="/dispatch">Run workflow</a></nav>
  <main class="page">
    <header class="page-heading"><div><h1>Projects</h1><p>Workflow sources and Project-scoped configuration</p></div><span class="count">{{len .Projects}}</span></header>
    <section class="projects" aria-label="Projects">
      {{range .Projects}}<article class="project">
        <a class="project-name" href="{{.URL}}"><strong>{{.Name}}</strong><small>{{.Namespace}}</small></a>
        <div class="status"><span class="status-mark {{.StatusClass}}" aria-hidden="true"></span><span>{{.Status}}</span></div>
        <div class="detail secret">{{if .SecretName}}{{.SecretName}}{{else}}No Secret{{end}}</div>
      </article>{{else}}<div class="empty">No Projects are configured.</div>{{end}}
    </section>
  </main>
</body>
</html>`

const projectPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{.Name}} · Projects · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;gap:24px;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(900px,100%);margin:0 auto;padding:28px 32px 56px}.breadcrumbs{display:flex;gap:8px;margin-bottom:22px;color:#8b949e}.heading{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;margin-bottom:24px}.heading h1{margin:0 0 6px;font-size:24px}.muted{color:#8b949e}.pill{display:flex;align-items:center;gap:7px}.status-mark{width:12px;height:12px;border:2px solid currentColor;border-radius:50%}.status-mark.succeeded{color:#3fb950}.status-mark.failed{color:#f85149}.status-mark.queued{color:#8b949e}.summary,.panel{padding:20px;border:1px solid #30363d;border-radius:6px}.summary{display:grid;grid-template-columns:170px 1fr;gap:12px 20px;margin-bottom:28px}.summary dt{color:#8b949e}.summary dd{margin:0;overflow-wrap:anywhere}.panel h2{margin:0 0 6px;font-size:18px}.hint{margin:0 0 18px;color:#8b949e;line-height:1.5}.notice{padding:12px;border:1px solid #30363d;border-radius:6px;background:#161b22;color:#c9d1d9}.secret-list{margin:0 0 22px;padding:0;list-style:none;border:1px solid #30363d;border-radius:6px}.secret-row{display:flex;align-items:center;justify-content:space-between;gap:16px;min-height:48px;padding:8px 12px;border-bottom:1px solid #21262d}.secret-row:last-child{border-bottom:0}.secret-row code{font:13px ui-monospace,SFMono-Regular,Consolas,monospace}.secret-row form{margin:0}.form{display:grid;grid-template-columns:minmax(180px,.7fr) minmax(220px,1fr) auto;align-items:end;gap:12px}.field label{display:block;margin-bottom:7px;font-size:13px;font-weight:600}.field input{width:100%;height:34px;padding:5px 10px;border:1px solid #30363d;border-radius:6px;background:#0d1117;color:#f0f6fc;font:inherit}.field input:focus{border-color:#2f81f7;outline:none;box-shadow:0 0 0 3px #2f81f74d}button{height:34px;padding:0 14px;border:1px solid #2ea043;border-radius:6px;background:#238636;color:#fff;font-weight:600;cursor:pointer}.delete{border-color:#da3633;background:#b62324}.empty{padding:18px;color:#8b949e;text-align:center}@media(max-width:800px){.topbar{padding:0 16px}}@media(max-width:680px){.page{padding:22px 16px}.heading{display:block}.pill{margin-top:12px}.summary{grid-template-columns:1fr}.summary dd{margin-bottom:8px}.form{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a><a href="/projects">Projects</a><a href="/dispatch">Run workflow</a></nav>
  <main class="page">
    <div class="breadcrumbs"><a href="/projects">Projects</a><span>/</span><span>{{.Namespace}}</span><span>/</span><span>{{.Name}}</span></div>
    <header class="heading"><div><h1>{{.Name}}</h1><span class="muted">{{.Namespace}}</span></div><div class="pill"><span class="status-mark {{.StatusClass}}" aria-hidden="true"></span><span>{{.Status}}</span></div></header>
    <dl class="summary">
      <dt>Installation</dt><dd>{{.Installation}}</dd>
      <dt>Workflow directory</dt><dd><code>{{.WorkflowDirectory}}</code></dd>
      <dt>Secret</dt><dd>{{if .SecretName}}<code>{{.SecretName}}</code>{{else}}Not configured{{end}}</dd>
      <dt>Configuration</dt><dd>{{.StatusMessage}}</dd>
    </dl>
    <section class="panel" aria-label="Workflow secrets">
      <h2>Workflow secrets</h2>
      <p class="hint">Secret values are write-only. Existing values are never returned to the browser.</p>
      {{if .CanReadSecrets}}
        {{if .SecretMissing}}<p class="notice">The referenced Secret does not exist. Adding the first value will create it.</p>{{end}}
        <ul class="secret-list">
          {{range .SecretNames}}<li class="secret-row"><code>{{.}}</code>{{if $.CanManageSecrets}}<form method="post" action="{{$.SecretsURL}}" onsubmit="return confirm('Delete this workflow secret?')"><input type="hidden" name="csrf" value="{{$.CSRFToken}}"><input type="hidden" name="action" value="delete"><input type="hidden" name="name" value="{{.}}"><button class="delete" type="submit">Delete</button></form>{{end}}</li>{{else}}<li class="empty">No workflow secrets are configured.</li>{{end}}
        </ul>
        {{if .CanManageSecrets}}
          <form class="form" method="post" action="{{.SecretsURL}}">
            <input type="hidden" name="csrf" value="{{.CSRFToken}}"><input type="hidden" name="action" value="set">
            <div class="field"><label for="secret-name">Name</label><input id="secret-name" name="name" pattern="[A-Za-z_][A-Za-z0-9_]*" maxlength="255" autocomplete="off" required></div>
            <div class="field"><label for="secret-value">New value</label><input id="secret-value" name="value" type="password" autocomplete="new-password" required></div>
            <button type="submit">Add or replace</button>
          </form>
        {{else}}<p class="notice"><a href="{{.LoginURL}}">Sign in as an administrator</a> to add, replace, or delete workflow secrets.</p>{{end}}
      {{else}}<p class="notice">{{.ManagementNotice}}</p>{{end}}
    </section>
  </main>
</body>
</html>`

const dispatchPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Run workflow · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;gap:24px;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(900px,100%);margin:0 auto;padding:32px 32px 56px}.heading{margin-bottom:22px}.heading h1{margin:0 0 7px;font-size:24px}.muted,.hint{color:#8b949e;line-height:1.5}.panel{padding:22px;border:1px solid #30363d;border-radius:6px}.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.field.full{grid-column:1/-1}.field label{display:block;margin-bottom:7px;font-size:13px;font-weight:600}.field input,.field select,.field textarea{width:100%;padding:7px 10px;border:1px solid #30363d;border-radius:6px;background:#0d1117;color:#f0f6fc;font:inherit}.field input,.field select{height:36px}.field textarea{min-height:72px;resize:vertical}.field input:focus,.field select:focus,.field textarea:focus{border-color:#2f81f7;outline:none;box-shadow:0 0 0 3px #2f81f74d}.ref{display:grid;grid-template-columns:120px 1fr;gap:10px}.inputs-heading{display:flex;align-items:center;justify-content:space-between;gap:16px;margin:28px 0 10px}.inputs-heading h2{margin:0;font-size:16px}.input-row{display:grid;grid-template-columns:minmax(180px,.55fr) minmax(240px,1fr) auto;align-items:end;gap:10px;padding:12px 0;border-top:1px solid #21262d}.input-row.declared{grid-template-columns:minmax(220px,.75fr) minmax(260px,1fr);align-items:start}.input-name{display:flex;align-items:center;gap:7px;margin-bottom:7px;font-size:13px;font-weight:600}.input-name code{font:600 13px ui-monospace,SFMono-Regular,Consolas,monospace}.input-type{padding:1px 6px;border:1px solid #30363d;border-radius:20px;color:#8b949e;font-size:11px;font-weight:400}.input-description{margin:0 0 9px;color:#8b949e;font-size:12px;line-height:1.5}.include-input{display:flex;align-items:center;gap:7px;color:#c9d1d9;font-size:12px}.include-input input{width:auto;height:auto;margin:0}.button{height:36px;padding:0 14px;border:1px solid #2ea043;border-radius:6px;background:#238636;color:#fff;font-weight:600;cursor:pointer}.button:hover{background:#2ea043}.button.secondary{border-color:#30363d;background:#21262d;color:#c9d1d9}.button.secondary:hover{background:#30363d}.submit{margin-top:24px}.notice{padding:16px;border:1px solid #30363d;border-radius:6px;background:#161b22;color:#c9d1d9}@media(max-width:800px){.topbar{padding:0 16px}}@media(max-width:680px){.page{padding:24px 16px}.form-grid{grid-template-columns:1fr}.field.full{grid-column:auto}.input-row,.input-row.declared{grid-template-columns:1fr}.ref{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a><a href="/projects">Projects</a><a href="/dispatch">Run workflow</a></nav>
  <main class="page">
    <header class="heading"><h1>Run workflow</h1><div class="muted">Create a manual workflow run at a pinned branch or tag revision.</div></header>
    {{if .Projects}}<form class="panel" method="post" action="/dispatch" autocomplete="off">
      <input type="hidden" name="csrf" value="{{.CSRFToken}}"><input type="hidden" name="request-id" value="{{.RequestID}}">
      <div class="form-grid">
        <div class="field full"><label for="project">Project</label><select id="project" name="project" required>{{range .Projects}}<option value="{{.Value}}"{{if .Selected}} selected{{end}}>{{.Label}}</option>{{end}}</select></div>
        <div class="field"><label for="repository-owner">Repository owner</label><input id="repository-owner" name="repository-owner" value="{{.RepositoryOwner}}" pattern="[A-Za-z0-9][A-Za-z0-9_-]*" maxlength="100" required></div>
        <div class="field"><label for="repository-name">Repository name</label><input id="repository-name" name="repository-name" value="{{.RepositoryName}}" pattern="[A-Za-z0-9._-]+" maxlength="100" required></div>
        <div class="field full"><label for="revision">Commit SHA</label><input id="revision" name="revision" value="{{.Revision}}" pattern="[0-9a-f]{40}" maxlength="40" spellcheck="false" required></div>
        <div class="field full"><label for="ref-name">Branch or tag</label><div class="ref"><select name="ref-type" aria-label="Git ref type"><option value="branch"{{if eq .RefType "branch"}} selected{{end}}>Branch</option><option value="tag"{{if eq .RefType "tag"}} selected{{end}}>Tag</option></select><input id="ref-name" name="ref-name" value="{{.RefName}}" maxlength="1013" placeholder="main" spellcheck="false" required></div></div>
        <div class="field full"><label for="workflow-path">Workflow path</label><input id="workflow-path" name="workflow-path" value="{{.WorkflowPath}}" maxlength="512" placeholder=".open-actions/workflows/deploy.yaml" spellcheck="false" required></div>
      </div>
      <div class="inputs-heading"><div><h2>Inputs</h2><span class="hint">{{if .InputsFromWorkflow}}Inputs declared by this workflow file.{{else}}Supply only inputs declared by the workflow; omitted defaults are resolved by the controller.{{end}}</span></div>{{if not .InputsFromWorkflow}}<button class="button secondary" id="add-input" type="button">Add input</button>{{end}}</div>
      <div id="inputs">{{if .InputsFromWorkflow}}{{range $index, $input := .Inputs}}<div class="input-row declared">
        <div><div class="input-name"><code>{{$input.Name}}</code><span class="input-type">{{$input.Type}}</span>{{if $input.Required}}<span aria-label="required">Required</span>{{end}}</div>{{if $input.Description}}<p class="input-description">{{$input.Description}}</p>{{end}}{{if not $input.Required}}<label class="include-input"><input type="checkbox" data-input-toggle{{if $input.Included}} checked{{end}}>Include this input</label>{{end}}<input type="hidden" name="input-name" value="{{$input.Name}}" data-input-field{{if not $input.Included}} disabled{{end}}></div>
        <div class="field"><label for="workflow-input-{{$index}}">Value</label>{{if eq $input.Type "choice"}}<select id="workflow-input-{{$index}}" name="input-value" data-input-field{{if not $input.Included}} disabled{{end}}>{{range $input.Options}}<option value="{{.}}"{{if eq . $input.Value}} selected{{end}}>{{.}}</option>{{end}}</select>{{else if eq $input.Type "boolean"}}<select id="workflow-input-{{$index}}" name="input-value" data-input-field{{if not $input.Included}} disabled{{end}}><option value="false"{{if eq $input.Value "false"}} selected{{end}}>false</option><option value="true"{{if eq $input.Value "true"}} selected{{end}}>true</option></select>{{else}}<textarea id="workflow-input-{{$index}}" name="input-value" maxlength="65535" data-input-field{{if not $input.Included}} disabled{{end}}{{if $input.Required}} required{{end}}>{{$input.Value}}</textarea>{{end}}</div>
      </div>{{else}}<div class="hint">This workflow does not declare any inputs.</div>{{end}}{{else}}{{range .Inputs}}<div class="input-row"><div class="field"><label>Input name</label><input name="input-name" value="{{.Name}}" pattern="[A-Za-z_][A-Za-z0-9_-]{0,99}" maxlength="100" required></div><div class="field"><label>Value</label><textarea name="input-value" maxlength="65535">{{.Value}}</textarea></div><button class="button secondary remove-input" type="button">Remove</button></div>{{end}}{{end}}</div>
      <button class="button submit" type="submit">Run workflow</button>
    </form>{{else}}<p class="notice">No GitHub Projects are configured.</p>{{end}}
  </main>
  <template id="input-template"><div class="input-row"><div class="field"><label>Input name</label><input name="input-name" pattern="[A-Za-z_][A-Za-z0-9_-]{0,99}" maxlength="100" required></div><div class="field"><label>Value</label><textarea name="input-value" maxlength="65535"></textarea></div><button class="button secondary remove-input" type="button">Remove</button></div></template>
  <script>
    const inputs=document.getElementById('inputs');const add=document.getElementById('add-input');const template=document.getElementById('input-template');
    if(add)add.addEventListener('click',()=>{const row=template.content.cloneNode(true);inputs.append(row);inputs.lastElementChild.querySelector('input').focus()});
    if(inputs)inputs.addEventListener('click',event=>{if(event.target.classList.contains('remove-input'))event.target.closest('.input-row').remove()});
    for(const toggle of document.querySelectorAll('[data-input-toggle]'))toggle.addEventListener('change',()=>{for(const field of toggle.closest('.input-row').querySelectorAll('[data-input-field]'))field.disabled=!toggle.checked});
  </script>
</body>
</html>`

const runPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{.WorkflowName}} · Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;gap:24px;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1180px,100%);margin:0 auto;padding:28px 32px 56px}.breadcrumbs{display:flex;align-items:center;gap:8px;margin-bottom:22px;color:#8b949e;font-size:14px}.breadcrumbs strong{color:#f0f6fc}.run-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:22px}.run-heading h1{margin:0 0 8px;font-size:24px;font-weight:600;line-height:1.25;letter-spacing:-.01em}.run-actions{display:flex;gap:8px}.run-actions form{margin:0}.run-action{display:inline-flex;align-items:center;height:34px;padding:0 14px;border:1px solid #30363d;border-radius:6px;background:#21262d;color:#f0f6fc;font:600 14px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;white-space:nowrap;cursor:pointer}.run-action:hover{background:#30363d;text-decoration:none}.run-action.danger{border-color:#f85149;background:#b62324}.run-action.danger:hover{background:#da3633}.muted{color:#8b949e}.workflow-path{overflow-wrap:anywhere}.summary{display:grid;grid-template-columns:auto 1fr;gap:14px;padding:20px;border:1px solid #30363d;border-radius:6px;background:#0d1117}.status-mark{display:grid;place-items:center;flex:0 0 auto;width:24px;height:24px;border:2px solid currentColor;border-radius:50%;font-size:14px;font-weight:800}.status-mark.succeeded{color:#3fb950}.status-mark.failed{color:#f85149}.status-mark.running,.status-mark.cancelling{color:#d29922;border-style:dashed}.status-mark.queued{color:#8b949e}.status-mark.succeeded::after{content:"✓"}.status-mark.failed::after{content:"×"}.status-mark.running::after,.status-mark.cancelling::after{content:""}.status-mark.queued::after{content:"·"}.summary-title{margin:1px 0 5px;font-size:16px;font-weight:600}.summary-meta{display:flex;flex-wrap:wrap;gap:6px 14px;color:#8b949e;font-size:13px}.summary-meta code{padding:2px 5px;border:1px solid #30363d;border-radius:5px;background:#161b22;color:#c9d1d9;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.section-title{display:flex;align-items:center;gap:8px;margin:30px 0 12px;font-size:16px;font-weight:600}.count{min-width:20px;padding:1px 6px;border-radius:20px;background:#30363d;color:#c9d1d9;font-size:12px;text-align:center}.workflow-file{overflow:hidden;border:1px solid #30363d;border-radius:6px}.workflow-file-header{padding:10px 16px;border-bottom:1px solid #30363d;background:#161b22}.workflow-file-header code{color:#c9d1d9;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.workflow-source{max-height:560px;margin:0;padding:16px;overflow:auto;background:#0d1117;color:#c9d1d9;font:12px/1.55 ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace;white-space:pre;tab-size:2}.jobs{overflow:hidden;border:1px solid #30363d;border-radius:6px}.job{display:grid;grid-template-columns:minmax(220px,1fr) 140px 180px 150px;align-items:center;min-height:68px;padding:12px 16px;border-bottom:1px solid #21262d}.job:last-child{border-bottom:0}.job:hover{background:#161b2266}.job-link{display:flex;align-items:center;gap:12px;min-width:0;color:#f0f6fc;font-weight:600}.job-link:hover{color:#58a6ff;text-decoration:none}.job-status{width:18px;height:18px;font-size:11px}.job-name{min-width:0}.job-name span{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.job-name small{display:block;margin-top:3px;color:#8b949e;font-weight:400}.cell{color:#8b949e;font-size:13px}.status-text{color:#c9d1d9}.empty{padding:32px;text-align:center;color:#8b949e}@media(max-width:800px){.topbar{padding:0 16px}}@media(max-width:760px){.page{padding:22px 16px 40px}.run-heading{display:block}.run-actions{margin-top:16px;flex-wrap:wrap}.summary{padding:16px}.workflow-source{padding:12px}.job{grid-template-columns:1fr auto;gap:8px}.job .runner,.job .started{display:none}.job .status-text{text-align:right}}
  </style>
</head>
<body>
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a><a href="/projects">Projects</a><a href="/dispatch">Run workflow</a></nav>
  <main class="page">
    <div class="breadcrumbs"><strong>{{.Repository}}</strong><span>/</span><span>Actions</span><span>/</span><span>{{.WorkflowName}}</span></div>
    <div class="run-heading">
      <div><h1>{{.WorkflowName}}</h1><div class="muted workflow-path">{{.WorkflowPath}}</div></div>
      <div class="run-actions">{{if .DispatchURL}}<a class="run-action" href="{{.DispatchURL}}">Run workflow</a>{{end}}{{if .ApproveURL}}{{if .CanApprove}}<form method="post" action="{{.ApproveURL}}" onsubmit="return confirm('Approve this exact pull request revision to run?')"><input type="hidden" name="csrf" value="{{.CSRFToken}}"><button class="run-action" type="submit">Approve workflow</button></form>{{else}}<a class="run-action" href="{{.LoginURL}}">Sign in to approve</a>{{end}}{{end}}{{if .CancelURL}}{{if .CanCancel}}<form method="post" action="{{.CancelURL}}" onsubmit="return confirm('Cancel this workflow run?')"><input type="hidden" name="csrf" value="{{.CSRFToken}}"><button class="run-action danger" type="submit">Cancel workflow</button></form>{{else}}<a class="run-action" href="{{.LoginURL}}">Sign in to cancel</a>{{end}}{{else if .RerunURL}}{{if .CanRerun}}{{if .CanRerunFailed}}<form method="post" action="{{.RerunURL}}"><input type="hidden" name="csrf" value="{{.CSRFToken}}"><input type="hidden" name="jobs" value="failed"><button class="run-action" type="submit">Re-run failed jobs</button></form>{{end}}<form method="post" action="{{.RerunURL}}"><input type="hidden" name="csrf" value="{{.CSRFToken}}"><input type="hidden" name="jobs" value="all"><button class="run-action" type="submit">Re-run all jobs</button></form>{{else}}<a class="run-action" href="{{.LoginURL}}">Sign in to re-run</a>{{end}}{{end}}</div>
    </div>
    <section class="summary" aria-label="Workflow run summary">
      <span class="status-mark {{.StatusClass}}" aria-hidden="true"></span>
      <div>
        <div class="summary-title">Workflow run {{.Status}}</div>
        <div class="summary-meta"><span>{{.RefName}}</span><code title="{{.Revision}}">{{.ShortRevision}}</code>{{if .Started}}<time data-time="{{.Started}}">{{.Started}}</time>{{end}}{{if .Duration}}<span>{{.Duration}}</span>{{end}}</div>
      </div>
    </section>
    <h2 class="section-title">Workflow file</h2>
    <section class="workflow-file" aria-label="Workflow file">
      {{if .HasWorkflowFile}}<div class="workflow-file-header"><code>{{.WorkflowPath}}</code></div><pre class="workflow-source"><code>{{.WorkflowFile}}</code></pre>{{else}}<div class="empty">The workflow file is not available for this run.</div>{{end}}
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
    :root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#f0f6fc}*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#f0f6fc;font-size:14px}a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}.topbar{height:64px;display:flex;align-items:center;gap:24px;padding:0 24px;border-bottom:1px solid #21262d;background:#010409}.brand{display:flex;align-items:center;gap:10px;color:#f0f6fc;font-weight:600}.brand:hover{text-decoration:none}.brand-mark{display:grid;place-items:center;width:30px;height:30px;border:1px solid #30363d;border-radius:7px;color:#58a6ff;font-size:12px;font-weight:800}.page{width:min(1440px,100%);margin:0 auto;padding:24px 32px 56px}.breadcrumbs{display:flex;align-items:center;gap:8px;margin-bottom:22px;color:#8b949e}.breadcrumbs strong{color:#f0f6fc}.layout{display:grid;grid-template-columns:260px minmax(0,1fr);gap:24px}.sidebar{min-width:0}.sidebar-title{margin:0 0 8px;padding:0 8px;color:#8b949e;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.05em}.summary-link,.side-job{display:flex;align-items:center;gap:9px;min-height:40px;padding:8px;border-radius:6px;color:#c9d1d9}.summary-link:hover,.side-job:hover{background:#161b22;text-decoration:none}.side-job.selected{background:#1f6feb26;color:#f0f6fc;font-weight:600}.side-dot{display:grid;place-items:center;width:17px;height:17px;flex:0 0 auto;border:2px solid currentColor;border-radius:50%;font-size:10px}.side-dot.succeeded{color:#3fb950}.side-dot.failed{color:#f85149}.side-dot.running{color:#d29922;border-style:dashed}.side-dot.queued{color:#8b949e}.side-dot.succeeded::after{content:"✓"}.side-dot.failed::after{content:"×"}.side-dot.queued::after{content:"·"}.side-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.job-header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:16px}.job-header h1{margin:0 0 6px;font-size:20px;line-height:1.3}.job-meta{display:flex;flex-wrap:wrap;gap:6px 14px;color:#8b949e;font-size:13px}.job-meta code{color:#c9d1d9;font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.state-pill{display:flex;align-items:center;gap:7px;white-space:nowrap;color:#8b949e;font-size:13px}.pulse{width:8px;height:8px;border-radius:50%;background:#8b949e}.state-pill.live .pulse{background:#3fb950;box-shadow:0 0 0 4px #23863633}.toolbar{display:flex;align-items:center;justify-content:space-between;gap:14px;min-height:44px;padding:7px 12px;border:1px solid #30363d;border-bottom:0;border-radius:6px 6px 0 0;background:#161b22}.toolbar-title{font-size:13px;font-weight:600}.controls{display:flex;align-items:center;gap:16px;color:#8b949e;font-size:12px}.control{display:flex;align-items:center;gap:6px;cursor:pointer}.control input{accent-color:#2f81f7}.badge{display:none;min-width:18px;padding:0 5px;border-radius:10px;background:#30363d;color:#c9d1d9;text-align:center}.badge.visible{display:inline-block}.log-shell{min-height:340px;overflow:hidden;border:1px solid #30363d;border-radius:0 0 6px 6px;background:#0d1117}.log{padding:8px 0 16px;color:#c9d1d9;font:12px/1.65 ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace}.placeholder{padding:28px 20px;color:#8b949e;text-align:center;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.log-cluster{contain:layout style}.log-cluster.virtual{overflow:hidden}.log-line{display:grid;grid-template-columns:auto auto minmax(0,1fr);min-height:20px;padding:0 16px;white-space:pre-wrap;overflow-wrap:anywhere}.show-time .log-line{grid-template-columns:auto auto auto minmax(0,1fr)}.line-number{width:42px;padding-right:14px;color:#484f58;text-align:right;white-space:nowrap;user-select:none}.line-time{display:none;width:178px;padding-right:12px;color:#6e7681;white-space:nowrap}.show-time .line-time{display:block}.line-icon{display:inline-block;width:16px;margin-right:2px;color:#8b949e;text-align:center}.line-text{min-width:0}.log-line.runner{color:#8b949e}.log-line.command{color:#79c0ff}.log-line.input,.log-line.step-output{color:#8b949e;background:#161b224d}.metadata-key{color:#79c0ff}.log-line.debug{display:none;color:#8b949e}.show-debug .log-line.debug{display:grid}.log-line.notice{color:#58a6ff;background:#388bfd1a}.log-line.warning{color:#d29922;background:#bb800926}.log-line.error{color:#ff7b72;background:#f8514926}.annotation{display:block;color:#8b949e;font-size:11px}.log-group{border-top:1px solid #21262d;border-bottom:1px solid #21262d;background:#0d1117}.log-group+.log-group{border-top:0}.log-group>summary{display:flex;align-items:center;min-height:42px;padding:8px 16px;cursor:pointer;background:#161b22;color:#f0f6fc;font:600 13px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;list-style:none}.log-group>summary::-webkit-details-marker{display:none}.log-group>summary::before{content:"›";width:18px;color:#8b949e;font-size:18px;transform:rotate(0);transition:transform .1s}.log-group[open]>summary::before{transform:rotate(90deg)}.group-status{display:grid;place-items:center;flex:0 0 auto;width:17px;height:17px;margin-right:8px;border:2px solid currentColor;border-radius:50%;font-size:10px;font-weight:800}.group-status.running{color:#d29922;border-style:dashed}.group-status.success{color:#3fb950}.group-status.failure{color:#f85149}.group-status.cancelled{color:#8b949e}.group-status.success::after{content:"✓"}.group-status.failure::after{content:"×"}.group-status.cancelled::after{content:"−"}.group-time{display:none;margin-left:auto;color:#6e7681;font:11px ui-monospace,SFMono-Regular,Consolas,monospace}.show-time .group-time{display:block}.log-group .log-group{margin:4px 16px;border:1px solid #30363d;border-radius:5px}@media(max-width:800px){.topbar{padding:0 16px}.page{padding:20px 12px 40px}.layout{grid-template-columns:1fr}.sidebar{display:none}.job-header{padding:0 4px}.job-meta .optional{display:none}.toolbar{align-items:flex-start}.controls{gap:10px;flex-wrap:wrap;justify-content:flex-end}.log-line{padding:0 8px}.show-time .log-line{grid-template-columns:auto auto minmax(0,1fr)}.line-number{width:34px;padding-right:9px}.show-time .line-time{display:none}.show-time .group-time{display:none}}
  </style>
</head>
<body data-stream-url="{{.StreamURL}}">
  <nav class="topbar" aria-label="Global"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">OA</span><span>Open Actions</span></a><a href="/projects">Projects</a><a href="/dispatch">Run workflow</a></nav>
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
          <div class="state-pill live" id="state-pill"><span class="pulse" aria-hidden="true"></span><span id="state" aria-live="polite">Connecting…</span></div>
        </header>
        <div class="toolbar">
          <span class="toolbar-title">Runner logs</span>
          <div class="controls">
            <label class="control"><input id="debug-toggle" type="checkbox"> Show debug <span class="badge" id="debug-count">0</span></label>
            <label class="control"><input id="time-toggle" type="checkbox"> Show timestamps</label>
          </div>
        </div>
        <div class="log-shell"><div class="placeholder" id="placeholder">Waiting for log output…</div><div class="log" id="logs" role="log" aria-live="off"></div></div>
      </section>
    </div>
  </main>
  <script>
    const output=document.getElementById('logs');const placeholder=document.getElementById('placeholder');const state=document.getElementById('state');const statePill=document.getElementById('state-pill');const debugToggle=document.getElementById('debug-toggle');const timeToggle=document.getElementById('time-toggle');const debugCount=document.getElementById('debug-count');
    const clusterSize=50;const renderEntryLimit=500;const maxRenderedClusters=80;const overscan=1200;
    const containers=[{body:output,scope:'root',cluster:null}];const clusters=[];const clusterByElement=new WeakMap();const renderedClusters=new Set();
    let debugLines=0;let lineNumber=0;let received=false;let queuedEntries=[];let queueIndex=0;let renderScheduled=false;
    const timestamp=value=>{if(!value)return '';const date=new Date(value);return Number.isNaN(date.valueOf())?value:date.toLocaleTimeString(undefined,{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit',fractionalSecondDigits:3})};
    const iconFor=kind=>({debug:'◆',notice:'●',warning:'▲',error:'✕',runner:'›',command:'$',input:'↳','step-output':'↳'})[kind]||'';
    const conclusionLabel=value=>({success:'Succeeded',failure:'Failed',cancelled:'Cancelled'})[value]||'Running';
    function setGroupConclusion(container,conclusion){const mark=container.status;if(!mark)return;mark.className='group-status '+conclusion;mark.setAttribute('aria-label',conclusionLabel(conclusion));mark.title=conclusionLabel(conclusion)}
    function appendLogText(target,entry){
      if(!entry.parts){target.textContent=entry.text||'';return}
      for(const part of entry.parts){const span=document.createElement('span');span.textContent=part.text||'';if(part.foreground)span.style.color=part.foreground;if(part.background)span.style.backgroundColor=part.background;if(part.bold)span.style.fontWeight='700';if(part.dim)span.style.opacity='.65';if(part.italic)span.style.fontStyle='italic';const decoration=[];if(part.underline)decoration.push('underline');if(part.strike)decoration.push('line-through');if(decoration.length)span.style.textDecorationLine=decoration.join(' ');target.append(span)}
    }
    function createLine(record){
      const entry=record.entry;const line=document.createElement('div');line.className='log-line '+entry.kind;
      const number=document.createElement('span');number.className='line-number';number.textContent=String(record.number);
      const time=document.createElement('span');time.className='line-time';time.textContent=timestamp(entry.time);
      const icon=document.createElement('span');icon.className='line-icon';icon.textContent=iconFor(entry.kind);
      const text=document.createElement('span');text.className='line-text';
      if(entry.kind==='input'||entry.kind==='step-output'){const key=document.createElement('span');key.className='metadata-key';key.textContent=(entry.kind==='input'?'with ':'output ')+(entry.text||'');text.append(key)}else{appendLogText(text,entry)}
      if(entry.properties){const detail=[];if(entry.properties.title)detail.push(entry.properties.title);let location=entry.properties.file||'';if(entry.properties.line)location+=(location?':':'line ')+entry.properties.line;if(entry.properties.col)location+=':'+entry.properties.col;if(entry.properties.endline){location+='-'+entry.properties.endline;if(entry.properties.endcolumn)location+=':'+entry.properties.endcolumn}else if(entry.properties.endcolumn){location+='-'+entry.properties.endcolumn}if(location)detail.push(location);if(detail.length){const annotation=document.createElement('small');annotation.className='annotation';annotation.textContent=detail.join(' · ');text.append(document.createElement('br'),annotation)}}
      line.append(number,time,icon,text);return line;
    }
    function charactersPerLine(){const reserved=timeToggle.checked?260:85;return Math.max(20,Math.floor(Math.max(160,output.clientWidth-reserved)/7.2))}
    function estimateClusterHeight(cluster){
      const width=charactersPerLine();let height=0;
      for(const record of cluster.records){const entry=record.entry;if(entry.kind==='debug'&&!debugToggle.checked)continue;let text=entry.text||'';if(entry.kind==='input'||entry.kind==='step-output')text=(entry.kind==='input'?'with ':'output ')+text;let rows=0;for(const part of text.split('\n'))rows+=Math.max(1,Math.ceil(Math.max(1,part.length)/width));if(entry.properties&&(entry.properties.title||entry.properties.file||entry.properties.line))rows++;height+=Math.max(20,rows*19.8)}
      return height;
    }
    function updateClusterEstimate(cluster){if(cluster.rendered)return;cluster.height=estimateClusterHeight(cluster);cluster.element.style.height=cluster.height+'px'}
    function renderClusters(values){
      for(const cluster of values){if(cluster.rendered)continue;const fragment=document.createDocumentFragment();for(const record of cluster.records)fragment.append(createLine(record));cluster.element.replaceChildren(fragment);cluster.element.style.height='';cluster.element.classList.remove('virtual');cluster.rendered=true;renderedClusters.add(cluster)}
    }
    function virtualizeClusters(values){
      const candidates=Array.from(new Set(values)).filter(cluster=>cluster.rendered);if(!candidates.length)return;
      const heights=candidates.map(cluster=>cluster.element.getBoundingClientRect().height||estimateClusterHeight(cluster));
      for(let index=0;index<candidates.length;index++){const cluster=candidates[index];cluster.height=heights[index];cluster.element.replaceChildren();cluster.element.style.height=cluster.height+'px';cluster.element.classList.add('virtual');cluster.rendered=false;renderedClusters.delete(cluster)}
    }
    function selectionActive(){const selection=window.getSelection();return Boolean(selection&&!selection.isCollapsed)}
    function clustersIn(details){const values=[];for(const element of details.querySelectorAll('.log-cluster')){const cluster=clusterByElement.get(element);if(cluster)values.push(cluster)}return values}
    function renderVisibleClusters(details){
      const values=[];for(const cluster of clustersIn(details)){if(cluster.rendered)continue;const bounds=cluster.element.getBoundingClientRect();if(bounds.bottom>=-overscan&&bounds.top<=window.innerHeight+overscan)values.push(cluster)}renderClusters(values);
    }
    function trimRenderedClusters(){
      if(renderedClusters.size<=maxRenderedClusters||selectionActive())return;const candidates=[];
      for(const cluster of renderedClusters){const bounds=cluster.element.getBoundingClientRect();if(bounds.bottom < -overscan||bounds.top > window.innerHeight+overscan)candidates.push(cluster);if(renderedClusters.size-candidates.length<=maxRenderedClusters)break}
      virtualizeClusters(candidates);
    }
    function createAppender(){
      const fragments=new Map();
      return {append(parent,node){let fragment=fragments.get(parent);if(!fragment){fragment=document.createDocumentFragment();fragments.set(parent,fragment)}fragment.append(node)},commit(){const values=Array.from(fragments.entries());for(let index=values.length-1;index>=0;index--)values[index][0].append(values[index][1])}};
    }
    function createCluster(container,appender,rendered){
      const element=document.createElement('div');element.className='log-cluster'+(rendered?'':' virtual');const cluster={element,records:[],rendered,height:0};container.cluster=cluster;clusters.push(cluster);clusterByElement.set(element,cluster);if(rendered)renderedClusters.add(cluster);appender.append(container.body,element);clusterObserver.observe(element);return cluster;
    }
    function addEntry(entry,appender,renderNewClusters,groupsToCollapse){
      if(entry.kind==='group'){
        if(entry.scope==='workflow'||entry.scope==='post')while(containers.length>1)containers.pop();const parent=containers[containers.length-1];parent.cluster=null;
        const details=document.createElement('details');details.className='log-group';details.open=true;
        const summary=document.createElement('summary');const label=document.createElement('span');if(entry.text)appendLogText(label,entry);else label.textContent='Log group';
        let status;if(entry.scope==='workflow'){status=document.createElement('span');status.className='group-status running';status.setAttribute('aria-label','Running');status.title='Running';summary.append(status)}summary.append(label);
        if(entry.time){const time=document.createElement('span');time.className='group-time';time.textContent=timestamp(entry.time);summary.append(time)}
        const body=document.createElement('div');details.append(summary,body);details.addEventListener('toggle',()=>{if(details.open)requestAnimationFrame(()=>renderVisibleClusters(details));else if(!selectionActive())virtualizeClusters(clustersIn(details))});appender.append(parent.body,details);containers.push({body,details,status,scope:entry.scope||'command',cluster:null});return;
      }
      if(entry.kind==='endgroup'){for(let index=containers.length-1;index>0;index--){if(containers[index].scope===entry.scope){const container=containers[index];containers.length=index;if(entry.conclusion)setGroupConclusion(container,entry.conclusion);if(entry.scope==='workflow'&&entry.conclusion==='success')groupsToCollapse.push(container);containers[containers.length-1].cluster=null;break}}return}
      const container=containers[containers.length-1];let cluster=container.cluster;if(!cluster||cluster.records.length===clusterSize)cluster=createCluster(container,appender,renderNewClusters);
      const record={entry,number:++lineNumber};cluster.records.push(record);if(cluster.rendered)appender.append(cluster.element,createLine(record));else updateClusterEstimate(cluster);if(entry.kind==='debug')debugLines++;
    }
    function refreshVirtualEstimates(){for(const cluster of clusters)updateClusterEstimate(cluster)}
    function flushEntries(){
      renderScheduled=false;if(queueIndex===queuedEntries.length)return;const pinned=window.innerHeight+window.scrollY>=document.documentElement.scrollHeight-80;const appender=createAppender();const groupsToCollapse=[];const previousDebugLines=debugLines;const end=Math.min(queueIndex+renderEntryLimit,queuedEntries.length);
      while(queueIndex<end)addEntry(queuedEntries[queueIndex++],appender,pinned,groupsToCollapse);appender.commit();
      for(const container of groupsToCollapse){virtualizeClusters(clustersIn(container.details));container.details.open=false}
      if(debugLines!==previousDebugLines){debugCount.textContent=String(debugLines);debugCount.classList.add('visible')}trimRenderedClusters();if(pinned)window.scrollTo(0,document.documentElement.scrollHeight);
      if(queueIndex<queuedEntries.length){if(queueIndex>=5000){queuedEntries=queuedEntries.slice(queueIndex);queueIndex=0}scheduleRender()}else{queuedEntries=[];queueIndex=0}
    }
    function scheduleRender(){if(renderScheduled)return;renderScheduled=true;requestAnimationFrame(flushEntries)}
    function receive(entries){if(!received){placeholder.hidden=true;received=true}if(!Array.isArray(entries))entries=[entries];for(const entry of entries)queuedEntries.push(entry);scheduleRender()}
    const clusterObserver=new IntersectionObserver(entries=>{const show=[];const hide=[];for(const item of entries){const cluster=clusterByElement.get(item.target);if(!cluster)continue;if(item.isIntersecting)show.push(cluster);else if(cluster.rendered)hide.push(cluster)}renderClusters(show);if(!selectionActive())virtualizeClusters(hide);trimRenderedClusters()},{rootMargin:overscan+'px 0px'});
    debugToggle.addEventListener('change',()=>{output.classList.toggle('show-debug',debugToggle.checked);requestAnimationFrame(refreshVirtualEstimates)});timeToggle.addEventListener('change',()=>{output.classList.toggle('show-time',timeToggle.checked);requestAnimationFrame(refreshVirtualEstimates)});
    let resizeFrame=0;window.addEventListener('resize',()=>{cancelAnimationFrame(resizeFrame);resizeFrame=requestAnimationFrame(refreshVirtualEstimates)});
    const source=new EventSource(document.body.dataset.streamUrl);
    source.addEventListener('status',event=>{state.textContent=JSON.parse(event.data)});
    source.addEventListener('log',event=>receive(JSON.parse(event.data)));
    source.addEventListener('end',event=>{state.textContent=JSON.parse(event.data);statePill.classList.remove('live');source.close()});
    source.addEventListener('error',event=>{if(event.data){state.textContent=JSON.parse(event.data);statePill.classList.remove('live');source.close()}else{state.textContent='Connection lost; retrying…'}});
  </script>
</body>
</html>`
