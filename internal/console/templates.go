package console

const loginPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Sign in - Open Actions</title>
  <style>
    :root{color-scheme:light dark;font-family:ui-sans-serif,system-ui,sans-serif}body{min-height:100vh;margin:0;display:grid;place-items:center;background:#0d1117;color:#e6edf3}.card{width:min(360px,calc(100% - 3rem));padding:2rem;border:1px solid #30363d;border-radius:12px;background:#161b22}h1{margin:.25rem 0 .5rem;font-size:1.5rem}p{color:#8b949e;line-height:1.5}label{display:block;margin:1.5rem 0 .4rem;font-weight:600}input,button{box-sizing:border-box;width:100%;padding:.7rem .8rem;border-radius:6px;font:inherit}input{border:1px solid #30363d;background:#0d1117;color:inherit}button{margin-top:1rem;border:0;background:#238636;color:white;font-weight:600;cursor:pointer}button:disabled{opacity:.6;cursor:wait}#error{min-height:1.5rem;margin:.75rem 0 0;color:#f85149}
  </style>
</head>
<body>
  <main class="card">
    <p>Open Actions Console</p>
    <h1>Sign in</h1>
    <p>Enter the administrator token configured for this Console.</p>
    <form id="login">
      <label for="token">Token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" required autofocus>
      <button type="submit">Sign in</button>
      <p id="error" role="alert"></p>
    </form>
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

const runPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{.WorkflowName}} - Open Actions</title>
  <style>
    :root{color-scheme:light dark;font-family:ui-sans-serif,system-ui,sans-serif}body{max-width:960px;margin:0 auto;padding:2rem}header{border-bottom:1px solid #8885;margin-bottom:2rem}h1{margin-bottom:.25rem}.meta{color:#777;overflow-wrap:anywhere}.status{display:inline-block;border:1px solid #8888;border-radius:999px;padding:.2rem .7rem;font-weight:600}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:.75rem;border-bottom:1px solid #8885}a{color:#2f81f7}code{font-size:.85em}
  </style>
</head>
<body>
  <header><p>Open Actions</p><h1>{{.WorkflowName}}</h1><p class="meta">{{.Repository}} · <code>{{.WorkflowPath}}</code> · <code>{{.Revision}}</code></p><p><span class="status">{{.Status}}</span></p></header>
  <main><h2>Jobs</h2><table><thead><tr><th>Job</th><th>Status</th><th>Runner</th><th>Started</th><th>Completed</th></tr></thead><tbody>
  {{range .Jobs}}<tr><td><a href="{{.URL}}">{{.ID}}</a></td><td>{{.Status}}</td><td>{{.Runner}}</td><td>{{.Started}}</td><td>{{.Completed}}</td></tr>{{else}}<tr><td colspan="5">No jobs have been created.</td></tr>{{end}}
  </tbody></table></main>
</body>
</html>`

const logPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{.JobID}} - Open Actions</title>
  <style>
    :root{color-scheme:dark;font-family:ui-sans-serif,system-ui,sans-serif}body{margin:0;background:#0d1117;color:#e6edf3}header{padding:1rem 1.5rem;border-bottom:1px solid #30363d}header a{color:#58a6ff}h1{font-size:1.15rem;margin:.6rem 0 0}#state{color:#8b949e}pre{white-space:pre-wrap;overflow-wrap:anywhere;margin:0;padding:1.5rem;font:13px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace}
  </style>
</head>
<body data-stream-url="{{.StreamURL}}">
  <header><a href="{{.RunURL}}">← Workflow run</a><h1>{{.JobID}} · {{.Status}}</h1><div id="state">Connecting…</div></header>
  <pre id="logs"></pre>
  <script>
    const output=document.getElementById('logs');const state=document.getElementById('state');
    const source=new EventSource(document.body.dataset.streamUrl);
    source.addEventListener('status',event=>{state.textContent=JSON.parse(event.data)});
    source.addEventListener('log',event=>{output.textContent+=JSON.parse(event.data);window.scrollTo(0,document.body.scrollHeight)});
    source.addEventListener('end',event=>{state.textContent=JSON.parse(event.data);source.close()});
    source.addEventListener('error',event=>{if(event.data){state.textContent=JSON.parse(event.data);source.close()}else{state.textContent='Connection lost; retrying…'}});
  </script>
</body>
</html>`
