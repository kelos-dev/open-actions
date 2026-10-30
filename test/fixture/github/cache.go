package main

const cacheWorkflowPath = ".open-actions/workflows/cache.yaml"

const cacheWorkflowData = `name: Cache exchange
on: push
jobs:
  cached:
    runs-on: ubuntu-latest
    outputs:
      hit: ${{ steps.cache.outputs.cache-hit }}
    steps:
      - id: cache
        uses: actions/cache@v4
        with:
          key: fixture-cache
          path: cached-marker
      - name: Verify restored content or create cache
        run: |
          if [ '${{ steps.cache.outputs.cache-hit }}' = true ]; then
            test "$(cat cached-marker)" = 'cache across runner Pods'
          else
            test ! -e cached-marker
            printf 'cache across runner Pods' > cached-marker
          fi
`

const cacheMetadata = `name: Cache protocol fixture
inputs:
  key:
    required: true
  path:
    required: true
runs:
  using: node20
  main: dist/index.js
  post: dist/index.js
  post-if: success()
`

const cacheScript = `const fs = require('fs');
async function rpc(method, payload) {
  const response = await fetch(process.env.ACTIONS_RESULTS_URL + '/twirp/github.actions.results.api.v1.CacheService/' + method, {
    method: 'POST', headers: {'Content-Type': 'application/json', Authorization: 'Bearer ' + process.env.ACTIONS_RUNTIME_TOKEN}, body: JSON.stringify(payload)
  });
  if (!response.ok) throw new Error(method + ': ' + response.status + ' ' + await response.text());
  return response.json();
}
async function main() {
  if (process.env.ACTIONS_CACHE_SERVICE_V2 !== 'true' || process.env.ACTIONS_CACHE_MODE !== 'write' || !process.env.ACTIONS_CACHE_URL) throw new Error('cache runtime environment is incomplete');
  const key = process.env.INPUT_KEY;
  const path = process.env.INPUT_PATH;
  const input = {key, version: 'fixture-cache-v1'};
  if (process.env.STATE_CACHE_PHASE === 'post') {
    if (process.env.STATE_CACHE_HIT === 'true') return;
    const created = await rpc('CreateCacheEntry', input);
    if (!created.ok) throw new Error('cache reservation failed');
    const data = fs.readFileSync(path);
    const uploaded = await fetch(created.signed_upload_url, {method: 'PUT', body: data});
    if (!uploaded.ok) throw new Error('cache upload failed: ' + uploaded.status);
    const finalized = await rpc('FinalizeCacheEntryUpload', {...input, size_bytes: String(data.length)});
    if (!finalized.ok) throw new Error('cache finalization failed');
    console.log('cache saved from post hook');
    return;
  }
  const result = await rpc('GetCacheEntryDownloadURL', input);
  if (result.ok) {
    const response = await fetch(result.signed_download_url);
    if (!response.ok) throw new Error('cache download failed: ' + response.status);
    fs.writeFileSync(path, Buffer.from(await response.arrayBuffer()));
  }
  const hit = result.ok ? 'true' : '';
  fs.appendFileSync(process.env.GITHUB_OUTPUT, 'cache-hit=' + hit + '\n');
  fs.appendFileSync(process.env.GITHUB_STATE, 'CACHE_PHASE=post\nCACHE_HIT=' + hit + '\n');
}
main().catch(error => { console.error(error); process.exitCode = 1; });
`
