package main

const jobServiceWorkflowPath = ".open-actions/workflows/job-service.yaml"

const jobServiceWorkflowData = `name: Workflow job Service
on: push
jobs:
  preview:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - name: Serve preview
        run: |
          node <<'JS'
          const http = require('node:http');
          if (Object.keys(process.env).some(name => name.startsWith('JOB_') && name.endsWith('_SERVICE_HOST'))) {
            throw new Error('Workflow steps received job Service links');
          }
          http.createServer((request, response) => {
            response.end('Workflow preview\n');
          }).listen(8080, '0.0.0.0');
          JS
`
