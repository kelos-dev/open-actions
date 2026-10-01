const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');

const html = readFileSync(0, 'utf8');
const scripts = Array.from(html.matchAll(/<script>([\s\S]*?)<\/script>/g), match => match[1]);
const listeners = new Map();
const form = {
  elements: {project: {value: ''}, repository: {value: ''}, workflow: {value: ''}},
  addEventListener(name, callback) { listeners.set(name, callback); },
  requestSubmit() { assert.fail('Filter selection must wait for Apply'); },
  submit() { assert.fail('Filter selection must wait for Apply'); },
};
const context = vm.createContext({
  document: {
    getElementById: id => id === 'run-filters' ? form : null,
    querySelectorAll: () => [],
  },
  performance: {now: () => 0},
  setInterval() {},
  setTimeout() {},
});
for (const script of scripts) vm.runInContext(script, context);

for (const {field, value, expected} of [
  {field: 'project', value: 'team/other', expected: ['team/other', '', '']},
  {field: 'repository', value: '456', expected: ['team/project', '456', '']},
  {field: 'workflow', value: 'test.yaml', expected: ['team/project', '123', 'test.yaml']},
]) {
  form.elements.project.value = 'team/project';
  form.elements.repository.value = '123';
  form.elements.workflow.value = 'ci.yaml';
  form.elements[field].value = value;
  listeners.get('change')({target: {name: field}});
  assert.deepEqual(Object.values(form.elements).map(element => element.value), expected);
}
