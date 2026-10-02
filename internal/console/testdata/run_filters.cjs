const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');

const scripts = Array.from(readFileSync(0, 'utf8').matchAll(/<script>([\s\S]*?)<\/script>/g), match => match[1]);
const choices = {project: ['', 'team/project', 'team/other'], repository: ['', '123', '456'], workflow: ['', 'ci.yaml', 'test.yaml']};
class Select {
  constructor(values, value = '') { this.options = values.map(value => ({value, text: value})); this.value = value; }
  replaceChildren(...options) { this.options = options; }
}
function page(selection = {}, count = 3, options = choices, durations = {values: {}, active: false}) {
  const elements = Object.fromEntries(Object.keys(choices).map(name => [name, new Select(options[name], selection[name] || '')]));
  const listeners = new Map();
  const button = {hidden: false};
  const clear = {hidden: !Object.values(selection).some(Boolean), addEventListener: (name, callback) => listeners.set('clear:' + name, callback)};
  const form = {elements, querySelector: () => button, addEventListener: (name, callback) => listeners.set(name, callback)};
  const duration = {dataset: {duration: '/runs/team/run'}};
  const nodes = {
    '.page-heading': {},
    '.runs': {
      dataset: {durationUrl: '/durations?run=team%2Frun'},
      querySelector: selector => { assert.equal(selector, '#run-durations'); return {textContent: JSON.stringify(durations)}; },
      querySelectorAll: selector => { assert.equal(selector, '.run'); return Array(count).fill({}); },
      setAttribute(name, value) { this[name] = value; },
      removeAttribute(name) { delete this[name]; },
    },
  };
  const status = {};
  const view = {
    elements, listeners, button, clear, nodes, status, duration,
    getElementById: id => ({'run-filters': form, 'clear-filters': clear, 'filter-status': status}[id] || elements[id.replace('run-', '')]),
    querySelector: selector => nodes[selector],
    querySelectorAll: selector => selector === '[data-duration]' ? [duration] : [],
  };
  for (const element of Object.values(elements)) element.focus = () => { view.activeElement = element; };
  for (const selector of Object.keys(nodes)) {
    nodes[selector].replaceWith = function replace(next) { nodes[selector] = next; next.replaceWith = replace; };
  }
  return view;
}

async function main() {
  const view = page();
  const requests = [];
  const history = [];
  const windowEvents = new Map();
  const location = {pathname: '/', search: ''};
  const timers = new Map();
  let timerID = 0;
  const context = vm.createContext({
    document: view,
    window: {addEventListener: (name, callback) => windowEvents.set(name, callback)},
    location,
    history: {pushState(_, __, url) { history.push(url); location.search = url.includes('?') ? '?' + url.split('?')[1] : ''; }},
    URLSearchParams: class extends URLSearchParams { get size() { return undefined; } },
    AbortController,
    DOMParser: class { parseFromString(data) { return data; } },
    performance: {now: () => 0},
    setInterval: callback => { timers.set(++timerID, callback); return timerID; },
    clearInterval: id => timers.delete(id),
    setTimeout: callback => { timers.set(++timerID, callback); return timerID; },
    clearTimeout: id => timers.delete(id),
    fetch: (url, options) => new Promise(resolve => requests.push({url, options, resolve})),
  });
  for (const script of scripts) vm.runInContext(script, context);
  const settle = async (request, result, ok = true) => {
    request.resolve({ok, text: async () => result, json: async () => result});
    await new Promise(resolve => setImmediate(resolve));
  };
  const change = (name, value) => {
    view.elements[name].value = value;
    view.listeners.get('change')({target: {name}});
    return requests.at(-1);
  };
  assert.equal(view.button.hidden, true);

  view.elements.project.value = 'team/project';
  view.elements.repository.value = '123';
  view.elements.workflow.value = 'ci.yaml';
  const project = change('project', 'team/other');
  assert.equal(project.url, '/?project=team%2Fother');
  assert.equal(view.elements.repository.value, '');
  assert.equal(view.elements.workflow.value, '');
  assert.equal(view.elements.repository.disabled, true);
  assert.equal(view.nodes['.runs']['aria-busy'], 'true');
  await settle(project, page({project: 'team/other'}, 2, {...choices, repository: ['', '456']}));
  assert.equal(view.elements.repository.options.length, 2);
  assert.equal(view.elements.repository.disabled, false);
  assert.equal(view.status.textContent, '2 workflow runs shown');
  assert.equal(history.at(-1), project.url);

  const repository = change('repository', '456');
  assert.equal(repository.url, '/?project=team%2Fother&repository=456');
  assert.equal(view.elements.workflow.disabled, true);
  await settle(repository, page({project: 'team/other', repository: '456'}, 1));
  const activeOptions = view.elements.workflow.options;
  const first = change('workflow', 'ci.yaml');
  const second = change('workflow', 'test.yaml');
  assert.equal(first.options.signal.aborted, true);
  await settle(second, page({project: 'team/other', repository: '456', workflow: 'test.yaml'}, 0));
  assert.equal(view.elements.workflow.options, activeOptions, 'Unchanged options should keep their DOM nodes');
  await settle(first, page({project: 'team/other', repository: '456', workflow: 'ci.yaml'}, 1));
  assert.equal(view.elements.workflow.value, 'test.yaml');
  assert.equal(view.status.textContent, '0 workflow runs shown');
  assert.equal(history.at(-1), second.url);

  const failed = change('workflow', 'ci.yaml');
  await settle(failed, null, false);
  assert.equal(history.at(-1), second.url);
  assert.equal(view.button.hidden, false);
  assert.equal(view.button.textContent, 'Retry');
  assert.equal(view.nodes['.runs']['aria-busy'], undefined);
  let prevented = false;
  view.listeners.get('submit')({preventDefault() { prevented = true; }});
  assert.equal(prevented, true);
  await settle(requests.at(-1), page({project: 'team/other', repository: '456', workflow: 'ci.yaml'}, 1));
  assert.equal(view.button.hidden, true);

  const historyLength = history.length;
  location.search = '?project=team%2Fproject';
  windowEvents.get('popstate')();
  assert.equal(requests.at(-1).url, '/?project=team%2Fproject');
  await settle(requests.at(-1), page({project: 'team/project'}, 2));
  assert.equal(view.elements.project.value, 'team/project');
  assert.equal(view.elements.repository.value, '');
  assert.equal(history.length, historyLength);

  view.activeElement = view.clear;
  view.listeners.get('clear:click')({button: 0, preventDefault() {}});
  assert.equal(requests.at(-1).url, '/');
  await settle(requests.at(-1), page({}, 3));
  assert.equal(view.clear.hidden, true);
  assert.equal(view.activeElement, view.elements.project);
  assert.equal(view.elements.project.value, '');
  assert.equal(location.search, '');

  const running = {values: {'/runs/team/run': {seconds: 10, running: true}}, active: true};
  const terminal = {values: {'/runs/team/run': {seconds: 20, running: false}}, active: false};
  await settle(change('repository', '123'), page({repository: '123'}, 1, choices, running));
  view.elements.repository.focus();
  assert.equal(view.duration.textContent, '10s');
  assert.equal(timers.size, 2);
  Array.from(timers.values()).at(-1)();
  const durationRequest = requests.at(-1);
  await settle(change('repository', '456'), page({repository: '456'}, 1, choices, terminal));
  assert.equal(view.duration.textContent, '20s');
  assert.equal(timers.size, 0);
  await settle(durationRequest, running);
  assert.equal(view.duration.textContent, '20s', 'Stale duration requests must not overwrite the current list');
  assert.equal(timers.size, 0);
  assert.equal(view.activeElement, view.elements.repository);

  view.activeElement = view.clear;
  view.listeners.get('clear:click')({button: 0, preventDefault() {}});
  view.elements.workflow.focus();
  await settle(requests.at(-1), page({}, 3));
  assert.equal(view.activeElement, view.elements.workflow, 'Completing a clear request must not take focus back from another control');
}
main().catch(error => { console.error(error); process.exitCode = 1; });
