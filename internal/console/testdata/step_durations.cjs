const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');

const html = readFileSync(0, 'utf8');
const scripts = Array.from(html.matchAll(/<script>([\s\S]*?)<\/script>/g), match => match[1]);
const epoch = Date.parse('2026-09-29T12:00:00Z');
const at = seconds => new Date(epoch + seconds * 1000).toISOString();
const start = (scope, seconds, extra = {}) => ({kind: 'group', scope, time: at(seconds), text: 'Build', ...extra});
const finish = (scope, seconds, conclusion = 'success') => ({kind: 'endgroup', scope, time: at(seconds), conclusion});

class Element {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.className = '';
    this.style = {};
    this.dataset = {};
    this.classList = {remove() {}, toggle() {}};
  }
  append(...children) {
    for (const child of children) {
      if (child.tag === 'fragment') this.children.push(...child.children.splice(0));
      else this.children.push(child);
    }
  }
  addEventListener() {}
  setAttribute(name, value) { this[name] = value; }
  querySelectorAll(selector) {
    return this.children.flatMap(child => [
      ...(child.className.split(' ').includes(selector.slice(1)) ? [child] : []),
      ...child.querySelectorAll(selector),
    ]);
  }
}

function page() {
  let now = epoch;
  const elements = new Map(['logs', 'placeholder', 'state', 'state-pill', 'time-toggle'].map(id => [id, new Element('div')]));
  const frames = [];
  const timers = new Map();
  const events = new Map();
  let timerID = 0;
  let closed = false;
  const context = vm.createContext({
    Date: class extends Date { static now() { return now; } },
    performance: {now: () => now - epoch},
    document: {
      addEventListener() {},
      getElementById: id => elements.get(id),
      createElement: tag => new Element(tag),
      createTextNode: text => Object.assign(new Element('text'), {textContent: text}),
      createDocumentFragment: () => new Element('fragment'),
      querySelectorAll: () => [],
      body: {dataset: {streamUrl: '/stream'}},
      documentElement: {scrollHeight: 0},
    },
    window: {innerHeight: 800, scrollY: 0, scrollTo() {}, addEventListener() {}, getSelection: () => null},
    requestAnimationFrame: callback => frames.push(callback),
    cancelAnimationFrame() {},
    setInterval: callback => { timers.set(++timerID, callback); return timerID; },
    clearInterval: id => timers.delete(id),
    setTimeout() {},
    IntersectionObserver: class { observe() {} },
    EventSource: class {
      addEventListener(name, callback) { events.set(name, callback); }
      close() { closed = true; }
    },
  });
  for (const script of scripts) vm.runInContext(script, context);
  const flush = () => { while (frames.length) frames.shift()(); };
  const emit = (name, data) => events.get(name)({data: data === undefined ? undefined : JSON.stringify(data)});
  return {
    emit,
    flush,
    receive(entries) { emit('log', entries); flush(); },
    tick(seconds) { now = epoch + seconds * 1000; for (const timer of timers.values()) timer(); },
    groups: () => elements.get('logs').querySelectorAll('.log-group'),
    durations: () => elements.get('logs').querySelectorAll('.group-duration').map(element => element.textContent),
    closed: () => closed,
  };
}

// GitHub displays each step's execution time in its log header:
// https://docs.github.com/en/actions/how-tos/monitor-workflows/use-workflow-run-logs
for (const conclusion of ['success', 'failure', 'cancelled']) {
  const view = page();
  view.tick(125);
  view.receive(start('workflow', 0));
  assert.deepEqual(view.durations(), ['2m 5s']);
  view.tick(126);
  assert.deepEqual(view.durations(), ['2m 6s']);
  view.receive(finish('workflow', 61, conclusion));
  assert.deepEqual(view.durations(), ['1m 1s']);
  assert.equal(view.groups()[0].open, conclusion !== 'success');
  view.tick(500);
  assert.deepEqual(view.durations(), ['1m 1s']);
}

{
  const view = page();
  view.tick(65);
  view.receive([start('workflow', 0), start('composite', 10), start('command', 15)]);
  assert.deepEqual(view.durations(), ['1m 5s', '55s']);
  view.receive(finish('command', 20));
  assert.deepEqual(view.durations(), ['1m 5s', '55s']);
  view.receive(finish('composite', 40));
  view.tick(70);
  assert.deepEqual(view.durations(), ['1m 10s', '30s']);
  view.receive(finish('workflow', 65));
  view.receive([start('workflow', 66, {text: 'Post actions/checkout@v4'}), finish('workflow', 68)]);
  assert.deepEqual(view.durations(), ['1m 5s', '30s', '2s']);
}

{
  const view = page();
  view.receive([
    start('workflow', 0, {conclusion: 'skipped'}),
    start('workflow', 1, {time: undefined}), finish('workflow', 5),
    start('workflow', 6), {...finish('workflow', 7), time: undefined},
    start('workflow', 8), finish('workflow', 8.9),
    start('workflow', 10), finish('workflow', 3671.9),
    start('workflow', 20), finish('workflow', 19),
  ]);
  assert.deepEqual(view.durations(), ['—', '—', '—', '0s', '1h 1m 1s', '0s']);
}

{
  const view = page();
  view.receive([start('workflow', 0), start('composite', 1), finish('workflow', 3, 'failure')]);
  assert.deepEqual(view.durations(), ['3s', '—']);
  view.receive([start('workflow', 4), start('workflow', 5), finish('workflow', 7)]);
  view.tick(20);
  assert.deepEqual(view.durations(), ['3s', '—', '—', '2s']);
}

for (const terminalEvent of ['end', 'error']) {
  const view = page();
  view.receive(start('workflow', 0));
  view.tick(10);
  view.emit('error');
  view.tick(11);
  assert.deepEqual(view.durations(), ['11s']);
  view.emit('log', [finish('workflow', 12), start('workflow', 13)]);
  view.emit(terminalEvent, 'Log stream ended.');
  view.flush();
  view.tick(60);
  assert.equal(view.closed(), true);
  assert.deepEqual(view.durations(), ['12s', '—']);
}
