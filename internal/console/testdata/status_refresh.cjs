const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');

const snapshots = JSON.parse(readFileSync(0, 'utf8'));
const script = Array.from(snapshots[0].html.matchAll(/<script>([\s\S]*?)<\/script>/g), match => match[1]).find(script => script.includes('function trackDurations('));
assert.ok(script);
const elements = (html, attribute, key) => Array.from(html.matchAll(new RegExp(attribute + '="([^"]*)"', 'g')), match => ({dataset: {[key]: match[1]}}));
function page(html) {
  const markup = html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g, '');
  const sections = elements(markup, 'data-refresh', 'refresh');
  const durations = elements(markup, 'data-duration', 'duration');
  const labels = elements(markup, 'data-status', 'status');
  const marks = elements(markup, 'data-status-class', 'statusClass');
  const view = {
    html, sections, durations, labels, marks,
    querySelector: selector => sections.find(section => selector === '[data-refresh="' + section.dataset.refresh + '"]'),
  };
  for (const section of sections) {
    section.page = view;
    section.contains = element => element === section || element?.parent === section;
  }
  return view;
}

function browser() {
  const view = page(snapshots[0].html);
  const requests = [];
  const timers = new Map();
  const intervals = new Map();
  let timerID = 0;
  let now = 0;
  let replacements = 0;
  let selectedSections = [];
  const replace = function(next) {
    const index = view.sections.indexOf(this);
    assert.ok(index >= 0);
    view.sections[index] = next;
    next.replaceWith = replace;
    view.durations = next.page.durations;
    replacements++;
  };
  for (const section of view.sections) section.replaceWith = replace;
  const document = {
    activeElement: null,
    querySelectorAll: selector => ({'[data-duration]': view.durations, '[data-refresh]': view.sections,
      '[data-status]': view.labels, '[data-status-class]': view.marks, '[data-time]': []}[selector] || []),
  };
  const context = vm.createContext({
    document,
    window: {getSelection: () => ({
      isCollapsed: selectedSections.length === 0,
      rangeCount: selectedSections.length,
      getRangeAt: index => ({intersectsNode: section => selectedSections[index] === section}),
    })},
    location: {pathname: '/runs/default/ci', search: '?view=progress'},
    DOMParser: class {
      parseFromString(html) { return page(html); }
    },
    performance: {now: () => now},
    setInterval: callback => { intervals.set(++timerID, callback); return timerID; },
    clearInterval: id => intervals.delete(id),
    setTimeout: (callback, delay) => { assert.equal(delay, 5000); timers.set(++timerID, callback); return timerID; },
    clearTimeout: id => timers.delete(id),
    fetch: (url, options) => new Promise(resolve => requests.push({url, options, resolve})),
  });
  vm.runInContext(script, context);
  return {
    view, requests, timers, intervals,
    interact(kind, section) {
      if (kind === 'focus') document.activeElement = section ? {parent: section} : null;
      else selectedSections = section ? [section] : [];
    },
    replacements: () => replacements,
    stop: () => vm.runInContext('stopDurationTracking()', context),
    tick: seconds => { now += seconds * 1000; for (const callback of intervals.values()) callback(); },
    poll() {
      const [id, callback] = timers.entries().next().value;
      timers.delete(id);
      callback();
      return requests.at(-1);
    },
  };
}
const settle = async (request, result, ok = true) => {
  assert.equal(request.options.cache, 'no-store');
  request.resolve({ok, text: async () => result, json: async () => result});
  await new Promise(resolve => setImmediate(resolve));
};
async function update(view, snapshot) {
  await settle(view.poll(), snapshot.data);
  if (view.view.sections.length) {
    assert.equal(view.requests.at(-1).url, '/runs/default/ci?view=progress');
    await settle(view.requests.at(-1), snapshot.html);
  }
}

async function main() {
  // Workflow pages expose the current result and job progress:
  // https://docs.github.com/en/actions/how-tos/monitor-workflows/use-workflow-run-logs
  const view = browser();
  const expectedSections = snapshots[0].html.includes('class="job-header"') ? 3 : snapshots[0].html.includes('class="run-heading"') ? 1 : 0;
  assert.equal(view.view.sections.length, expectedSections);
  assert.equal(view.timers.size, 1);
  await settle(view.poll(), snapshots[0].data);
  assert.equal(view.requests.length, 1, 'Unchanged status must not fetch another page');
  await update(view, snapshots[1]);
  assert.ok(view.view.durations.every(element => /^1m \d+s$/.test(element.textContent)));
  view.tick(1);
  const changed = view.replacements();
  await update(view, snapshots[2]);
  assert.ok(view.view.durations.every(element => element.textContent === '1m 5s'));
  assert.equal(view.timers.size, 0, 'Stop polling after rendering the final result');
  assert.equal(view.intervals.size, 0);
  if (view.view.sections.length) {
    assert.equal(view.replacements(), changed * 2);
  } else {
    assert.ok(view.view.labels.length > 0);
    assert.ok(view.view.labels.every(element => element.textContent === 'Succeeded'));
    assert.ok(view.view.marks.every(element => element.className === 'status-mark succeeded'));
  }

  if (view.view.sections.length) {
    for (const kind of ['focus', 'selection']) {
      for (const timing of ['before request', 'during request']) {
        const interacting = browser();
        if (timing === 'before request') interacting.interact(kind, interacting.view.sections[0]);
        await settle(interacting.poll(), snapshots[2].data);
        if (timing === 'during request') {
          interacting.interact(kind, interacting.view.sections[0]);
          await settle(interacting.requests.at(-1), snapshots[2].html);
        } else {
          assert.equal(interacting.requests.length, 1, 'Do not fetch page sections while they are in use');
        }
        assert.equal(interacting.replacements(), 0, `${kind} ${timing} must postpone section replacement`);
        assert.equal(interacting.timers.size, 1, 'Completion must keep polling while an update is postponed');
        interacting.interact(kind, null);
        await update(interacting, snapshots[2]);
        assert.equal(interacting.replacements(), interacting.view.sections.length, 'Refresh after interaction ends');
        assert.equal(interacting.timers.size, 0);
      }
      const outside = browser();
      outside.interact(kind, {});
      await update(outside, snapshots[2]);
      assert.equal(outside.replacements(), outside.view.sections.length, 'Interaction outside refreshed sections must not delay progress');
      assert.equal(outside.timers.size, 0);
    }

    const retry = browser();
    await settle(retry.poll(), snapshots[2].data);
    await settle(retry.requests.at(-1), null, false);
    assert.equal(retry.replacements(), 0);
    assert.equal(retry.timers.size, 1, 'Retry a failed page refresh even after completion');
    await update(retry, snapshots[2]);
    assert.equal(retry.replacements(), retry.view.sections.length);
    assert.equal(retry.timers.size, 0);

    const stopped = browser();
    await settle(stopped.poll(), snapshots[1].data);
    const pending = stopped.requests.at(-1);
    stopped.stop();
    await settle(pending, snapshots[1].html);
    assert.equal(stopped.replacements(), 0, 'A stopped refresh must not overwrite another view');
    assert.equal(stopped.timers.size, 0);

    const added = browser();
    const data = structuredClone(snapshots[0].data);
    data.statuses.test = {label: 'Queued', class: 'queued'};
    await settle(added.poll(), data);
    assert.equal(added.requests.length, 2, 'New jobs must refresh the job list even without a status transition');
    await settle(added.requests.at(-1), snapshots[0].html);
    assert.equal(added.replacements(), added.view.sections.length);
    added.stop();
  }

  const stopped = browser();
  const pending = stopped.poll();
  stopped.stop();
  await settle(pending, snapshots[2].data);
  assert.equal(stopped.requests.length, 1);
  assert.equal(stopped.timers.size, 0);
  assert.equal(stopped.replacements(), 0);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
