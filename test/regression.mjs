// INC-005 regression suite for the dsh-schedule-later fork (0.6.0-fork.1).
// Pure node, no GUI, no network. Run: node test/regression.mjs
//
// Covers the three INC-005 root-cause fixes:
//   t1  NO_LIVE_AGENT backoff (no setTimeout(0) re-arm on a past sendAt;
//       nextAttempt = min(now + 60s*2^floor(attempts/3), now + 15min);
//       attempts increment; bounded rate; success resets the counter).
//   t2  erasure immunity (persist = locked read-modify-write, merge by id —
//       a second process context's items survive this instance's persists;
//       intentionally-cancelled items do NOT resurrect).
//   t3  settle-before-remove (in-flight flag 'delivering' + startedAt
//       persisted before await; item removed only after delivery settles;
//       crash-on-disk 'delivering' restore re-queues → at-least-once).
//   t4  generated lib/client.js registers exactly the package.json name
//       (INC-2026-10-18: a bundle built under a pre-rename name executes but
//       never registers the awaited loader row → web boot fails).
//
// CLI: `node test/regression.mjs <bundle.js>` runs ONLY the t4 registration
// assertion against an arbitrary bundle (e.g. the pre-fix backup copy, where
// it must FAIL).

import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createScheduler } from '../src/scheduler.js';

const HERE = dirname(fileURLToPath(import.meta.url));
let passed = 0;
let failed = 0;
function ok(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.log(`  FAIL ${name}: ${err.message}`);
  }
}
async function okAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.log(`  FAIL ${name}: ${err && err.message}`);
  }
}

// Fake clock + absolute-time timer queue, advanced in 1 ms steps.
function makeFakeTimers(initialNow) {
  const st = { t: initialNow };
  const clock = { now: () => st.t };
  const timers = {
    queue: [],
    nextId: 1,
    setTimeoutAt(fn, atMs) {
      const id = this.nextId++;
      this.queue.push({ id, at: atMs, fn });
      return id;
    },
    clearTimeout(id) {
      this.queue = this.queue.filter((x) => x.id !== id);
    },
    pending() {
      return this.queue.map((x) => ({ id: x.id, at: x.at }));
    },
    async advance(ms) {
      const target = st.t + ms;
      for (;;) {
        st.t += 1;
        // fire every timer that came due at this instant (in at-order)
        for (;;) {
          const ready = this.queue.filter((x) => x.at <= st.t).sort((a, b) => a.at - b.at);
          if (!ready.length) break;
          this.queue = this.queue.filter((x) => x.at > st.t);
          for (const x of ready) {
            const r = x.fn();
            if (r && typeof r.then === 'function') await r;
          }
        }
        if (st.t >= target) break;
      }
      return st.t;
    },
  };
  return { clock, timers, state: st };
}

function noLiveAgentErr() {
  const e = new Error('no live agent for this chat; the task stays queued and is delivered once the chat is back');
  e.code = 'NO_LIVE_AGENT';
  return e;
}

function readItems(dataDir) {
  return JSON.parse(readFileSync(join(dataDir, 'tasks.json'), 'utf8')).items;
}

// t4 (INC-2026-10-18): the DSH web loader keys browser boot rows by npm
// package name, so the generated lib/client.js must call
// window.__ModuleLoader__.load( exactly once and register exactly that name.
function checkRegistrationIdMatchesPackage(bundlePath, pkgName) {
  const src = readFileSync(bundlePath, 'utf8');
  const loads = src.match(/window\.__ModuleLoader__\.load\(/g) || [];
  assert.equal(loads.length, 1, `exactly one window.__ModuleLoader__.load( call (found ${loads.length})`);
  const escaped = pkgName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const reg = src.match(new RegExp('window\\.__ModuleLoader__\\.load\\(\\{\\s*id:\\s*"(' + escaped + ')"'));
  assert.ok(reg, `registration id must equal package name "${pkgName}"`);
  // stale-registration guard (negative lookahead: not the hardened name)
  const stale = src.match(/window\.__ModuleLoader__\.load\(\{\s*id:\s*"dsh-schedule-later"(?!-hardened)/g) || [];
  assert.equal(stale.length, 0, 'no registration under the pre-rename id "dsh-schedule-later"');
}

if (process.argv[2]) {
  const target = process.argv[2];
  const pkgName = JSON.parse(readFileSync(join(HERE, '..', 'package.json'), 'utf8')).name;
  try {
    checkRegistrationIdMatchesPackage(target, pkgName);
    console.log(`REGISTRATION CHECK (external bundle ${target}): PASS`);
    process.exit(0);
  } catch (err) {
    console.log(`REGISTRATION CHECK (external bundle ${target}): FAIL — ${err.message}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
console.log('t1: NO_LIVE_AGENT exponential backoff (no past-timestamp re-arm)');
await okAsync('t1.1 schedule + first NO_LIVE_AGENT failure → nextAttempt ≥ 60 s in the future', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sl-t1-'));
  const { clock, timers, state } = makeFakeTimers(1_000_000_000_000);
  const sched = await createScheduler({
    dataDir: dir,
    clock,
    timers,
    deliver: async () => { throw noLiveAgentErr(); },
  });
  const originalSendAt = state.t + 1000;
  const item = await sched.schedule({ content: 't1', sendAt: originalSendAt, conversationId: 'dead-chat' });
  await timers.advance(1000); // fire at due time
  const t1 = sched.list().find((x) => x.id === item.id);
  assert.ok(t1, 'item still in queue after failure');
  assert.equal(t1.attempts, 1, 'attempts incremented to 1');
  assert.ok(t1.sendAt - state.t >= 60_000, `nextAttempt ${t1.sendAt - state.t} ms must be ≥ 60 s future`);
  // no zero/past-delay re-arm: every pending timer is strictly in the future
  for (const p of timers.pending()) assert.ok(p.at > state.t, `timer re-armed at/past now: ${p.at - state.t} ms`);
  // due-time history kept, original first (item is a live reference: sendAt
  // has moved to the backoff time by now, history[0] must still be the original)
  assert.equal(t1.sendAtHistory[0], originalSendAt, 'original sendAt kept in history');
  assert.notEqual(t1.sendAt, originalSendAt, 'sendAt moved off the past timestamp');
  await sched.dispose();
  await rm(dir, { recursive: true, force: true });
});

await okAsync('t1.2 backoff grows 60 s → 120 s by attempts 1→3; rate bounded (≤2 attempts in a 26 s window like INC-005)', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sl-t1b-'));
  const { clock, timers, state } = makeFakeTimers(1_000_000_000_000);
  const sched = await createScheduler({
    dataDir: dir,
    clock,
    timers,
    deliver: async () => { throw noLiveAgentErr(); },
  });
  await sched.schedule({ content: 't1', sendAt: state.t + 100, conversationId: 'dead-chat' });
  await timers.advance(100); // attempt 1 @ t+100
  let item = sched.list()[0];
  const gap1 = item.sendAt - state.t;
  assert.equal(item.attempts, 1);
  assert.ok(gap1 >= 60_000, `gap1 ${gap1}`);
  await timers.advance(gap1 + 1); // attempt 2
  item = sched.list()[0];
  assert.equal(item.attempts, 2);
  await timers.advance(item.sendAt - state.t + 1); // attempt 3
  item = sched.list()[0];
  assert.equal(item.attempts, 3);
  const gap3 = item.sendAt - state.t;
  assert.ok(gap3 > gap1, `backoff must grow: attempts1→${gap1} ms, attempts3→${gap3} ms (expect 120 s)`);
  // INC-005 window: 0.6.0 hit 63109 attempts in 26 s (~2.4k/s) after the first
  // failure. Here, from attempt 1 to attempt 3 the clock had to advance
  // ~120 s → retry rate ≈ 1 per minute, not per millisecond.
  const firstFailureAt = (1_000_000_000_000) + 100;
  const elapsed = state.t - firstFailureAt;
  const extraAttempts = sched.list()[0].attempts - 1; // attempts after the first
  assert.ok(extraAttempts > 0);
  assert.ok(elapsed / extraAttempts >= 60_000, `retry gap ${elapsed / extraAttempts} ms must be ≥ 60 s (was ~0.4 ms in 0.6.0)`);
  for (const p of timers.pending()) assert.ok(p.at > state.t, 'no past-timestamp timer armed');
  await sched.dispose();
  await rm(dir, { recursive: true, force: true });
});

await okAsync('t1.3 delivery success removes the item and resets the attempts counter', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sl-t1c-'));
  const { clock, timers, state } = makeFakeTimers(1_000_000_000_000);
  let deliverMode = 'fail';
  const sched = await createScheduler({
    dataDir: dir,
    clock,
    timers,
    deliver: async (item) => {
      if (deliverMode === 'fail') throw noLiveAgentErr();
    },
  });
  const item = await sched.schedule({ content: 't1', sendAt: state.t + 100, conversationId: 'chat' });
  await timers.advance(100); // attempt 1 (fails)
  assert.equal(sched.list()[0].attempts, 1);
  deliverMode = 'ok';
  await timers.advance(sched.list()[0].sendAt - state.t + 1); // attempt 2 (succeeds)
  assert.equal(sched.list().length, 0, 'item removed after settled success');
  assert.equal(item.attempts, 0, 'attempts counter reset on success');
  await sched.dispose();
  await rm(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
console.log('t2: erasure immunity (locked read-modify-write, merge by id)');
const t2dir = await mkdtemp(join(tmpdir(), 'sl-t2-'));
const realNow = Date.now();
const t2Clock = { now: () => Date.now() };
const t2Sched = await createScheduler({ dataDir: t2dir, clock: t2Clock, deliver: async () => {} });
await t2Sched.schedule({ content: 'item A', sendAt: realNow + 3_600_000, conversationId: 'a' });
const idA = t2Sched.list()[0].id;

// second process context: its own createScheduler on the SAME dataDir adds B
const childScript = `
import { createScheduler } from ${JSON.stringify(join(HERE, '..', 'src', 'scheduler.js'))};
const dir = process.argv[1];
const s = await createScheduler({ dataDir: dir, deliver: async () => {} });
await s.schedule({ content: 'item B (second process)', sendAt: Date.now() + 3_600_000, conversationId: 'b' });
const ids = s.list().map((x) => x.id);
console.log(JSON.stringify(ids));
`;
const childOut = await new Promise((resolve, reject) => {
  execFile(process.execPath, ['--input-type=module', '-e', childScript, t2dir], (err, stdout) =>
    err ? reject(err) : resolve(stdout.trim()));
});
const childIds = JSON.parse(childOut);
assert.ok(childIds.some((i) => i !== idA), 'child scheduled its own item B');
const idB = childIds.find((i) => i !== idA);

await okAsync('t2.1 disk holds A (first instance) AND B (second process) after child persist', async () => {
  const items = readItems(t2dir);
  assert.ok(items.some((i) => i.id === idA), 'A present on disk after second-process persist');
  assert.ok(items.some((i) => i.id === idB), 'B present on disk');
});

await t2Sched.schedule({ content: 'item C', sendAt: realNow + 7_200_000, conversationId: 'c' });

await okAsync('t2.2 first instance persisting C does NOT erase A or B (no truncate of unseen items)', async () => {
  const items = readItems(t2dir);
  for (const id of [idA, idB]) assert.ok(items.some((i) => i.id === id), `${id} must survive`);
  assert.ok(items.some((i) => i.content === 'item C'), 'C present');
});

await okAsync('t2.3 cancelling A removes A but keeps the never-seen B', async () => {
  const removed = await t2Sched.cancel(idA);
  assert.ok(removed, 'cancel returns true');
  const items = readItems(t2dir);
  assert.ok(!items.some((i) => i.id === idA), 'A intentionally dropped');
  assert.ok(items.some((i) => i.id === idB), 'B (unseen by first instance) NOT resurrected-or-erased — still on disk');
});

await okAsync('t2.4 advisory lock is released after persist (no stale lock left)', async () => {
  await t2Sched.schedule({ content: 'item D', sendAt: realNow + 3_600_000, conversationId: 'd' });
  let present = true;
  try { await readFile(join(t2dir, 'tasks.json.lock')); } catch { present = false; }
  assert.equal(present, false, 'lock file absent after persist cycle');
});
await t2Sched.dispose();
await rm(t2dir, { recursive: true, force: true });

// ---------------------------------------------------------------------------
console.log('t3: settle-before-remove (in-flight flag, at-least-once on crash)');
await okAsync('t3.1 pending deliver → item still present with status delivering (memory + disk flag)', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sl-t3-'));
  const { clock, timers, state } = makeFakeTimers(1_000_000_000_000);
  let resolveDeliver;
  const gate = new Promise((r) => { resolveDeliver = r; });
  const sched = await createScheduler({
    dataDir: dir,
    clock,
    timers,
    deliver: async () => { await gate; }, // never settles until we say so
  });
  const item = await sched.schedule({ content: 't3', sendAt: state.t, conversationId: 'chat' });
  const tickPromise = sched.tick(); // starts fireDue; awaits deliver
  await new Promise((r) => setTimeout(r, 50)); // let it reach the deliver await
  const live = sched.list().find((x) => x.id === item.id);
  assert.ok(live, 'item STILL in queue while delivery pending (0.6.0 removed it before await)');
  assert.equal(live.status, 'delivering', 'in-flight flag set');
  assert.ok(live.startedAt, 'startedAt recorded');
  const onDisk = readItems(dir).find((x) => x.id === item.id);
  assert.equal(onDisk.status, 'delivering', 'in-flight flag persisted BEFORE the await');
  assert.ok(onDisk.startedAt, 'startedAt persisted');
  // resolve → settled success → removed
  resolveDeliver();
  await tickPromise;
  assert.equal(sched.list().length, 0, 'item removed AFTER delivery settles');
  assert.deepEqual(readItems(dir), [], 'disk empty after settled success');
  await sched.dispose();
  await rm(dir, { recursive: true, force: true });
});

await okAsync('t3.2 failed deliver (settled) keeps the item re-queued with backoff (never removed on failure)', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sl-t3b-'));
  const { clock, timers, state } = makeFakeTimers(1_000_000_000_000);
  const sched = await createScheduler({
    dataDir: dir,
    clock,
    timers,
    deliver: async () => { throw new Error('agent busy'); },
  });
  const item = await sched.schedule({ content: 't3', sendAt: state.t, conversationId: 'chat' });
  await sched.tick();
  const live = sched.list().find((x) => x.id === item.id);
  assert.ok(live, 'item still queued after settled failure');
  assert.equal(live.status, 'pending');
  assert.ok(live.sendAt - state.t >= 60_000, 're-armed in the future');
  await sched.dispose();
  await rm(dir, { recursive: true, force: true });
});

await okAsync('t3.3 crash-on-disk (status delivering) restore re-queues as pending (at-least-once)', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sl-t3c-'));
  await mkdir(dir, { recursive: true });
  const crashed = {
    items: [{
      id: 'task-1-999',
      content: 'crashed mid-delivery',
      sendAt: 1_000_000_000_000,
      conversationId: 'chat',
      model: null,
      meta: null,
      createdAt: 1_000_000_000_000,
      status: 'delivering',
      startedAt: 1_000_000_000_000,
      attempts: 1,
    }],
  };
  await writeFile(join(dir, 'tasks.json'), JSON.stringify(crashed, null, 2) + '\n', 'utf8');
  const { clock, timers } = makeFakeTimers(1_000_000_000_001);
  let delivered = false;
  const sched = await createScheduler({
    dataDir: dir,
    clock,
    timers,
    deliver: async () => { delivered = true; },
  });
  const live = sched.list().find((x) => x.id === 'task-1-999');
  assert.ok(live, 'crashed item restored into the live queue');
  assert.equal(live.status, 'pending', "restored to 'pending' (0.6.0 had no such guarantee)");
  // it is due (sendAt in the past) → fires on next tick; duplicate delivery accepted
  await sched.tick();
  assert.ok(delivered, 're-delivered after restore (at-least-once)');
  await sched.dispose();
  await rm(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
console.log('t4: generated client bundle registration id (INC-2026-10-18)');
await okAsync('t4.1 lib/client.js has exactly one __ModuleLoader__.load( registering the package.json name', async () => {
  const pkgName = JSON.parse(readFileSync(join(HERE, '..', 'package.json'), 'utf8')).name;
  checkRegistrationIdMatchesPackage(join(HERE, '..', 'lib', 'client.js'), pkgName);
});

// ---------------------------------------------------------------------------
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
console.log('REGRESSION SUITE: ALL GREEN');
