// Scheduled sending: persistent task queue in <dataDir>/tasks.json.
// Each task carries {content, sendAt, conversationId, model?} and fires via
// the deliver callback at sendAt. The queue survives restarts (restored +
// re-armed; overdue tasks are re-delivered to their ORIGINAL conversation).
// Clock and timers are injectable for tests.
//
// FORK (INC-005, 0.6.0-fork.1) — behavior changes vs 0.6.0:
//   (a) NO_LIVE_AGENT no longer keeps the ORIGINAL (past) due time: 0.6.0
//       re-armed setTimeout(0) on a past timestamp in an unbounded hot loop
//       (63k attempts in 26 s observed live, INC-005 timeline). It now backs
//       off exponentially — nextAttempt = min(now + 60s * 2^floor(attempts/3),
//       now + 15min) — and the item's sendAt ALWAYS lands in the future
//       (never re-armed on a past timestamp). The original due time is kept
//       in item.sendAtHistory. The attempts counter resets on delivery
//       success. After MAX_ATTEMPTS (1000) the task stops retrying: status
//       'failed' + lastError, and the record stays in the queue/file for the
//       UI (cancellable, reschedulable).
//   (b) settle-before-remove: 0.6.0 removed the item from the queue BEFORE
//       `await deliver` and re-queued only on throw — a deliver that never
//       settled dropped the task from the live queue, and the next persist
//       (any later schedule/cancel) then erased it from disk too (silent
//       user-message loss, INC-005 H5/H6). The item now stays in the queue
//       while delivery is in flight, flagged status 'delivering' + startedAt
//       and persisted BEFORE the await. Only a SETTLED delivery changes the
//       queue: success → removed; terminal failure → kept as 'failed'; user
//       cancel → removed via cancel(). A crash mid-delivery leaves the flag
//       on disk; restore flips it back to 'pending', so delivery is
//       at-least-once (a duplicate user bubble after a crash is acceptable;
//       a silently lost task is not).
//   (c) locked, merge-only persist: 0.6.0 wrote the file from memory alone
//       (truncate + overwrite), so any disk item memory had lost was erased.
//       persist() now does a read-modify-write under an advisory lock file
//       (pure node:fs, O_EXCL create + retry, stale-lock breaking; no native
//       dependencies) and MERGES by id: it keeps EVERY disk item except the
//       ids this instance intentionally removed (delivered / cancelled —
//       tracked in removedIds), then overwrites with the in-memory queue. So
//       items written by another process, or present on disk but lost from
//       memory, survive a persist; intentionally-dropped items do not
//       resurrect. Never truncates unseen items.

import { fs } from './deps.js';

// FORK (a): terminal retry cap — stop after this many attempts, mark 'failed'.
const MAX_ATTEMPTS = 1000;
// FORK (a): NO_LIVE_AGENT backoff — 60 s base, doubling every 3 attempts,
// 15 min cap: delay = min(60_000 * 2 ** floor(attempts / 3), 900_000).
const NO_LIVE_BASE_MS = 60_000;
const NO_LIVE_CAP_MS = 15 * 60_000;
// Pre-fork backoff for NON-NO_LIVE_AGENT errors (kept as in 0.6.0):
const OTHER_BASE_MS = 60_000;
const OTHER_CAP_MS = 300_000;
// FORK (c): advisory lock is considered stale after this (crashed holder).
const LOCK_STALE_MS = 5_000;
const LOCK_RETRIES = 40;
const LOCK_RETRY_MS = 25;

function queuePath(dataDir) {
  return `${dataDir}/tasks.json`;
}

// FORK (c): advisory lock file next to the queue (no native deps).
function lockPath(dataDir) {
  return `${dataDir}/tasks.json.lock`;
}

// FORK (c): acquire the advisory lock — O_EXCL create; retry while the
// current lock is fresh; break it when stale (older than LOCK_STALE_MS, e.g.
// crashed holder) or unreadable.
async function acquireLock(path) {
  for (let i = 0; i < LOCK_RETRIES; i++) {
    try {
      await fs.writeFile(path, JSON.stringify({ pid: process.pid, at: Date.now() }), { flag: 'wx' });
      return;
    } catch (err) {
      if (!err || err.code !== 'EEXIST') throw err;
      let stale = false;
      try {
        const info = JSON.parse(await fs.readFile(path, 'utf8'));
        stale = Date.now() - (Number(info.at) || 0) > LOCK_STALE_MS;
      } catch {
        stale = true; // unreadable/corrupt lock → treat as stale
      }
      if (stale) {
        await fs.unlink(path).catch(() => {}); // break the stale lock
        continue; // retry the create immediately
      }
      await new Promise((r) => setTimeout(r, LOCK_RETRY_MS));
    }
  }
  throw new Error('schedule-later: could not acquire tasks.json lock (concurrent writer?)');
}

// FORK (c): release the advisory lock (best-effort).
function releaseLock(path) {
  return fs.unlink(path).catch(() => {});
}

/**
 * Create the scheduler.
 * @param {object} opts
 * @param {string} opts.dataDir persistence directory
 * @param {{now:()=>number}} [opts.clock] injectable clock (default Date)
 * @param {{setTimeoutAt:(fn,atMs)=>id, clearTimeout:(id)=>void}} [opts.timers]
 *        injectable timer queue; defaults to global setTimeout/clearTimeout
 * @param {(item:object)=>Promise<void>} opts.deliver deliver one due task
 */
export async function createScheduler({ dataDir, clock = { now: () => Date.now() }, timers = defaultTimers(), deliver } = {}) {
  if (typeof deliver !== 'function') throw new Error('createScheduler: deliver callback required');
  await fs.mkdir(dataDir, { recursive: true });
  const path = queuePath(dataDir);
  const lpath = lockPath(dataDir); // FORK (c)

  // restore persisted queue (best-effort)
  let queue = [];
  try {
    const parsed = JSON.parse(await fs.readFile(path, 'utf8'));
    if (Array.isArray(parsed.items)) queue = parsed.items.filter((it) => it && it.id && typeof it.sendAt === 'number');
  } catch {
    /* missing/corrupt → start empty */
  }
  // FORK (b): a crash mid-delivery leaves status 'delivering' on disk. Flip it
  // back to 'pending' on restore — at-least-once delivery (a duplicate user
  // bubble after a crash is acceptable; a silently dropped task is not).
  for (const it of queue) {
    if (it.status === 'delivering') it.status = 'pending';
    if (!it.status) it.status = 'pending'; // normalize pre-fork items
  }
  // FORK (c): ids this instance INTENTIONALLY removed (delivered success,
  // cancelled). persist() keeps every disk item except these, so foreign
  // items (another process) and disk-only items survive, while dropped ones
  // do not resurrect. Items merely lost from memory (the 0.6.0 bug) are NOT
  // in removedIds, so they are kept.
  const removedIds = new Set();

  let seq = queue.reduce((m, it) => Math.max(m, Number(String(it.id).split('-')[1]) || 0), 0);
  let armed = null; // { timerId }

  const persist = async () => {
    // FORK (c): read-modify-write under the advisory lock; merge by id. Never
    // truncate/overwrite items this instance has not seen.
    await acquireLock(lpath);
    try {
      let disk = [];
      try {
        const parsed = JSON.parse(await fs.readFile(path, 'utf8'));
        if (Array.isArray(parsed.items)) disk = parsed.items.filter((it) => it && it.id);
      } catch {
        /* missing/corrupt file → only our items */
      }
      const byId = new Map();
      for (const it of disk) {
        if (removedIds.has(it.id)) continue; // intentionally dropped → do not resurrect
        byId.set(it.id, it); // keep everything else (foreign / disk-only)
      }
      for (const it of queue) byId.set(it.id, it); // in-memory queue is authoritative
      await fs.writeFile(path, JSON.stringify({ items: [...byId.values()] }, null, 2) + '\n', 'utf8');
    } finally {
      await releaseLock(lpath);
    }
  };

  // FORK (a)/(b): only pending items with a due time can arm/fire.
  const isFireable = (it) => it.status !== 'failed' && it.status !== 'delivering';
  const nextDue = () => queue.reduce((m, it) => (isFireable(it) && (m === null || it.sendAt < m) ? it.sendAt : m), null);

  // FORK (a): backoff delay (ms) after a failed attempt — always positive,
  // always bounded; the resulting sendAt is strictly in the future.
  const backoffDelay = (item, code) => {
    if (code === 'NO_LIVE_AGENT') {
      return Math.min(NO_LIVE_BASE_MS * 2 ** Math.floor(item.attempts / 3), NO_LIVE_CAP_MS);
    }
    return Math.min(OTHER_BASE_MS * 2 ** (item.attempts - 1), OTHER_CAP_MS); // pre-fork formula, other errors
  };

  const fireDue = async () => {
    const now = clock.now();
    // fire in sendAt order (ascending); 'failed'/'delivering' items never fire
    const due = queue.filter((it) => isFireable(it) && it.sendAt <= now).sort((a, b) => a.sendAt - b.sendAt);
    for (const item of due) {
      // FORK (b): settle-before-remove — mark in-flight and persist the flag
      // BEFORE awaiting delivery, so a crash here re-queues on restore
      // (at-least-once; duplicate delivery acceptable, comment above).
      item.status = 'delivering';
      item.startedAt = clock.now();
      item.sendAtHistory = item.sendAtHistory || [];
      await persist().catch(() => {});
      try {
        await deliver(item);
        // settled SUCCESS: only now does the item leave the queue.
        item.attempts = 0; // FORK (a): reset counter on delivery success
        queue = queue.filter((it) => it.id !== item.id);
        removedIds.add(item.id); // FORK (c): delivered → never resurrect from disk
      } catch (err) {
        // settled FAILURE: the item was never removed — update in place and
        // re-arm strictly in the future (or stop, see below).
        item.attempts = (item.attempts || 0) + 1;
        item.lastError = String(err && err.message);
        if (item.attempts >= MAX_ATTEMPTS) {
          // FORK (a): terminal — stop retrying; keep the record for the UI
          // (still visible via list()/state, cancellable, reschedulable).
          item.status = 'failed';
          item.lastErrorAt = clock.now();
        } else {
          item.status = 'pending';
          // FORK (a): never re-arm on a past timestamp. 0.6.0 kept the
          // NO_LIVE_AGENT original (past) due time → arm() computed
          // setTimeout(0) → unbounded hot loop. Backoff instead; the original
          // due time stays in sendAtHistory.
          const nextAt = clock.now() + backoffDelay(item, err && err.code);
          item.sendAtHistory.push(nextAt);
          item.sendAt = nextAt;
        }
      }
    }
    if (due.length) await persist().catch(() => {});
    arm();
  };

  const arm = () => {
    if (armed) {
      timers.clearTimeout(armed.timerId);
      armed = null;
    }
    const due = nextDue();
    if (due === null) return;
    // A past due time (e.g. restored after downtime) fires once now — catch-up
    // delivery. Retry backoffs always land in the future (FORK (a)), so this
    // can no longer degenerate into a zero-delay re-arm loop.
    const delay = Math.max(0, due - clock.now());
    const timerId = timers.setTimeoutAt(() => {
      armed = null;
      return fireDue(); // return the promise so injectable timer queues can await
    }, clock.now() + delay);
    armed = { timerId };
  };

  arm();
  await persist().catch(() => {}); // ensure the file exists after first create

  return {
    /** Schedule a task. sendAt (epoch ms) is required; default is computed by the caller. */
    async schedule({ content, sendAt, conversationId, model, meta }) {
      if (typeof content !== 'string' || !content.trim()) throw new Error('scheduled content must not be empty');
      if (typeof sendAt !== 'number' || !Number.isFinite(sendAt)) throw new Error('sendAt must be a timestamp (epoch ms)');
      const item = {
        id: `task-${++seq}-${sendAt}`,
        content,
        sendAt,
        conversationId: conversationId || null,
        model: model || null,
        meta: meta || null,
        createdAt: clock.now(),
        status: 'pending', // FORK (b): explicit status ('delivering'/'failed' set later)
        attempts: 0,
        sendAtHistory: [sendAt], // FORK (a): keep the due-time history
      };
      queue.push(item);
      await persist();
      arm();
      return item;
    },
    /** Cancel a pending task before it fires. Returns true when removed. */
    async cancel(id) {
      const before = queue.length;
      queue = queue.filter((it) => it.id !== id);
      if (queue.length !== before) {
        // FORK (c): a cancelled task is intentionally dropped — track the id
        // (the filter above removed exactly this id) so the merge never
        // resurrects it from disk.
        removedIds.add(id);
        await persist();
        arm();
        return true;
      }
      return false;
    },
    /**
     * Move a pending task to a new time (the id and content stay). A time at
     * or before now delivers it on the next tick — that is "Send now".
     * Returns the updated task, or null when no such task is pending.
     */
    async reschedule(id, sendAt) {
      if (typeof sendAt !== 'number' || !Number.isFinite(sendAt)) throw new Error('sendAt must be a timestamp (epoch ms)');
      const item = queue.find((it) => it.id === id);
      if (!item) return null;
      if (item.status === 'failed') {
        // FORK (a): rescheduling a terminal task revives it (explicit user act).
        item.status = 'pending';
        item.attempts = 0;
        delete item.lastError;
        delete item.lastErrorAt;
      }
      item.sendAt = sendAt;
      item.sendAtHistory = item.sendAtHistory || [];
      item.sendAtHistory.push(sendAt); // FORK (a)
      await persist();
      arm();
      return { ...item };
    },
    /** Pending tasks sorted by sendAt ascending (display order). */
    list() {
      return [...queue].sort((a, b) => a.sendAt - b.sendAt).map((it) => ({ ...it }));
    },
    /** Manual tick (for tests / external loops). */
    tick: fireDue,
    async dispose() {
      if (armed) timers.clearTimeout(armed.timerId);
      armed = null;
    },
  };
}

function defaultTimers() {
  return {
    // setTimeoutAt receives an ABSOLUTE epoch-ms target (same contract as the
    // injectable test timers); convert to a relative delay for real setTimeout.
    setTimeoutAt: (fn, atMs) => {
      const t = setTimeout(fn, Math.max(0, atMs - Date.now()));
      t.unref?.();
      return t;
    },
    clearTimeout: (t) => clearTimeout(t),
  };
}
