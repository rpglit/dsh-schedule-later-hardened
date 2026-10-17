# dsh-schedule-later-hardened

> **Provenance.** This repository is a **hardened fork** of
> [dsh-schedule-later](https://github.com/adithyanraj03/dsh-schedule-later)
> **0.6.0** by Adithya N Raj (`adithyanraj03`), MIT license (upstream npm
> publish 2026-09-24). The upstream repository has been dormant since
> 2026-09-24 (last push), and the changes below fix a live incident (INC-005)
> in the original scheduler, so the fork is maintained standalone. This
> repository is **not affiliated with, endorsed by, or maintained by** the
> upstream author. All screenshots are from the upstream repository.

![language](https://img.shields.io/badge/language-javascript-f7dc6f) ![style](https://img.shields.io/badge/style-vanilla-gray) ![license](https://img.shields.io/badge/license-MIT-blue) ![node](https://img.shields.io/badge/node-%E2%89%A5%2018-brightgreen) ![dsh](https://img.shields.io/badge/dsh-web%20profile-orange) ![runtime](https://img.shields.io/badge/runtime-offline_%C2%B7_100%25_local-brightgreen) ![tests](https://img.shields.io/badge/tests-passing-brightgreen)

A [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) plugin that schedules a message into a dsh chat for later — picked by you from a date and time picker in the message box, or by the model with its own tools.

**It runs on the host, not in the browser.** Schedule it, close the tab, go to sleep: the dsh process sends it on time as a **normal user bubble**, and the model replies to it exactly as if you had pressed Send.

## What this fork hardens

The upstream scheduler (`src/scheduler.js`, 160 lines in 0.6.0) had three
persistence defects, one of which caused a live incident (INC-005: an
unbounded hot loop of ~2,400 failed retries per second, and silent
user-message loss). The fork rewrites the scheduler (160 → 324 lines);
**every other file is byte-identical to 0.6.0**.

1. **NO_LIVE_AGENT exponential backoff** — 0.6.0 kept the *original (past)*
   due time when no live agent existed for the chat, so `arm()` computed
   `setTimeout(0)`: an unbounded hot loop (63,109 attempts in 26 s observed
   live). The fork computes `delay = min(60 s · 2^⌊attempts/3⌋, 15 min)` and
   always moves `sendAt` strictly into the future; `attempts` resets on
   delivery success; after 1,000 attempts the task stops retrying with
   `status: 'failed'` + `lastErrorAt` (the record stays in the queue for the
   UI — cancellable, reschedulable via `reschedule`, which revives failed
   tasks). Original due times are kept in `sendAtHistory`.
   → `src/scheduler.js:43-55` (limits), `:166-173` (backoff delay),
   `:189-212` (failure path, counter reset, terminal state), `:286-292` (revive).
2. **Settle-before-remove with a persisted `delivering` flag
   (at-least-once delivery)** — 0.6.0 removed the item from the queue
   *before* `await deliver` and re-queued only on throw; a deliver that never
   settled dropped the task from memory, and the next persist erased it from
   disk (silent user-message loss). The fork sets `status: 'delivering'` +
   `startedAt` and **persists the flag before the await**; the item leaves
   the queue only on settled success. A crash mid-delivery leaves the flag on
   disk; restore flips it back to `pending`, so delivery is at-least-once —
   a duplicate user bubble after a crash is acceptable, a silently lost
   message is not.
   → `src/scheduler.js:180-192` (mark + persist before await),
   `:121-127` (restore flips `delivering` → `pending`).
3. **Locked, merge-only `tasks.json` persistence with `removedIds`** — 0.6.0
   wrote the queue file from memory alone (truncate + overwrite), so any item
   memory had lost was erased from disk. The fork's `persist()` does a
   read-modify-write under an advisory lock file (`tasks.json.lock`;
   `O_EXCL` create + retry, 5 s stale-lock breaking, pure `node:fs`, no
   native dependencies) and **merges by id**: it keeps every disk item except
   the ids this instance intentionally removed (delivered / cancelled —
   tracked in `removedIds`). Items written by another process, or present on
   disk but lost from memory, survive a persist; intentionally-dropped items
   do not resurrect. It never truncates unseen items.
   → `src/scheduler.js:61-96` (advisory lock), `:138-160` (merge-only persist),
   `:128-133, :192, :270` (`removedIds`).

All three behaviors are pinned by the 10-case regression suite
([## Tests](#tests)).

## Install

Requires a working dsh with a `web` profile (DSH `>=0.1.0`, Node 18+).

```sh
# from this repository — the prebuilt lib/ is committed, no build step needed
dsh plugin --profile web add github:rpglit/dsh-schedule-later-hardened
dsh --profile web
```

or from a local checkout:

```sh
dsh plugin --profile web add /path/to/dsh-schedule-later-hardened
```

`dsh plugin` manages the profile's `package.json` dependency and reconciles
the `dsh.profile.bundles` list; the package's own `cordis.patch.yml` (mounted
via `dsh.bundle.patch`) inserts its loader row itself — the row's `name` is
the npm package name, so no manual roster edit is needed. Then **restart dsh**
if it was already running and **hard-refresh the browser**
(Ctrl+Shift+R / Cmd+Shift+R) — a plain refresh can serve a cached client
bundle. A **⏱️ Schedule** button appears in the message box row and a
**⏱️ Scheduled** entry at the bottom of the sidebar.

## Migrating from `dsh-schedule-later` (upstream or local fork)

This fork is published under its own npm name, `dsh-schedule-later-hardened`,
because the registry name `dsh-schedule-later` belongs to the upstream
author. The DSH harness resolves plugins **by npm package name** — the loader
row's `name` must be the exact package name, and the profile's `package.json`
dependency key plus the `dsh.profile.bundles` entry carry the same identity —
so the swap is:

```sh
dsh plugin --profile web remove dsh-schedule-later
dsh plugin --profile web add github:rpglit/dsh-schedule-later-hardened
dsh --profile web
```

and the profile's `package.json` should show the dependency key
`dsh-schedule-later-hardened` (and the same name in
`dsh.profile.bundles`) in place of the old one.

What does **not** change:

- **Pending messages survive.** The queue directory is hard-coded to
  `~/.dsh/dsh-schedule-later/` (`src/index.js:33`), independent of the
  package name.
- **Plugin identity is stable.** `dsh.plugin.json` keeps
  `id: "dsh-schedule-later"` — the harness does not read that file at all
  (it is a descriptive label); the cordis plugin entry keeps
  `name = 'dsh-schedule-later'` (`src/index.js:22`), and the model tools
  (`schedule_message`, `list_scheduled_messages`,
  `cancel_scheduled_message`), the `schedule-later` skill, the host routes
  and the UI keep their names and behaviour.
- Do **not** run the upstream package and this fork in one profile: they
  register the same tools, routes and UI entry.

### Coming from `dsh-send-later`?

`dsh-send-later` was the predecessor name of the upstream package (no longer
published). If you still have it, remove it first and copy the queue across
while dsh is stopped:

```sh
mkdir -p ~/.dsh/dsh-schedule-later
cp ~/.dsh/dsh-send-later/tasks.json ~/.dsh/dsh-schedule-later/tasks.json
```

## What you get

(Feature documentation below is from the upstream 0.6.0 README; screenshots
are upstream's.)

It does five things:

- **A ⏱️ Schedule button in the message box** — a day/night scheduler with a sky header that follows the chosen time, an analog clock, an AM/PM sun-and-moon pill and quick picks, on the same row as Send.
- **Host-side delivery** — timing and message injection both run inside the dsh host process, so the browser is only a view; it works with the tab closed.
- **A sidebar “⏱️ Scheduled” panel** — every pending message across all chats, grouped by chat, soonest first, tinted by the time it sends, where each message can be sent now, re-timed or cancelled.
- **Three tools for the model** — `schedule_message`, `list_scheduled_messages`, `cancel_scheduled_message` — so the assistant can come back to a chat on its own.
- **A `schedule-later` skill** — teaches the model when to schedule its own check-ins unprompted.

![The scheduler in motion: the sky follows the chosen time and the countdown ticks](https://raw.githubusercontent.com/adithyanraj03/dsh-schedule-later/main/assets/main.gif)

### The scheduler

Sits on the message box row, next to Send. Type as usual → click **⏱️ Schedule** → the scheduler opens (defaults to now + 5 minutes, any future time allowed) → **Confirm** turns the draft into a scheduled message and clears the box. **Nothing is sent immediately.** Multi-line text and Markdown source are kept exactly as typed.

The header is a sky that follows the chosen time — dawn, day, dusk, night — with the sun or moon moving along its arc, and an analog clock sweeping to the hour. The time carries an **AM/PM pill with a sun or a moon**, so 9:00 PM is never mistaken for 9:00 AM. Quick picks: +5 min, +15 min, +1 hour, Tonight 9 PM, Tomorrow 9 AM, and a live “Sends in …” underneath. On phones and narrow windows (≤480px) the picker folds into a near-full-width popover with touch targets at least 40px tall. Cancel any pending message from the in-chat list or the sidebar panel — both ask first.

### The sidebar panel

An entry at the bottom of the sidebar, with a badge counting every pending message, opens a panel listing pending messages across **all chats**, soonest first, each row showing the message in monospace (clamped to two lines), the send time and a countdown. Messages are grouped under their chat — click the chat's name to open it. Click a message for its details: the full text, who scheduled it and when, and Send now, Change time and Cancel. Pending messages for the current chat also appear in-chat, above dsh's To-dos.

![The sidebar panel with a live countdown](https://raw.githubusercontent.com/adithyanraj03/dsh-schedule-later/main/assets/scheduled.gif)

### The model tools

Three tools let the assistant schedule messages into the chat it is working in — to come back to something later without you having to remember:

| Tool | What it does |
|---|---|
| `schedule_message` | Schedule a message into **this chat**, by `delay_minutes` or an ISO 8601 `at` (no offset = this machine's local time). |
| `list_scheduled_messages` | List what is pending in this chat — yours and the assistant's — and the current local time. |
| `cancel_scheduled_message` | Cancel a message **the assistant** scheduled. |

Ask things like *“remind me at 6pm to push the release”* or *“check the CI run again in 20 minutes”*. Guard rails: this chat only; messages visibly marked `[Scheduled by the assistant]`; at least 1 minute ahead, at most 30 days out, at most 10 pending per chat; the assistant cannot cancel yours; everything it schedules shows up in the ⏱️ panel and the in-chat list.

### The skill

The plugin ships a `schedule-later` skill (`SKILL.md`), whose catalog entry the model sees every turn. It teaches when to come back unprompted — a build, deploy or job still running, a page or status that will change, something you said you would do later — how to write the message so its future self can act on it cold, and to tell you what it scheduled and when. Edit `SKILL.md` to change that behaviour.

## Tests

`test/regression.mjs` — 10 cases, pure node, no GUI, no network (all state
confined to `os.tmpdir()`), covering the three hardening behaviors:
`t1` NO_LIVE_AGENT backoff (3 cases), `t2` erasure immunity / locked
merge-only persistence (4 cases), `t3` settle-before-remove with the
persisted `delivering` flag (3 cases).

```sh
node test/regression.mjs
# …
# 10 passed, 0 failed
# REGRESSION SUITE: ALL GREEN
```

## Requirements

- A `web` dsh profile — `engines: dsh >= 0.1.0`
- Node 18+
- No runtime dependencies — the client bundle requires only `react`

## License

MIT — see the [LICENSE](LICENSE) file. The upstream copyright notice
(Copyright (c) 2026 Adithya N Raj) is retained unchanged; the hardened-fork
contributions carry an additional copyright line
(Copyright (c) 2026 rpglit, hardened fork). This repository is a third-party
community fork, **not affiliated with** the upstream author.
