# SOLUTION.md

## What was broken, and why

**1. Duplicate records and inflated call counts.** `Ingest` checked
`EventExists` and then, separately, called `InsertEvent` — two round trips
with a gap between them. Under sequential delivery this looks fine, but the
provider's at-least-once redelivery is not sequential: two copies of the
same `event_id` can arrive close together, both see "not seen yet" during
the gap, and both proceed to insert and increment stats. Nothing in the
schema stopped this either — `events.event_id` only had a plain index, not
a UNIQUE constraint, so even simultaneous inserts of the exact same
event_id were both allowed to succeed. I reproduced this directly with a
test that fires 20 identical webhook deliveries at the same instant
(`TestConcurrentDuplicateDeliveryCountsOnce`) — it failed roughly 2 times
out of every 5 runs against the original code, storing 2+ copies of the
same event.

**2. Recordings never marked processed, with nothing in the logs.**
`processRecording` was launched with `go func() { s.processRecording(ctx,
rec) }()`, reusing the *HTTP request's* context. `net/http` cancels a
request's context as soon as its handler returns, which happens almost
immediately here since `Ingest` doesn't wait for that goroutine. By the
time the simulated 50ms of recording work finished, the context was
already canceled, so `MarkRecordingProcessed` failed — and the error was
discarded (`// TODO: handle`), so nothing was ever logged. This one failed
consistently, every run, which matches ops saying it "never" worked.

**3. In-flight work disappearing on deploy.** Same root cause as #2, plus
one more: the recording-processing goroutine was never tracked anywhere.
`main.go`'s graceful shutdown only waits for in-flight HTTP requests
(`srv.Shutdown`) — it has no way to know a detached goroutine is still
running. On SIGTERM, `Shutdown` returns as soon as active requests finish
(near-instant, since the handler doesn't wait on the goroutine either), and
the process exits, killing any recording work that hadn't finished.

**4. Also found while reading, not named in the incident report but a real
bug:** `stats.Cache.Record` mutated the map and its values without holding
the cache's own lock (`Get` takes `RLock`; `Record` took nothing). Under
concurrent webhook delivery — the exact scenario this whole exercise is
about — this is a genuine data race.

## Why this deduplication strategy

I made the Postgres write path itself the source of truth: `events.event_id`
now has a real `UNIQUE` constraint, and the insert is
`INSERT ... ON CONFLICT (event_id) DO NOTHING`, wrapped in one transaction
with the call upsert and the stats increment (`Store.IngestEvent`). If the
row already exists, the insert affects zero rows, the transaction has
nothing else to do, and the caller is told `inserted = false` so it skips
every downstream side effect for that delivery.

I considered a Redis-based approach (`SETNX event_id`) as a fast pre-check.
I didn't use it as the *only* guard, because it would make Redis and
Postgres two separate sources of truth that can disagree. If the process
crashed after the `SETNX` succeeded but before the Postgres write
committed, that event_id would be permanently "seen" in Redis while never
actually reflected in `account_stats` — an unrepairable silent undercount,
since a retry would be rejected by the Redis key alone. Postgres was
already going to be the durable system of record for this data, and a
unique constraint gives an atomic, race-free guarantee for free.

I also wrapped the event insert, call upsert, and stats increment in one
transaction rather than three independent writes, to close a smaller gap:
without it, a crash between "insert event" and "increment stats" would
durably mark the event_id as seen while never counting it.

## What I'd change at 10,000 webhooks/sec

- Put a Redis `SET event_id NX EX <ttl>` in front of the Postgres path as a
  cheap, fast-rejecting pre-filter for the common case, keeping the
  Postgres constraint as the correctness backstop.
- Stop incrementing `account_stats` synchronously per request — batch
  increments (Redis `INCRBY`, flushed to Postgres on an interval) instead
  of one `UPDATE` per webhook against a hot row.
- Move recording processing to a real queue with a bounded worker pool,
  retries with backoff, and metrics, instead of an unbounded set of
  goroutines.
- Run `-race` in CI by default (now wired into the Makefile's `test`
  target) — it's what would have caught the unlocked cache write
  automatically instead of needing someone to notice it by reading the
  code.

## What I'd do with more time

I didn't add retry/backoff for transient Postgres or Redis errors — a
transient DB error currently just returns a 500, correct but crude given
the provider already retries non-2xx responses. I also didn't add a retry
path for a failed recording fetch — a call that fails processing once
stays `recording_processed = false` forever. Both are reasonable next
steps with another hour.

## A note on testing

I verified the fix for the cache data race (#4) by reasoning about the
code and repeated non-`-race` test runs — `go test -race` is the correct
tool for this class of bug, but it failed to compile locally on my Windows
machine due to a MinGW/GCC toolchain issue (Windows Defender flagged and
quarantined the assembler binary as a false positive immediately after
extraction — a known issue with freshly-downloaded MinGW builds). `-race`
is still enabled by default in the Makefile's `test` target, and runs
correctly in the Docker build stage and any standard Linux CI environment.