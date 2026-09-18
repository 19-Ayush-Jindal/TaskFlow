# TaskFlow

A fault-tolerant distributed job scheduler and worker queue. Built on Node.js,
Express and Redis.

This is a queue that assumes workers will crash, and makes sure no work is
lost when they do.

## Status

All eight steps are complete. A job submitted over HTTP is persisted in
Redis and executed by one of N interchangeable worker processes, with strict
priority ordering, delayed and recurring schedules, at-least-once delivery
under worker crashes, capped retries with exponential backoff, a
dead-letter queue, and a live dashboard.

| Step | Feature | Done |
|------|---------|------|
| 1 | API -> ready list -> worker executes | yes |
| 2 | Job records and state transitions | yes |
| 3 | Delayed jobs (`run_at`) and the promoter loop | yes |
| 4 | Leases, heartbeating, and the reaper | yes |
| 5 | Attempts, exponential backoff, dead-letter queue | yes |
| 6 | Priorities exposed end to end | yes |
| 7 | Recurring cron jobs | yes |
| 8 | Metrics, introspection, retention, dashboard | yes |

## Running it

    cp .env.example .env
    docker compose up --build --scale worker=3

Or locally, with Redis already running:

    npm install
    npm run api      # terminal 1
    npm run worker   # terminal 2

## Trying it

Submit a job:

    curl -X POST localhost:3000/jobs \
      -H 'content-type: application/json' \
      -d '{"type":"demo.echo","payload":{"hello":"world"}}'

Inspect it:

    curl localhost:3000/jobs/<id>

Watch priority ordering work - submit a low-priority job while the workers
are busy, then a high-priority one, and the high-priority job runs first:

    curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
      -d '{"type":"demo.slow","payload":{"ms":5000},"priority":2}'
    curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
      -d '{"type":"demo.echo","payload":{"urgent":true},"priority":0}'

## Data model

| Purpose | Structure | Key |
|---|---|---|
| Job record | HASH | `taskflow:job:{id}` |
| Delayed, retrying and cron jobs | ZSET scored by `run_at` | `taskflow:scheduled:{queue}` |
| Runnable now | LIST per priority | `taskflow:ready:{queue}:{0,1,2}` |
| In-flight leases | ZSET scored by expiry | `taskflow:leases:{queue}` |
| Permanently failed | LIST | `taskflow:dead:{queue}` |

Only the first and third exist so far.

Two ideas hold the design together:

1. **Delayed, retrying and recurring jobs are the same thing** - a member of
   the scheduled ZSET with a future timestamp. One mechanism, three features.
2. **A lease expiring is the crash detection.** There is no separate health
   check. If a worker stops renewing its lease, the reaper reclaims the job.

## Recurring jobs

A cron definition is a template plus a schedule, not a new execution path.
When it fires, the beat enqueues an ordinary job - so priorities, leases,
retries and the dead-letter queue all apply to it unchanged.

    curl -X POST localhost:3000/crons -H 'content-type: application/json' \
      -d '{"name":"nightly-report","expression":"0 2 * * *",
           "type":"demo.echo","timezone":"Asia/Kolkata"}'

`GET /crons`, `GET /crons/:name`, `DELETE /crons/:name`, and
`PATCH /crons/:name {"enabled":false}` to pause without deleting.

Two details that matter:

**The schedule advances from the slot that fired, not from now.** A job due
at 09:00 that fires at 09:02 sets the next one to 10:00, not 10:02.
Computing from `now` lets a late beat silently drift the schedule.

**Several schedulers can beat at once.** Claiming an occurrence is a ZREM
on the schedule ZSET, so exactly one wins it. If a scheduler dies between
claiming and rescheduling, that definition drops out of the schedule - the
roster set (`cron-names`) is how it gets noticed, and the orphan sweep puts
it back. The semantics are therefore: never double-fire, but an occurrence
can be skipped if a scheduler dies mid-claim.

## Observability

Open `http://localhost:3000` for a live dashboard: worker count, queue
depth by priority, in-flight jobs, per-type latency, recurring definitions,
recently completed jobs and the dead-letter queue. Refreshes every second.

| Endpoint | Purpose |
|---|---|
| `GET /` | live dashboard |
| `GET /health` | 503 when jobs are queued and no workers exist |
| `GET /stats` | queue depth and live worker count |
| `GET /metrics` | counters plus mean and max latency per job type |
| `GET /metrics/prometheus` | same data in exposition format |
| `GET /jobs?limit=n` | recently completed jobs |
| `GET /dead?limit=n` | dead-letter queue with last errors |

Counters are incremented inside the same Lua scripts that change job state,
so a metric can never disagree with the transition it describes, and none
are lost to a crash between two round trips.

Two latency numbers are tracked per type, and they answer different
questions. **Run time** describes the handler - it changes when the code or
its dependencies change. **Wait time** is how long a job sat between
becoming runnable and being picked up; it is the number that tells you to
add workers.

## Retention

Succeeded job records are deleted after `RETENTION_MS` (default 24h) by a
sweeper in the maintenance loop. Without it Redis grows without bound,
since every job ever run stays as a hash.

Dead jobs are deliberately never swept. They are actionable, and discarding
them would defeat the point of a dead-letter queue.

## Known limitations

These are deliberate, and worth being able to defend.

- **Pickup latency is ~25ms, not ~1ms.** Leasing is a Lua script and Lua
  cannot block, so workers poll instead of parking on `BLPOP`. Redis has no
  command that atomically moves from several priority-ordered lists into
  another structure, so blocking, priority and atomic claiming cannot all
  be had at once. Atomic claiming was the one worth keeping.
- **Delayed jobs are late by up to one tick** (200ms). Precision is bounded
  by the polling interval.
- **A cron occurrence can be skipped** if a scheduler dies between claiming
  it and writing the next fire time. Never double-fired, occasionally
  skipped.
- **Single Redis instance.** No cluster support: the Lua scripts build some
  keys from prefixes rather than declaring them in `KEYS`, which Redis
  Cluster forbids. Durability is bounded by `appendfsync everysec`, so a
  hard crash can lose about a second of writes.
- **Metrics are counters since the last `FLUSHALL`**, not a time series.
  Rates and percentiles are the scraper's job.

## Retries

A failed attempt has two possible causes and one response. A handler that
threw, and a worker that died holding the lease, are both just "attempt N
did not succeed" - only the detection differs. Both route through the same
decision:

    attempts++
    if attempts < max_attempts:  re-schedule at now + backoff
    else:                        push to the dead-letter queue

Backoff is `base * 2^(attempts-1)`, capped, then jittered so a batch of
jobs that failed together does not retry in lockstep. Defaults: 2s base,
5min cap, 3 attempts.

A job in the dead-letter queue is inert - it consumes no workers and will
never run again on its own. `GET /dead` lists them with their last error;
`POST /jobs/:id/replay` resets attempts and puts one back on the queue.

## Delivery guarantee

At-least-once. A job can run more than once - for example when a worker
completes the work and then dies before recording the result. **Handlers must
be idempotent.** Exactly-once delivery is not offered, because across a
network it is not achievable; exactly-once *effects* are the caller's job.
