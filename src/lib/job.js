import { ulid } from 'ulid';
import { config, DEFAULT_PRIORITY, PRIORITIES } from '../config.js';
import {
  jobKey, readyKey, scheduledKey, leasesKey, deadKey, workerPrefix,
  metricsKey, succeededZset,
} from './keys.js';

export const STATES = {
  SCHEDULED: 'scheduled',
  DEAD:      'dead',
  READY:     'ready',
  LEASED:    'leased',
  SUCCEEDED: 'succeeded',
  FAILED:    'failed',
};

// Redis hashes hold strings only, so every field is serialised on the way
// in and parsed on the way out. Doing it in one place keeps the rest of
// the codebase working with real types.
function serialise(job) {
  return {
    id:           job.id,
    type:         job.type,
    payload:      JSON.stringify(job.payload ?? {}),
    queue:        job.queue,
    priority:     String(job.priority),
    state:        job.state,
    attempts:     String(job.attempts),
    max_attempts: String(job.max_attempts),
    run_at:       String(job.run_at),
    created_at:   String(job.created_at),
    updated_at:   String(job.updated_at),
    ...(job.cron_name ? { cron_name: job.cron_name } : {}),
  };
}

function deserialise(hash) {
  if (!hash || !hash.id) return null;
  return {
    id:           hash.id,
    type:         hash.type,
    payload:      JSON.parse(hash.payload || '{}'),
    queue:        hash.queue,
    priority:     Number(hash.priority),
    state:        hash.state,
    attempts:     Number(hash.attempts),
    max_attempts: Number(hash.max_attempts),
    run_at:       Number(hash.run_at),
    created_at:   Number(hash.created_at),
    updated_at:   Number(hash.updated_at),
    worker_id:    hash.worker_id || null,
    lease_expires_at: hash.lease_expires_at ? Number(hash.lease_expires_at) : null,
    reclaims:     Number(hash.reclaims || 0),
    cron_name:    hash.cron_name || null,
    run_ms:       hash.run_ms ? Number(hash.run_ms) : null,
    wait_ms:      hash.wait_ms ? Number(hash.wait_ms) : null,
    last_error:   hash.last_error || null,
    result:       hash.result ? JSON.parse(hash.result) : null,
  };
}

// A job may be scheduled either by absolute time (run_at) or by an offset
// from now (delay_ms). Absolute wins if both are given.
export function buildJob({ type, payload, queue, priority, max_attempts, run_at, delay_ms, cron_name }) {
  const now = Date.now();

  let runAt = now;
  if (run_at !== undefined) runAt = Number(run_at);
  else if (delay_ms !== undefined) runAt = now + Number(delay_ms);

  const delayed = runAt > now;

  return {
    id:           ulid(),
    type,
    payload:      payload ?? {},
    queue:        queue || config.defaultQueue,
    priority:     priority ?? DEFAULT_PRIORITY,
    state:        delayed ? STATES.SCHEDULED : STATES.READY,
    attempts:     0,
    max_attempts: max_attempts ?? 3,
    run_at:       runAt,
    created_at:   now,
    updated_at:   now,
    cron_name:    cron_name ?? null,
  };
}

// Two destinations, one decision point. A job due now goes straight onto a
// ready list; a job due later waits in the scheduled ZSET until the promoter
// moves it across. Retries and cron instances reuse this same path, which is
// why delayed execution is worth building before either of them.
//
// Order matters: the hash is written before the id becomes visible, or a
// worker could pop an id pointing at a record that does not exist yet.
export async function enqueue(redis, job) {
  const tx = redis.multi().hset(jobKey(job.id), serialise(job));

  if (job.state === STATES.SCHEDULED) {
    tx.zadd(scheduledKey(job.queue), job.run_at, job.id);
  } else {
    tx.rpush(readyKey(job.queue, job.priority), job.id);
  }

  tx.hincrby(metricsKey(), 'enqueued', 1);
  tx.hincrby(metricsKey(), `enqueued:${job.type}`, 1);

  await tx.exec();
  return job;
}

export async function getJob(redis, id) {
  return deserialise(await redis.hgetall(jobKey(id)));
}

export async function setState(redis, id, state, extra = {}) {
  await redis.hset(jobKey(id), { state, updated_at: String(Date.now()), ...extra });
}

// Count live workers by scanning their presence keys. SCAN is used rather
// than KEYS because KEYS blocks Redis for the whole walk.
export async function countWorkers(redis) {
  let cursor = '0';
  const found = new Set();
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', `${workerPrefix()}*`, 'COUNT', 100);
    keys.forEach((k) => found.add(k));
    cursor = next;
  } while (cursor !== '0');
  return found.size;
}

// Queue depth across every structure, for GET /stats.
export async function queueStats(redis, queue) {
  const pipeline = redis.pipeline();
  PRIORITIES.forEach((p) => pipeline.llen(readyKey(queue, p)));
  pipeline.zcard(scheduledKey(queue));
  pipeline.zcard(leasesKey(queue));
  pipeline.llen(deadKey(queue));
  const results = await pipeline.exec();

  const values = results.map(([err, val]) => (err ? 0 : val));
  const workers = await countWorkers(redis);
  return {
    workers,
    queue,
    ready:     Object.fromEntries(PRIORITIES.map((p, i) => [`priority_${p}`, values[i]])),
    scheduled: values[PRIORITIES.length],
    leased:    values[PRIORITIES.length + 1],
    dead:      values[PRIORITIES.length + 2],
  };
}

// Read the counters back and derive the averages. Stored as sums and counts
// rather than averages, because you cannot meaningfully average an average.
export async function readMetrics(redis) {
  const raw = await redis.hgetall(metricsKey());
  const n = (k) => Number(raw[k] || 0);

  const types = new Set();
  for (const k of Object.keys(raw)) {
    const i = k.indexOf(':');
    if (i > 0) types.add(k.slice(i + 1));
  }

  const byType = {};
  for (const t of types) {
    const runs = n(`run_count:${t}`);
    byType[t] = {
      enqueued:  n(`enqueued:${t}`),
      succeeded: n(`succeeded:${t}`),
      avg_run_ms:  runs ? Math.round(n(`run_ms_sum:${t}`) / runs) : null,
      max_run_ms:  n(`run_ms_max:${t}`) || null,
      avg_wait_ms: runs ? Math.round(n(`wait_ms_sum:${t}`) / runs) : null,
    };
  }

  return {
    totals: {
      enqueued:        n('enqueued'),
      succeeded:       n('succeeded'),
      failed_attempts: n('failed_attempts'),
      dead:            n('dead'),
      reclaimed:       n('reclaimed'),
      cron_fired:      n('cron_fired'),
    },
    by_type: byType,
  };
}

// Most recently completed jobs, newest first.
export async function recentJobs(redis, queue, limit = 20) {
  const ids = await redis.zrevrange(succeededZset(queue), 0, limit - 1);
  const jobs = await Promise.all(ids.map((id) => getJob(redis, id)));
  return jobs.filter(Boolean).map((j) => ({
    id: j.id, type: j.type, state: j.state, cron_name: j.cron_name,
    attempts: j.attempts, updated_at: j.updated_at,
  }));
}
