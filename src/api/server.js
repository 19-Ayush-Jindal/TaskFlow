import express from 'express';
import { config, PRIORITIES } from '../config.js';
import { createRedis } from '../lib/redis.js';
import { log } from '../lib/log.js';
import {
  buildJob, enqueue, getJob, queueStats, readMetrics, recentJobs, STATES,
} from '../lib/job.js';
import { DASHBOARD_HTML } from './dashboard.js';
import { registerScripts, replayJob } from '../lib/scripts.js';
import {
  putCron, getCron, listCrons, deleteCron, setCronEnabled, validateCron,
} from '../lib/cron.js';
import { deadKey } from '../lib/keys.js';
import { knownTypes, getHandler } from '../handlers/index.js';

const app = express();
app.use(express.json());

// Without this, a request missing the JSON content-type gets silently
// ignored by express.json(), req.body stays {}, and the caller sees a
// confusing "type is required" instead of the real problem.
app.use((req, res, next) => {
  // Only enforce this when a body was actually sent. Some POSTs are pure
  // commands with nothing to parse (POST /jobs/:id/replay), and demanding
  // a content-type for an empty body is just an obstacle.
  const hasBody = Number(req.get('content-length') || 0) > 0
    || req.get('transfer-encoding') !== undefined;
  if (hasBody && ['POST', 'PUT', 'PATCH'].includes(req.method) && !req.is('application/json')) {
    return res.status(415).json({
      error: 'content-type must be application/json',
      received: req.get('content-type') || '(none)',
    });
  }
  next();
});

const redis = registerScripts(createRedis('api'));

app.get('/health', async (_req, res) => {
  try {
    await redis.ping();
    const stats = await queueStats(redis, config.defaultQueue);
    const backlog = Object.values(stats.ready).reduce((a, b) => a + b, 0);

    // Jobs waiting with nobody to run them is the failure this endpoint
    // used to miss entirely: Redis up, API up, throughput zero.
    const stalled = stats.workers === 0 && backlog > 0;

    res.status(stalled ? 503 : 200).json({
      ok: !stalled,
      redis: 'up',
      workers: stats.workers,
      backlog,
      stalled,
      ...(stalled ? { reason: 'jobs are queued but no workers are running' } : {}),
    });
  } catch (err) {
    res.status(503).json({ ok: false, redis: 'down', error: err.message });
  }
});

app.get('/job-types', (_req, res) => res.json({ types: knownTypes() }));

// A live view of the queue. Much easier to watch than interleaved logs.
app.get('/', (_req, res) => {
  res.type('html').send(DASHBOARD_HTML);
});

app.get('/metrics', async (_req, res) => {
  res.json(await readMetrics(redis));
});

// Prometheus exposition format, so this can be scraped by anything standard
// rather than needing a bespoke collector.
app.get('/metrics/prometheus', async (_req, res) => {
  const [m, stats] = await Promise.all([
    readMetrics(redis), queueStats(redis, config.defaultQueue),
  ]);

  const lines = [];
  const emit = (name, help, type, value, labels = '') => {
    if (!lines.some((l) => l === `# TYPE ${name} ${type}`)) {
      lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
    }
    lines.push(`${name}${labels} ${value}`);
  };

  emit('taskflow_jobs_enqueued_total', 'Jobs accepted', 'counter', m.totals.enqueued);
  emit('taskflow_jobs_succeeded_total', 'Jobs completed successfully', 'counter', m.totals.succeeded);
  emit('taskflow_job_attempts_failed_total', 'Individual failed attempts', 'counter', m.totals.failed_attempts);
  emit('taskflow_jobs_dead_total', 'Jobs that exhausted their attempts', 'counter', m.totals.dead);
  emit('taskflow_jobs_reclaimed_total', 'Jobs reclaimed from expired leases', 'counter', m.totals.reclaimed);
  emit('taskflow_cron_fires_total', 'Recurring job occurrences fired', 'counter', m.totals.cron_fired);

  emit('taskflow_workers', 'Workers currently reporting presence', 'gauge', stats.workers);
  emit('taskflow_queue_depth', 'Jobs waiting to run', 'gauge', stats.scheduled, '{state="scheduled"}');
  emit('taskflow_queue_depth', 'Jobs waiting to run', 'gauge', stats.leased, '{state="leased"}');
  emit('taskflow_queue_depth', 'Jobs waiting to run', 'gauge', stats.dead, '{state="dead"}');
  for (const [p, n] of Object.entries(stats.ready)) {
    emit('taskflow_queue_depth', 'Jobs waiting to run', 'gauge', n,
      `{state="ready",priority="${p.replace('priority_', '')}"}`);
  }

  for (const [type, t] of Object.entries(m.by_type)) {
    if (t.avg_run_ms !== null) {
      emit('taskflow_job_run_ms_avg', 'Mean handler duration', 'gauge', t.avg_run_ms, `{type="${type}"}`);
    }
    if (t.avg_wait_ms !== null) {
      emit('taskflow_job_wait_ms_avg', 'Mean time spent queued', 'gauge', t.avg_wait_ms, `{type="${type}"}`);
    }
  }

  res.type('text/plain; version=0.0.4').send(lines.join('\n') + '\n');
});

// Recently completed jobs, newest first.
app.get('/jobs', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  res.json({ jobs: await recentJobs(redis, config.defaultQueue, limit) });
});

app.get('/stats', async (_req, res) => {
  res.json(await queueStats(redis, config.defaultQueue));
});

app.post('/jobs', async (req, res) => {
  const { type, payload, queue, priority, max_attempts, delay_ms, run_at } = req.body || {};

  if (!type || typeof type !== 'string') {
    return res.status(400).json({ error: 'type is required and must be a string' });
  }
  if (!getHandler(type)) {
    return res.status(400).json({ error: `unknown job type: ${type}`, known: knownTypes() });
  }
  if (priority !== undefined && !PRIORITIES.includes(priority)) {
    return res.status(400).json({ error: `priority must be one of ${PRIORITIES.join(', ')}` });
  }
  if (delay_ms !== undefined && (!Number.isFinite(delay_ms) || delay_ms < 0)) {
    return res.status(400).json({ error: 'delay_ms must be a non-negative number' });
  }
  if (run_at !== undefined && !Number.isFinite(Number(run_at))) {
    return res.status(400).json({ error: 'run_at must be a unix timestamp in milliseconds' });
  }

  try {
    const job = buildJob({ type, payload, queue, priority, max_attempts, delay_ms, run_at });
    await enqueue(redis, job);

    log.info('job.enqueued', {
      job_id: job.id, type: job.type, state: job.state, priority: job.priority,
      run_at: job.run_at,
    });

    // 202, not 200: the work has been accepted, not performed.
    res.status(202).json({
      id: job.id,
      state: job.state,
      queue: job.queue,
      priority: job.priority,
      run_at: job.run_at,
      runs_in_ms: Math.max(0, job.run_at - Date.now()),
    });
  } catch (err) {
    log.error('job.enqueue_failed', { error: err.message });
    res.status(500).json({ error: 'failed to enqueue job' });
  }
});

// Recurring job definitions. A cron is a template plus a schedule; when it
// fires it produces an ordinary job, so priorities, leases, retries and the
// DLQ all apply to it unchanged.
app.get('/crons', async (_req, res) => {
  res.json({ crons: await listCrons(redis) });
});

app.post('/crons', async (req, res) => {
  const input = req.body || {};
  const problem = validateCron(input);
  if (problem) return res.status(400).json({ error: problem });

  const def = await putCron(redis, input);
  log.info('cron.registered', {
    cron: def.name, expression: def.expression, type: def.type,
    timezone: def.timezone, next: new Date(def.next_run_at).toISOString(),
  });
  res.status(201).json(def);
});

app.get('/crons/:name', async (req, res) => {
  const def = await getCron(redis, req.params.name);
  if (!def) return res.status(404).json({ error: 'cron not found' });
  res.json(def);
});

app.delete('/crons/:name', async (req, res) => {
  const removed = await deleteCron(redis, req.params.name);
  if (!removed) return res.status(404).json({ error: 'cron not found' });
  log.info('cron.deleted', { cron: req.params.name });
  res.json({ deleted: req.params.name });
});

app.patch('/crons/:name', async (req, res) => {
  const { enabled } = req.body || {};
  if (typeof enabled !== 'boolean') {
    return res.status(400).json({ error: 'body must be { "enabled": true|false }' });
  }
  const def = await setCronEnabled(redis, req.params.name, enabled);
  if (!def) return res.status(404).json({ error: 'cron not found' });
  log.info('cron.toggled', { cron: def.name, enabled });
  res.json(def);
});

// The dead-letter queue: jobs that used up every attempt. Nothing will
// touch these again until a human decides to.
app.get('/dead', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  const ids = await redis.lrange(deadKey(config.defaultQueue), 0, limit - 1);
  const total = await redis.llen(deadKey(config.defaultQueue));
  const jobs = await Promise.all(ids.map((id) => getJob(redis, id)));
  res.json({
    total,
    showing: jobs.length,
    jobs: jobs.filter(Boolean).map((j) => ({
      id: j.id, type: j.type, attempts: j.attempts, reclaims: j.reclaims,
      last_error: j.last_error, payload: j.payload, updated_at: j.updated_at,
    })),
  });
});

// Fix the bug, then replay what it broke. Resets attempts to 0.
app.post('/jobs/:id/replay', async (req, res) => {
  const { ok, reason } = await replayJob(redis, config.defaultQueue, req.params.id);
  if (!ok) {
    const code = reason === 'no-such-job' ? 404 : 409;
    return res.status(code).json({ error: reason });
  }
  log.info('job.replayed', { job_id: req.params.id });
  res.status(202).json({ id: req.params.id, state: 'ready', attempts: 0 });
});

app.get('/jobs/:id', async (req, res) => {
  const job = await getJob(redis, req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found' });

  if (job.state === STATES.SCHEDULED) {
    job.runs_in_ms = Math.max(0, job.run_at - Date.now());
  }
  if (job.state === STATES.LEASED && job.lease_expires_at) {
    // Negative means the lease is already past due and the reaper has not
    // got to it yet - it will be reclaimed within one tick.
    job.lease_expires_in_ms = job.lease_expires_at - Date.now();
  }
  res.json(job);
});

// Express identifies error handlers by their four-argument signature, and
// only runs them if they are registered after the routes that throw.
app.use((err, _req, res, next) => {
  if (err instanceof SyntaxError && 'body' in err) {
    return res.status(400).json({ error: 'malformed JSON body' });
  }
  next(err);
});

const server = app.listen(config.port, () => {
  log.info('api.listening', { port: config.port });
});

function shutdown(signal) {
  log.info('api.shutdown', { signal });
  server.close(() => redis.quit().then(() => process.exit(0)));
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
