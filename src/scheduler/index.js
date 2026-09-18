import { config } from '../config.js';
import { createRedis } from '../lib/redis.js';
import { log } from '../lib/log.js';
import {
  registerScripts, promoteDue, reapExpired, claimDueCrons, orphanedCrons,
  sweepCompleted,
} from '../lib/scripts.js';
import { buildJob, enqueue } from '../lib/job.js';
import { getCron, nextRunAt } from '../lib/cron.js';
import { cronKey, cronsZset, metricsKey } from '../lib/keys.js';

// Three maintenance jobs, one loop:
//
//   promote - move jobs whose run_at has passed onto a ready list
//   reap    - take back jobs whose lease expired, because the worker
//             holding them stopped renewing it and is therefore gone
//   beat    - fire recurring job definitions that are due
//   sweep   - delete records of jobs that succeeded past the retention
//             window, so Redis does not grow without bound
//
// All three use ZREM's return value as an ownership token, so any number of
// these can run at once without a lock and without double-firing.

const redis = registerScripts(createRedis('scheduler'));
const queue = config.defaultQueue;

let running = true;
let lastSweep = 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function tick() {
  const now = Date.now();

  const { moved, orphaned } = await promoteDue(redis, queue, now, config.promoteBatchSize);
  if (moved > 0) log.info('scheduler.promoted', { count: moved });
  if (orphaned > 0) log.warn('scheduler.orphaned', { count: orphaned });

  const { retried, buried } = await reapExpired(redis, queue, now, config.reapBatchSize);
  if (retried > 0) {
    log.warn('reaper.retried', {
      count: retried,
      note: 'lease expired, worker presumed dead - retrying with backoff',
    });
  }
  if (buried > 0) {
    log.error('reaper.buried', {
      count: buried,
      note: 'attempts exhausted - moved to dead-letter queue',
    });
  }

  const fired = await beat(now);

  // Sweeping is cheap and idempotent, but there is no point doing it 5
  // times a second. Roughly once a minute is plenty.
  if (now - lastSweep > 60000) {
    lastSweep = now;
    const swept = await sweepCompleted(
      redis, queue, now - config.retentionMs, config.sweepBatchSize,
    );
    if (swept > 0) log.info('sweeper.deleted', { count: swept });
  }

  return Math.max(moved, retried + buried, fired);
}

// Fire every due cron definition. A fired cron produces an ordinary job -
// nothing downstream knows or cares that it came from a schedule.
async function beat(now) {
  const claimed = await claimDueCrons(redis, now, 50);

  for (const { name, slot } of claimed) {
    try {
      const def = await getCron(redis, name);
      if (!def || !def.enabled) continue;

      const job = buildJob({
        type: def.type,
        payload: def.payload,
        queue: def.queue,
        priority: def.priority,
        max_attempts: def.max_attempts,
        cron_name: name,
      });
      await enqueue(redis, job);

      // Advance the schedule from the slot that just fired, not from now.
      // Computing from now would let a late beat silently drift the
      // schedule forward - a job due at 09:00 fired at 09:02 would set the
      // next one to 10:02.
      const next = nextRunAt(def.expression, def.timezone, new Date(slot));

      await redis.multi()
        .zadd(cronsZset(), next, name)
        .hset(cronKey(name), {
          last_fired_at: String(now),
          last_job_id: job.id,
        })
        .hincrby(cronKey(name), 'fire_count', 1)
        .hincrby(metricsKey(), 'cron_fired', 1)
        .exec();

      log.info('cron.fired', {
        cron: name, job_id: job.id, type: def.type,
        slot: new Date(slot).toISOString(),
        next: new Date(next).toISOString(),
        late_by_ms: now - slot,
      });
    } catch (err) {
      log.error('cron.fire_failed', { cron: name, error: err.message });
      // Put it back so the definition is not lost, even if this occurrence
      // was. Recomputed from now, so it will not immediately re-fire.
      try {
        const def = await getCron(redis, name);
        if (def) await redis.zadd(cronsZset(), nextRunAt(def.expression, def.timezone), name);
      } catch { /* the orphan sweep below is the backstop */ }
    }
  }

  // Backstop: any enabled definition on the roster but missing from the
  // schedule gets re-added. Covers a scheduler that died between claiming
  // an occurrence and rescheduling it.
  const orphans = await orphanedCrons(redis);
  for (const name of orphans) {
    const def = await getCron(redis, name);
    if (!def) continue;
    await redis.zadd(cronsZset(), nextRunAt(def.expression, def.timezone), name);
    log.warn('cron.rescheduled_orphan', { cron: name });
  }

  return claimed.length;
}

async function loop() {
  log.info('scheduler.started', {
    scheduler: config.schedulerId, queue, tick_ms: config.tickIntervalMs,
  });

  while (running) {
    try {
      const worked = await tick();
      // A full batch means more is waiting; drain it instead of trickling
      // one batch per tick.
      if (worked < config.promoteBatchSize) await sleep(config.tickIntervalMs);
    } catch (err) {
      // A malformed Lua script will never compile, so retrying is pointless
      // and buries every other log line. Die loudly instead.
      if (/Error compiling script|NOSCRIPT/i.test(err.message)) {
        log.error('scheduler.fatal', { error: err.message });
        process.exit(1);
      }
      log.error('scheduler.tick_failed', { error: err.message });
      await sleep(1000);
    }
  }

  log.info('scheduler.stopped', { scheduler: config.schedulerId });
  await redis.quit();
  process.exit(0);
}

function shutdown(signal) {
  log.info('scheduler.shutdown_requested', { signal });
  running = false;
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

loop();
