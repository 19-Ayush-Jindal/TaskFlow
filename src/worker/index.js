import { config } from '../config.js';
import { createRedis } from '../lib/redis.js';
import { log } from '../lib/log.js';
import { getJob, STATES } from '../lib/job.js';
import { workerKey } from '../lib/keys.js';
import { registerScripts, leaseNext, renewLease, completeJob, failJob } from '../lib/scripts.js';
import { getHandler } from '../handlers/index.js';

const redis = registerScripts(createRedis('worker'));
const queue = config.defaultQueue;

let running = true;
let currentJobId = null;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function processJob(id) {
  currentJobId = id;
  const started = Date.now();
  let lostLease = false;

  const job = await getJob(redis, id);
  if (!job) {
    log.warn('job.missing', { job_id: id });
    return;
  }

  log.info('job.started', {
    job_id: id, type: job.type, worker: config.workerId,
    lease_ms: config.leaseMs, reclaims: job.reclaims,
  });

  // Heartbeat. While the handler runs, push the deadline out every
  // heartbeatMs. Stop renewing - because the process died, or froze, or
  // lost its network - and the lease expires on its own. That is the whole
  // crash detection mechanism: nothing polls the worker, the worker simply
  // stops asserting it is alive.
  const heartbeat = setInterval(async () => {
    try {
      const ok = await renewLease(redis, queue, id, config.workerId, Date.now() + config.leaseMs);
      if (!ok) {
        lostLease = true;
        log.warn('job.lease_lost', { job_id: id, worker: config.workerId });
      }
    } catch (err) {
      log.error('job.heartbeat_failed', { job_id: id, error: err.message });
    }
  }, config.heartbeatMs);

  const handler = getHandler(job.type);

  try {
    if (!handler) throw new Error(`no handler for type ${job.type}`);
    const result = await handler(job.payload, job);

    if (lostLease) {
      // Someone else was handed this job while we were working. Writing our
      // result now would clobber theirs, so we drop it on the floor.
      log.warn('job.result_discarded', { job_id: id, reason: 'lease lost' });
      return;
    }

    const runMs = Date.now() - started;
    // Wait time is how long the job sat between becoming runnable and being
    // picked up. It is the number that tells you whether to add workers -
    // run time tells you about the handler, wait time about capacity.
    const waitMs = Math.max(0, started - (job.run_at || job.created_at));

    const applied = await completeJob(
      redis, queue, id, config.workerId, STATES.SUCCEEDED,
      'result', JSON.stringify(result ?? null),
      { type: job.type, runMs, waitMs },
    );
    if (applied) {
      log.info('job.succeeded', { job_id: id, type: job.type, ms: runMs, wait_ms: waitMs });
    } else {
      log.warn('job.completion_rejected', { job_id: id, reason: 'no longer the owner' });
    }
  } catch (err) {
    if (lostLease) {
      log.warn('job.result_discarded', { job_id: id, reason: 'lease lost' });
      return;
    }
    // failJob decides between another attempt and the dead-letter queue.
    // The worker does not know or care which - it just reports the failure.
    let outcome, attempts, delayMs;
    try {
      ({ outcome, attempts, delayMs } = await failJob(
        redis, queue, id, config.workerId, err.message,
      ));
    } catch (failErr) {
      // Could not even record the failure. The lease will expire and the
      // reaper will pick this up, but the cause needs to be visible.
      log.error('job.fail_record_failed', { job_id: id, error: failErr.message });
      return;
    }
    if (outcome === 'dead') {
      log.error('job.dead', {
        job_id: id, type: job.type, attempts, error: err.message,
      });
    } else {
      log.warn('job.retry_scheduled', {
        job_id: id, type: job.type, attempts,
        max_attempts: job.max_attempts, retry_in_ms: delayMs, error: err.message,
      });
    }
  } finally {
    clearInterval(heartbeat);
    currentJobId = null;
  }
}

async function loop() {
  log.info('worker.started', {
    worker: config.workerId, queue, lease_ms: config.leaseMs,
    heartbeat_ms: config.heartbeatMs,
  });

  // Presence. The worker asserts its own existence with a short TTL, the
  // same trick as a lease applied to the process itself. A worker that dies
  // while IDLE holds no lease and would otherwise vanish with no trace -
  // this is what lets /stats notice it is gone.
  const presence = setInterval(() => {
    redis.set(workerKey(config.workerId), String(Date.now()), 'PX', config.presenceTtlMs)
      .catch((err) => log.warn('worker.presence_failed', { error: err.message }));
  }, Math.floor(config.presenceTtlMs / 3));
  await redis.set(workerKey(config.workerId), String(Date.now()), 'PX', config.presenceTtlMs);

  let idle = 0;

  while (running) {
    try {
      const lease = await leaseNext(redis, queue, config.workerId, Date.now(), config.leaseMs);

      if (!lease) {
        idle += 1;
        // Poll fast for a moment after work dries up, then back off.
        await sleep(idle > 20 ? config.idlePollMs : config.pollIntervalMs);
        continue;
      }

      idle = 0;
      await processJob(lease.id);
    } catch (err) {
      if (!running) break;
      log.error('worker.loop_error', { error: err.message });
      await sleep(1000);
    }
  }

  clearInterval(presence);
  await redis.del(workerKey(config.workerId));
  log.info('worker.stopped', { worker: config.workerId });
  await redis.quit();
  process.exit(0);
}

// Graceful shutdown. The flag is only read at the top of the loop, so an
// in-flight job runs to completion first. A SIGKILL gives us none of this -
// which is exactly the case leases now cover.
function shutdown(signal) {
  log.info('worker.shutdown_requested', { signal, current_job: currentJobId });
  running = false;
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

loop();
