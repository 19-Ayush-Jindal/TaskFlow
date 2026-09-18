import os from 'node:os';

export const config = {
  port: Number(process.env.PORT || 3000),
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  defaultQueue: process.env.DEFAULT_QUEUE || 'default',

  // In a container the Node process is always PID 1, so process.pid gives
  // every worker the identical id. The hostname is the container's short
  // id, which is actually unique - and uniqueness matters now that
  // worker_id decides who is allowed to write a job's result.
  workerId: process.env.WORKER_ID || `worker-${os.hostname()}`,
  schedulerId: process.env.SCHEDULER_ID || `scheduler-${os.hostname()}`,

  // How long a worker owns a job before the lease expires. Long enough to
  // survive a GC pause or a slow Redis call, short enough that a crashed
  // worker's job comes back quickly.
  leaseMs: Number(process.env.LEASE_MS || 30000),
  // Renew well before expiry so one dropped heartbeat is not fatal.
  heartbeatMs: Number(process.env.HEARTBEAT_MS || 10000),

  // Leasing is a Lua script, and Lua cannot block, so the worker polls.
  // Idle workers back off to idlePollMs; a worker that just got a job
  // tries again immediately.
  pollIntervalMs: Number(process.env.POLL_INTERVAL_MS || 25),
  idlePollMs: Number(process.env.IDLE_POLL_MS || 200),

  // Retry backoff: base * 2^(attempts-1), capped, then jittered so a batch
  // of jobs that failed together does not retry in lockstep.
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  retryCapMs: Number(process.env.RETRY_CAP_MS || 300000),

  // Default timezone for cron expressions. A daily 9am report should mean
  // 9am where the business is, not 9am UTC.
  cronTimezone: process.env.CRON_TIMEZONE || 'UTC',

  // Presence key TTL. A worker refreshes this while it polls; if it stops,
  // the key expires and /stats reports one fewer worker.
  presenceTtlMs: Number(process.env.PRESENCE_TTL_MS || 15000),

  // How long a succeeded job's record is kept before the sweeper deletes
  // it. Without this, Redis memory grows without bound - every job ever run
  // stays as a hash forever.
  retentionMs: Number(process.env.RETENTION_MS || 24 * 60 * 60 * 1000),
  sweepBatchSize: Number(process.env.SWEEP_BATCH_SIZE || 500),

  // Maintenance loop: promote due jobs, reclaim expired leases.
  tickIntervalMs: Number(process.env.TICK_INTERVAL_MS || 200),
  promoteBatchSize: Number(process.env.PROMOTE_BATCH_SIZE || 100),
  reapBatchSize: Number(process.env.REAP_BATCH_SIZE || 100),
};

// Priority 0 is highest. The lease script tries the ready lists in this
// order and takes from the first non-empty one.
export const PRIORITIES = [0, 1, 2];
export const DEFAULT_PRIORITY = 1;
