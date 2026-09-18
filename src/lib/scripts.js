import { config, PRIORITIES } from '../config.js';
import {
  jobPrefix, readyPrefix, readyKeysInPriorityOrder,
  scheduledKey, leasesKey, deadKey, cronsZset, cronNames, cronPrefix,
  metricsKey, succeededZset,
} from './keys.js';

// ---------------------------------------------------------------------------
// Lua comments start with --, not //. Redis will refuse to compile the
// script otherwise, and the scheduler will retry the broken script forever.
// ---------------------------------------------------------------------------

// Take the next job and claim it, in ONE atomic operation.
//
// This replaces the bare BLPOP the worker used before. BLPOP removed an id
// from the ready list and left it owned by nobody until the worker got
// around to writing state=leased - and a crash in that gap lost the job
// with no trace. Here the pop, the lease and the state write cannot be
// separated: Redis runs the whole script on one thread, to completion.
//
// The cost: Lua cannot block, so the worker polls instead of parking on
// BLPOP. Pickup latency goes from ~1ms to ~25ms. That is the price of
// never losing a job, and it is worth it.
const LEASE_NEXT = `
local leasesKey = KEYS[#KEYS]
local now       = tonumber(ARGV[1])
local leaseMs   = tonumber(ARGV[2])
local workerId  = ARGV[3]
local jobPrefix = ARGV[4]

for i = 1, #KEYS - 1 do
  local id = redis.call('LPOP', KEYS[i])
  if id then
    local expiresAt = now + leaseMs
    redis.call('ZADD', leasesKey, expiresAt, id)
    redis.call('HSET', jobPrefix .. id,
      'state', 'leased',
      'worker_id', workerId,
      'lease_expires_at', expiresAt,
      'updated_at', now)
    return { id, tostring(expiresAt) }
  end
end

return nil
`;

// Push the lease deadline further out while the job is still running.
//
// Returns 0 if this worker no longer owns the job - either the lease
// expired and a reaper reclaimed it, or another worker now holds it. A
// worker that gets 0 must stop and discard its result, because someone
// else is running the same job. This is fencing: the check is what stops
// a slow worker from overwriting a newer worker's outcome.
const RENEW_LEASE = `
local leasesKey = KEYS[1]
local id        = ARGV[1]
local workerId  = ARGV[2]
local expiresAt = tonumber(ARGV[3])
local jobPrefix = ARGV[4]

if redis.call('ZSCORE', leasesKey, id) == false then return 0 end
if redis.call('HGET', jobPrefix .. id, 'worker_id') ~= workerId then return 0 end

redis.call('ZADD', leasesKey, 'XX', expiresAt, id)
redis.call('HSET', jobPrefix .. id, 'lease_expires_at', expiresAt)
return 1
`;

// Finish a job: drop the lease and write the terminal state - but only if
// this worker still owns it. Same fencing check as the heartbeat.
const COMPLETE_JOB = `
local leasesKey = KEYS[1]
local id        = ARGV[1]
local workerId  = ARGV[2]
local state     = ARGV[3]
local field     = ARGV[4]
local value     = ARGV[5]
local now       = ARGV[6]
local jobPrefix = ARGV[7]
local jobType   = ARGV[8]
local runMs     = tonumber(ARGV[9])
local waitMs    = tonumber(ARGV[10])
local metrics   = KEYS[2]
local doneZset  = KEYS[3]

if redis.call('HGET', jobPrefix .. id, 'worker_id') ~= workerId then return 0 end

redis.call('ZREM', leasesKey, id)
redis.call('HSET', jobPrefix .. id,
  'state', state, 'updated_at', now, field, value,
  'run_ms', runMs, 'wait_ms', waitMs)

-- Metrics are written in the same script as the transition, so they cannot
-- drift from reality or be lost to a crash between two round trips.
redis.call('HINCRBY', metrics, 'succeeded', 1)
redis.call('HINCRBY', metrics, 'succeeded:' .. jobType, 1)
redis.call('HINCRBY', metrics, 'run_ms_sum:' .. jobType, runMs)
redis.call('HINCRBY', metrics, 'run_count:' .. jobType, 1)
redis.call('HINCRBY', metrics, 'wait_ms_sum:' .. jobType, waitMs)

local prevMax = tonumber(redis.call('HGET', metrics, 'run_ms_max:' .. jobType)) or 0
if runMs > prevMax then
  redis.call('HSET', metrics, 'run_ms_max:' .. jobType, runMs)
end

redis.call('ZADD', doneZset, tonumber(now), id)
return 1
`;


// One attempt failed. Decide between retrying and giving up.
//
// This is the only place that decision lives. A handler that threw and a
// worker that died both arrive here, because "attempt N did not succeed"
// is the same fact either way - only the detection differed.
const FAIL_JOB = `
-- Retry backoff, computed inside Lua because only Redis knows the new
-- attempt count. Redis seeds math.random identically on every script call
-- so replicas stay in sync, which makes it useless for jitter - so the
-- caller passes a random number in instead.
local function backoffFor(attempts, baseMs, capMs, rand)
  local delay = baseMs * (2 ^ (attempts - 1))
  if delay > capMs then delay = capMs end
  -- Equal jitter: half the delay is fixed, half is random. Keeps a floor
  -- while still spreading a batch of simultaneous failures apart.
  return math.floor(delay / 2 + rand * (delay / 2))
end

local leasesKey    = KEYS[1]
local scheduledKey = KEYS[2]
local deadKey      = KEYS[3]

local id        = ARGV[1]
local workerId  = ARGV[2]
local errMsg    = ARGV[3]
local now       = tonumber(ARGV[4])
local baseMs    = tonumber(ARGV[5])
local capMs     = tonumber(ARGV[6])
local rand      = tonumber(ARGV[7])
local jobPrefix = ARGV[8]
local metrics   = KEYS[4]

local jobKey = jobPrefix .. id

-- Fencing: a worker whose lease was already reaped must not touch the job.
-- Pass '' as workerId to skip the check (the reaper owns nothing).
if workerId ~= '' and redis.call('HGET', jobKey, 'worker_id') ~= workerId then
  return { 0, 'not-owner', 0 }
end

redis.call('ZREM', leasesKey, id)

local attempts = redis.call('HINCRBY', jobKey, 'attempts', 1)
local maxAttempts = tonumber(redis.call('HGET', jobKey, 'max_attempts')) or 3

if attempts < maxAttempts then
  local delay = backoffFor(attempts, baseMs, capMs, rand)
  local runAt = now + delay
  redis.call('ZADD', scheduledKey, runAt, id)
  redis.call('HSET', jobKey,
    'state', 'scheduled',
    'run_at', runAt,
    'last_error', errMsg,
    'updated_at', now)
  redis.call('HDEL', jobKey, 'worker_id', 'lease_expires_at')
  redis.call('HINCRBY', metrics, 'failed_attempts', 1)
  return { 1, 'scheduled', attempts, delay }
end

-- Attempts exhausted. Into the dead-letter queue, where it stops consuming
-- workers and waits for a human to look at it.
redis.call('RPUSH', deadKey, id)
redis.call('HSET', jobKey,
  'state', 'dead',
  'last_error', errMsg,
  'updated_at', now)
redis.call('HDEL', jobKey, 'worker_id', 'lease_expires_at')
redis.call('HINCRBY', metrics, 'failed_attempts', 1)
redis.call('HINCRBY', metrics, 'dead', 1)
return { 1, 'dead', attempts, 0 }
`;

// Put a dead job back in play with a clean attempt counter. This is the
// operator's escape hatch: fix the bug, then replay what it broke.
const REPLAY_JOB = `
local deadKey   = KEYS[1]
local id        = ARGV[1]
local now       = tonumber(ARGV[2])
local jobPrefix = ARGV[3]
local readyPfx  = ARGV[4]

local jobKey = jobPrefix .. id
if redis.call('EXISTS', jobKey) == 0 then return { 0, 'no-such-job' } end
if redis.call('HGET', jobKey, 'state') ~= 'dead' then return { 0, 'not-dead' } end

redis.call('LREM', deadKey, 0, id)
local priority = redis.call('HGET', jobKey, 'priority') or '1'
redis.call('RPUSH', readyPfx .. priority, id)
redis.call('HSET', jobKey,
  'state', 'ready', 'attempts', 0, 'run_at', now, 'updated_at', now)
redis.call('HDEL', jobKey, 'last_error', 'worker_id', 'lease_expires_at')
return { 1, 'replayed' }
`;



// Delete the records of jobs that succeeded longer ago than the retention
// window. Without this Redis grows forever: every job ever run stays as a
// hash. Dead jobs are deliberately NOT swept - they are actionable, and
// losing them would defeat the point of a dead-letter queue.
const SWEEP_COMPLETED = `
local doneZset = KEYS[1]
local cutoff   = ARGV[1]
local limit    = ARGV[2]
local jobPrefix = ARGV[3]

local old = redis.call('ZRANGEBYSCORE', doneZset, '-inf', cutoff, 'LIMIT', 0, limit)
local swept = 0

for i = 1, #old do
  local id = old[i]
  if redis.call('ZREM', doneZset, id) == 1 then
    redis.call('DEL', jobPrefix .. id)
    swept = swept + 1
  end
end

return swept
`;

// Claim the cron definitions that are due.
//
// ZREM is the ownership token again, so several schedulers can beat at once
// and each occurrence is claimed by exactly one of them. The caller must
// re-add the name with its next fire time - it cannot be computed here,
// because Lua has no cron parser.
//
// The window: if a scheduler dies between winning the claim and re-adding
// the name, that definition drops out of the schedule. The roster set is
// how it gets noticed - see recoverCrons below.
const CLAIM_DUE_CRONS = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local claimed = {}

for i = 1, #due do
  local name = due[i]
  local slot = redis.call('ZSCORE', KEYS[1], name)
  if redis.call('ZREM', KEYS[1], name) == 1 then
    table.insert(claimed, name)
    table.insert(claimed, slot)
  end
end

return claimed
`;

// Names on the roster that are missing from the schedule. Either a
// scheduler died mid-claim, or the definition was just created by another
// process. Returned so the beat can put them back.
const ORPHANED_CRONS = `
local names = redis.call('SMEMBERS', KEYS[1])
local orphaned = {}

for i = 1, #names do
  local name = names[i]
  if redis.call('ZSCORE', KEYS[2], name) == false then
    if redis.call('HGET', ARGV[1] .. name, 'enabled') == '1' then
      table.insert(orphaned, name)
    end
  end
end

return orphaned
`;

// Move every job whose run_at has passed onto a ready list.
// ZREM returning 1 is the ownership token: removal is destructive, so only
// one caller can win it. That is why several schedulers can run at once
// with no lock and no leader election.
const PROMOTE_DUE = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local moved = 0
local orphaned = 0

for i = 1, #due do
  local id = due[i]
  if redis.call('ZREM', KEYS[1], id) == 1 then
    local jobKey = ARGV[3] .. id
    local priority = redis.call('HGET', jobKey, 'priority')
    if priority == false then
      orphaned = orphaned + 1
    else
      redis.call('RPUSH', ARGV[4] .. priority, id)
      redis.call('HSET', jobKey, 'state', 'ready', 'updated_at', ARGV[1])
      moved = moved + 1
    end
  end
end

return { moved, orphaned }
`;

// Reclaim jobs whose lease expired. An expired lease means the worker
// stopped renewing it, which means the worker is gone - crashed, OOM
// killed, partitioned, or wedged. No health check needed: silence past
// the deadline IS the evidence.
//
// A reclaim now counts as a failed attempt. Without that, a job that kills
// its worker gets handed to the next worker forever, and eventually there
// are no workers left.
const REAP_EXPIRED = `
-- Retry backoff, computed inside Lua because only Redis knows the new
-- attempt count. Redis seeds math.random identically on every script call
-- so replicas stay in sync, which makes it useless for jitter - so the
-- caller passes a random number in instead.
local function backoffFor(attempts, baseMs, capMs, rand)
  local delay = baseMs * (2 ^ (attempts - 1))
  if delay > capMs then delay = capMs end
  -- Equal jitter: half the delay is fixed, half is random. Keeps a floor
  -- while still spreading a batch of simultaneous failures apart.
  return math.floor(delay / 2 + rand * (delay / 2))
end

local leasesKey    = KEYS[1]
local scheduledKey = KEYS[2]
local deadKey      = KEYS[3]

local now       = tonumber(ARGV[1])
local limit     = ARGV[2]
local jobPrefix = ARGV[3]
local baseMs    = tonumber(ARGV[4])
local capMs     = tonumber(ARGV[5])
local rand      = tonumber(ARGV[6])
local metrics   = KEYS[4]

local expired = redis.call('ZRANGEBYSCORE', leasesKey, '-inf', now, 'LIMIT', 0, limit)
local retried = 0
local buried = 0

for i = 1, #expired do
  local id = expired[i]
  -- ZREM returning 1 is the ownership token, so several reapers are safe.
  if redis.call('ZREM', leasesKey, id) == 1 then
    local jobKey = jobPrefix .. id
    local prevOwner = redis.call('HGET', jobKey, 'worker_id') or 'unknown'
    local attempts = redis.call('HINCRBY', jobKey, 'attempts', 1)
    local maxAttempts = tonumber(redis.call('HGET', jobKey, 'max_attempts')) or 3
    redis.call('HINCRBY', jobKey, 'reclaims', 1)

    local errMsg = 'lease expired, reclaimed from ' .. prevOwner

    if attempts < maxAttempts then
      -- Vary the jitter across the batch so reclaimed jobs do not all come
      -- back at the same instant.
      local r = (rand + i * 0.37) % 1
      local delay = backoffFor(attempts, baseMs, capMs, r)
      redis.call('ZADD', scheduledKey, now + delay, id)
      redis.call('HSET', jobKey,
        'state', 'scheduled', 'run_at', now + delay,
        'last_error', errMsg, 'updated_at', now)
      retried = retried + 1
      redis.call('HINCRBY', metrics, 'reclaimed', 1)
    else
      redis.call('RPUSH', deadKey, id)
      redis.call('HSET', jobKey,
        'state', 'dead', 'last_error', errMsg, 'updated_at', now)
      buried = buried + 1
      redis.call('HINCRBY', metrics, 'reclaimed', 1)
      redis.call('HINCRBY', metrics, 'dead', 1)
    end
    redis.call('HDEL', jobKey, 'worker_id', 'lease_expires_at')
  end
end

return { retried, buried }
`;

export function registerScripts(redis) {
  redis.defineCommand('leaseNext',   { numberOfKeys: PRIORITIES.length + 1, lua: LEASE_NEXT });
  redis.defineCommand('renewLease',  { numberOfKeys: 1, lua: RENEW_LEASE });
  redis.defineCommand('completeJob', { numberOfKeys: 3, lua: COMPLETE_JOB });
  redis.defineCommand('promoteDue',  { numberOfKeys: 1, lua: PROMOTE_DUE });
  redis.defineCommand('failJob',     { numberOfKeys: 4, lua: FAIL_JOB });
  redis.defineCommand('reapExpired', { numberOfKeys: 4, lua: REAP_EXPIRED });
  redis.defineCommand('sweepCompleted', { numberOfKeys: 1, lua: SWEEP_COMPLETED });
  redis.defineCommand('replayJob',   { numberOfKeys: 1, lua: REPLAY_JOB });
  redis.defineCommand('claimDueCrons', { numberOfKeys: 1, lua: CLAIM_DUE_CRONS });
  redis.defineCommand('orphanedCrons', { numberOfKeys: 2, lua: ORPHANED_CRONS });
  return redis;
}

export async function leaseNext(redis, queue, workerId, nowMs, leaseMs) {
  const keys = [...readyKeysInPriorityOrder(queue, PRIORITIES), leasesKey(queue)];
  const result = await redis.leaseNext(
    ...keys,
    String(nowMs), String(leaseMs), workerId, jobPrefix(),
  );
  return result ? { id: result[0], expiresAt: Number(result[1]) } : null;
}

export async function renewLease(redis, queue, id, workerId, expiresAt) {
  const ok = await redis.renewLease(
    leasesKey(queue), id, workerId, String(expiresAt), jobPrefix(),
  );
  return ok === 1;
}

export async function completeJob(
  redis, queue, id, workerId, state, field, value,
  { type = 'unknown', runMs = 0, waitMs = 0 } = {},
) {
  const ok = await redis.completeJob(
    leasesKey(queue), metricsKey(), succeededZset(queue),
    id, workerId, state, field, value,
    String(Date.now()), jobPrefix(), type, String(runMs), String(waitMs),
  );
  return ok === 1;
}

export async function promoteDue(redis, queue, nowMs, limit) {
  const [moved, orphaned] = await redis.promoteDue(
    scheduledKey(queue), String(nowMs), String(limit), jobPrefix(), readyPrefix(queue),
  );
  return { moved, orphaned };
}

export async function failJob(redis, queue, id, workerId, errMsg) {
  const [applied, outcome, attempts, delay] = await redis.failJob(
    leasesKey(queue), scheduledKey(queue), deadKey(queue), metricsKey(),
    id, workerId, errMsg, String(Date.now()),
    String(config.retryBaseMs), String(config.retryCapMs), String(Math.random()),
    jobPrefix(),
  );
  return { applied: applied === 1, outcome, attempts, delayMs: delay };
}

export async function reapExpired(redis, queue, nowMs, limit) {
  const [retried, buried] = await redis.reapExpired(
    leasesKey(queue), scheduledKey(queue), deadKey(queue), metricsKey(),
    String(nowMs), String(limit), jobPrefix(),
    String(config.retryBaseMs), String(config.retryCapMs), String(Math.random()),
  );
  return { retried, buried };
}

export async function replayJob(redis, queue, id) {
  const [ok, reason] = await redis.replayJob(
    deadKey(queue), id, String(Date.now()), jobPrefix(), readyPrefix(queue),
  );
  return { ok: ok === 1, reason };
}

// Returns [{ name, slot }] for every cron occurrence this caller won.
export async function claimDueCrons(redis, nowMs, limit) {
  const flat = await redis.claimDueCrons(cronsZset(), String(nowMs), String(limit));
  const out = [];
  for (let i = 0; i < flat.length; i += 2) {
    out.push({ name: flat[i], slot: Number(flat[i + 1]) });
  }
  return out;
}

export async function orphanedCrons(redis) {
  return redis.orphanedCrons(cronNames(), cronsZset(), cronPrefix());
}

export async function sweepCompleted(redis, queue, cutoffMs, limit) {
  return redis.sweepCompleted(
    succeededZset(queue), String(cutoffMs), String(limit), jobPrefix(),
  );
}
