import { CronExpressionParser } from 'cron-parser';
import { config, DEFAULT_PRIORITY, PRIORITIES } from '../config.js';
import { cronKey, cronsZset, cronNames } from './keys.js';
import { getHandler, knownTypes } from '../handlers/index.js';

// Recurring jobs are not a new execution path. A cron definition is just a
// template plus a schedule; when it fires, the beat enqueues an ordinary
// job with a future-or-now run_at, and everything downstream - priorities,
// leases, retries, the DLQ - applies unchanged.

export function nextRunAt(expression, timezone, from = new Date()) {
  const interval = CronExpressionParser.parse(expression, {
    tz: timezone || 'UTC',
    currentDate: from,
  });
  return interval.next().getTime();
}

export function validateCron({ name, expression, type, priority, timezone }) {
  if (!name || !/^[a-zA-Z0-9_.:-]{1,64}$/.test(name)) {
    return 'name is required: 1-64 chars, letters/digits/._:- only';
  }
  if (!expression || typeof expression !== 'string') return 'expression is required';
  if (!type || !getHandler(type)) return `unknown job type: ${type} (known: ${knownTypes().join(', ')})`;
  if (priority !== undefined && !PRIORITIES.includes(priority)) {
    return `priority must be one of ${PRIORITIES.join(', ')}`;
  }
  try {
    nextRunAt(expression, timezone);
  } catch (err) {
    return `invalid cron expression: ${err.message}`;
  }
  return null;
}

function serialise(def) {
  return {
    name: def.name,
    expression: def.expression,
    type: def.type,
    payload: JSON.stringify(def.payload ?? {}),
    queue: def.queue,
    priority: String(def.priority),
    max_attempts: String(def.max_attempts),
    timezone: def.timezone,
    enabled: def.enabled ? '1' : '0',
    created_at: String(def.created_at),
    fire_count: String(def.fire_count ?? 0),
  };
}

function deserialise(hash) {
  if (!hash || !hash.name) return null;
  return {
    name: hash.name,
    expression: hash.expression,
    type: hash.type,
    payload: JSON.parse(hash.payload || '{}'),
    queue: hash.queue,
    priority: Number(hash.priority),
    max_attempts: Number(hash.max_attempts),
    timezone: hash.timezone,
    enabled: hash.enabled === '1',
    created_at: Number(hash.created_at),
    fire_count: Number(hash.fire_count || 0),
    last_fired_at: hash.last_fired_at ? Number(hash.last_fired_at) : null,
    last_job_id: hash.last_job_id || null,
  };
}

// Upsert. Re-registering an existing name replaces the definition and
// recomputes the next fire time from the new expression.
export async function putCron(redis, input) {
  const def = {
    name: input.name,
    expression: input.expression,
    type: input.type,
    payload: input.payload ?? {},
    queue: input.queue || config.defaultQueue,
    priority: input.priority ?? DEFAULT_PRIORITY,
    max_attempts: input.max_attempts ?? 3,
    timezone: input.timezone || config.cronTimezone,
    enabled: input.enabled !== false,
    created_at: Date.now(),
  };

  const next = nextRunAt(def.expression, def.timezone);

  const tx = redis.multi()
    .hset(cronKey(def.name), serialise(def))
    .sadd(cronNames(), def.name);

  if (def.enabled) tx.zadd(cronsZset(), next, def.name);
  else tx.zrem(cronsZset(), def.name);

  await tx.exec();
  return { ...def, next_run_at: next };
}

export async function getCron(redis, name) {
  const def = deserialise(await redis.hgetall(cronKey(name)));
  if (!def) return null;
  const score = await redis.zscore(cronsZset(), name);
  return { ...def, next_run_at: score ? Number(score) : null };
}

export async function listCrons(redis) {
  const names = await redis.smembers(cronNames());
  const defs = await Promise.all(names.map((n) => getCron(redis, n)));
  return defs.filter(Boolean).sort((a, b) => (a.next_run_at ?? Infinity) - (b.next_run_at ?? Infinity));
}

export async function deleteCron(redis, name) {
  const existed = await redis.exists(cronKey(name));
  if (!existed) return false;
  await redis.multi()
    .del(cronKey(name))
    .srem(cronNames(), name)
    .zrem(cronsZset(), name)
    .exec();
  return true;
}

export async function setCronEnabled(redis, name, enabled) {
  const def = await getCron(redis, name);
  if (!def) return null;

  const tx = redis.multi().hset(cronKey(name), { enabled: enabled ? '1' : '0' });
  if (enabled) tx.zadd(cronsZset(), nextRunAt(def.expression, def.timezone), name);
  else tx.zrem(cronsZset(), name);
  await tx.exec();

  return getCron(redis, name);
}
