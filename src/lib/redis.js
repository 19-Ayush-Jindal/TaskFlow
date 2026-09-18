import Redis from 'ioredis';
import { config } from '../config.js';

// Blocking commands (BLPOP) monopolise a connection, so anything that
// blocks gets its own client. Sharing one would stall every other command.
export function createRedis(label = 'main') {
  const client = new Redis(config.redisUrl, { maxRetriesPerRequest: null });
  client.on('error', (err) => console.error(`[redis:${label}]`, err.message));
  return client;
}
