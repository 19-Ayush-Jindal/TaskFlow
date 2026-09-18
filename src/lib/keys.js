// Every Redis key TaskFlow touches is defined here. Keeping them in one
// place makes the data model obvious, and stops key typos from turning
// into silent "job vanished" bugs.

export const NS = 'taskflow';

export const jobKey       = (id)          => `${NS}:job:${id}`;
export const readyKey     = (queue, prio) => `${NS}:ready:${queue}:${prio}`;
export const scheduledKey = (queue)       => `${NS}:scheduled:${queue}`;
export const leasesKey    = (queue)       => `${NS}:leases:${queue}`;
export const deadKey      = (queue)       => `${NS}:dead:${queue}`;

// Counters and rolling latency aggregates. One hash, incremented inside the
// same Lua scripts that change job state, so a metric can never disagree
// with the transition it describes.
export const metricsKey = () => `${NS}:metrics`;

// Terminal successes, scored by completion time. Serves two purposes: the
// recent-jobs listing reads the top of it, and the sweeper reads the bottom
// to delete job hashes past their retention.
export const succeededZset = (queue) => `${NS}:succeeded:${queue}`;

// Recurring job definitions. cronsZset is the schedule (score = next fire
// time); cronNames is the durable roster, used to notice a definition that
// fell out of the schedule because a scheduler died mid-claim.
export const cronKey   = (name) => `${NS}:cron:${name}`;
export const cronsZset = ()     => `${NS}:crons`;
export const cronNames = ()     => `${NS}:cron-names`;
export const cronPrefix = ()    => `${NS}:cron:`;

// A worker asserts its own existence with a short TTL. Stop asserting and
// the key disappears - the same trick as a lease, applied to the process.
export const workerKey    = (id) => `${NS}:worker:${id}`;
export const workerPrefix = ()   => `${NS}:worker:`;

// Prefixes handed to Lua scripts, which build keys themselves.
export const jobPrefix   = ()      => `${NS}:job:`;
export const readyPrefix = (queue) => `${NS}:ready:${queue}:`;

// Ordered highest-priority-first, for multi-key BLPOP.
export const readyKeysInPriorityOrder = (queue, priorities) =>
  priorities.map((p) => readyKey(queue, p));
