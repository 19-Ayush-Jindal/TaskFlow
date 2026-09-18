// A handler is just an async function that receives the job payload.
// Throwing marks the job failed; returning normally marks it succeeded.
// Handlers MUST be idempotent - at-least-once delivery means a job can
// legitimately run twice (worker finishes the work, then dies before
// recording the result).

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const handlers = {
  'demo.echo': async (payload) => {
    return { echoed: payload };
  },

  'demo.slow': async (payload) => {
    const ms = Number(payload.ms ?? 3000);
    await sleep(ms);
    return { slept_ms: ms };
  },

  // Simulates a hard crash: the process vanishes mid-job with no chance to
  // catch a signal, write a state, or log anything. Exactly what an OOM kill
  // or a yanked power cable looks like. The lease is the only thing that
  // notices.
  'demo.crash': async (payload) => {
    const after = Number(payload.after_ms ?? 2000);
    setTimeout(() => process.exit(137), after);
    await sleep(after + 60000); // never reached
  },

  'demo.fail': async () => {
    throw new Error('this handler always fails, on purpose');
  },
};

export function getHandler(type) {
  return handlers[type] || null;
}

export const knownTypes = () => Object.keys(handlers);
