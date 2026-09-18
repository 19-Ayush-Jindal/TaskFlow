const ts = () => new Date().toISOString();

export const log = {
  info:  (msg, meta = {}) => console.log(JSON.stringify({ ts: ts(), level: 'info', msg, ...meta })),
  warn:  (msg, meta = {}) => console.warn(JSON.stringify({ ts: ts(), level: 'warn', msg, ...meta })),
  error: (msg, meta = {}) => console.error(JSON.stringify({ ts: ts(), level: 'error', msg, ...meta })),
};
