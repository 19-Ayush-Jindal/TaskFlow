// A single self-contained page served at /. Polls /stats and /metrics so
// the queue can be watched while experiments run, which is much easier than
// reading interleaved container logs.

export const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TaskFlow</title>
<style>
  :root {
    --bg: #fbfbfa; --panel: #fff; --ink: #1c1c1a; --muted: #75746f;
    --line: #e6e5e1; --accent: #3f6b4e; --warn: #9a6b1f; --bad: #9a3535;
    --mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #17181a; --panel: #1f2124; --ink: #ecebe7; --muted: #94938d;
      --line: #2e3135; --accent: #86bb98; --warn: #d6a84e; --bad: #e08a8a;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 32px 24px 64px; background: var(--bg); color: var(--ink);
    font: 15px/1.55 system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  .wrap { max-width: 1000px; margin: 0 auto; }
  header { display: flex; align-items: baseline; gap: 14px; margin-bottom: 4px; }
  h1 { font-size: 21px; margin: 0; letter-spacing: -0.01em; }
  .sub { color: var(--muted); font-size: 13px; margin-bottom: 26px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--accent); display: inline-block; }
  .dot.bad { background: var(--bad); }
  h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.07em;
       color: var(--muted); margin: 30px 0 12px; font-weight: 600; }
  .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(132px, 1fr)); }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; }
  .card .n { font-size: 26px; font-variant-numeric: tabular-nums; letter-spacing: -0.02em; }
  .card .l { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .card.hot .n { color: var(--warn); }
  .card.bad .n { color: var(--bad); }
  table { width: 100%; border-collapse: collapse; background: var(--panel);
          border: 1px solid var(--line); border-radius: 10px; overflow: hidden; }
  th, td { text-align: left; padding: 9px 14px; border-bottom: 1px solid var(--line); font-size: 13px; }
  th { font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); font-weight: 600; }
  tr:last-child td { border-bottom: none; }
  td.mono, .mono { font-family: var(--mono); font-size: 12px; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .empty { color: var(--muted); font-style: italic; padding: 14px; }
  .banner { background: var(--bad); color: #fff; padding: 10px 14px; border-radius: 8px;
            margin-bottom: 18px; font-size: 13px; }
  .scroll { overflow-x: auto; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>TaskFlow</h1>
    <span class="sub" style="margin:0"><span class="dot" id="dot"></span> <span id="status">connecting</span></span>
  </header>
  <div class="sub">distributed job scheduler &amp; worker queue &middot; refreshing every second</div>

  <div id="banner"></div>

  <h2>Queue</h2>
  <div class="grid" id="queue"></div>

  <h2>Since start</h2>
  <div class="grid" id="totals"></div>

  <h2>By job type</h2>
  <div class="scroll"><table id="types"></table></div>

  <h2>Recurring</h2>
  <div class="scroll"><table id="crons"></table></div>

  <h2>Recently completed</h2>
  <div class="scroll"><table id="recent"></table></div>

  <h2>Dead letter queue</h2>
  <div class="scroll"><table id="dead"></table></div>
</div>

<script>
const el = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

function cards(target, items) {
  el(target).innerHTML = items.map(([label, value, cls]) =>
    '<div class="card ' + (cls || '') + '"><div class="n">' + esc(value) +
    '</div><div class="l">' + esc(label) + '</div></div>').join('');
}

function table(target, headers, rows, emptyMsg) {
  const t = el(target);
  if (!rows.length) {
    t.innerHTML = '<tr><td class="empty">' + esc(emptyMsg) + '</td></tr>';
    return;
  }
  t.innerHTML =
    '<tr>' + headers.map((h) => '<th>' + esc(h) + '</th>').join('') + '</tr>' +
    rows.map((r) => '<tr>' + r.map((c) =>
      '<td class="' + (c && c.cls || '') + '">' +
      esc(c && c.v !== undefined ? c.v : c) + '</td>').join('') + '</tr>').join('');
}

const ago = (ms) => {
  if (!ms) return '-';
  const s = Math.round((Date.now() - ms) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.round(s / 60) + 'm ago';
  return Math.round(s / 3600) + 'h ago';
};

async function tick() {
  try {
    const [stats, metrics, crons, dead, recent, health] = await Promise.all(
      ['/stats', '/metrics', '/crons', '/dead?limit=10', '/jobs?limit=10', '/health']
        .map((u) => fetch(u).then((r) => r.json())));

    el('dot').className = health.ok ? 'dot' : 'dot bad';
    el('status').textContent = health.ok
      ? stats.workers + ' worker' + (stats.workers === 1 ? '' : 's') + ' live'
      : 'stalled';
    el('banner').innerHTML = health.stalled
      ? '<div class="banner">' + esc(health.reason) + '</div>' : '';

    const ready = Object.values(stats.ready).reduce((a, b) => a + b, 0);
    cards('queue', [
      ['workers', stats.workers, stats.workers ? '' : 'bad'],
      ['ready', ready, ready > 20 ? 'hot' : ''],
      ['in flight', stats.leased],
      ['scheduled', stats.scheduled],
      ['dead letter', stats.dead, stats.dead ? 'bad' : ''],
    ]);

    const t = metrics.totals;
    cards('totals', [
      ['enqueued', t.enqueued],
      ['succeeded', t.succeeded],
      ['failed attempts', t.failed_attempts, t.failed_attempts ? 'hot' : ''],
      ['reclaimed', t.reclaimed, t.reclaimed ? 'hot' : ''],
      ['buried', t.dead, t.dead ? 'bad' : ''],
      ['cron fires', t.cron_fired],
    ]);

    table('types', ['type', 'enqueued', 'succeeded', 'avg run', 'max run', 'avg wait'],
      Object.entries(metrics.by_type).map(([name, m]) => [
        { v: name, cls: 'mono' },
        { v: m.enqueued, cls: 'num' }, { v: m.succeeded, cls: 'num' },
        { v: m.avg_run_ms === null ? '-' : m.avg_run_ms + 'ms', cls: 'num' },
        { v: m.max_run_ms === null ? '-' : m.max_run_ms + 'ms', cls: 'num' },
        { v: m.avg_wait_ms === null ? '-' : m.avg_wait_ms + 'ms', cls: 'num' },
      ]), 'no jobs yet');

    table('crons', ['name', 'expression', 'type', 'enabled', 'fires', 'next'],
      (crons.crons || []).map((c) => [
        { v: c.name, cls: 'mono' }, { v: c.expression, cls: 'mono' },
        { v: c.type, cls: 'mono' }, c.enabled ? 'yes' : 'paused',
        { v: c.fire_count, cls: 'num' },
        c.next_run_at ? new Date(c.next_run_at).toLocaleTimeString() : '-',
      ]), 'no recurring jobs registered');

    table('recent', ['id', 'type', 'from cron', 'attempts', 'finished'],
      (recent.jobs || []).map((j) => [
        { v: j.id.slice(-10), cls: 'mono' }, { v: j.type, cls: 'mono' },
        j.cron_name || '-', { v: j.attempts, cls: 'num' }, ago(j.updated_at),
      ]), 'nothing completed yet');

    table('dead', ['id', 'type', 'attempts', 'last error'],
      (dead.jobs || []).map((j) => [
        { v: j.id.slice(-10), cls: 'mono' }, { v: j.type, cls: 'mono' },
        { v: j.attempts, cls: 'num' }, j.last_error,
      ]), 'empty - nothing has exhausted its attempts');
  } catch (err) {
    el('dot').className = 'dot bad';
    el('status').textContent = 'api unreachable';
  }
}

tick();
setInterval(tick, 1000);
</script>
</body>
</html>`;
