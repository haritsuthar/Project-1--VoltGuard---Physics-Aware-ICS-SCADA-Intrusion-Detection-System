/* ── VoltGuard Dashboard ── app.js ─────────────────────────────────────── */
'use strict';

// ── State ───────────────────────────────────────────────────────────────────
let donutChart   = null;
let barChart     = null;
let lineChart    = null;
let allRecords   = [];
let sseSource    = null;
let apiSseSource = null;
let sidebarOpen  = window.innerWidth > 768;

// ── Boot ────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  tickClock();
  setInterval(tickClock, 1000);
  loadAll();
  if (window.innerWidth <= 768) {
    document.getElementById('sidebar').classList.remove('open');
  }
});

function tickClock() {
  const el = document.getElementById('topbar-clock');
  if (el) el.textContent = new Date().toLocaleString(undefined, {
    month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', second:'2-digit'
  });
}

// ── Navigation ───────────────────────────────────────────────────────────────
function showSection(name, el) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const sec = document.getElementById('section-' + name);
  if (sec) sec.classList.add('active');
  if (el) el.classList.add('active');
  const crumb = document.getElementById('breadcrumb-text');
  if (crumb) crumb.textContent = el ? el.textContent.trim() : name;
}

function toggleSidebar() {
  const sb = document.getElementById('sidebar');
  sidebarOpen = !sidebarOpen;
  sb.classList.toggle('open', sidebarOpen);
}

// ── Data Loading ─────────────────────────────────────────────────────────────
async function loadAll() {
  try {
    await Promise.all([loadStats(), loadDecisions()]);
  } catch (e) {
    console.warn('loadAll error:', e);
  }
}

async function loadStats() {
  const res  = await fetch('/api/stats');
  const data = await res.json();

  // KPI cards
  setText('kpi-total', data.total ?? '—');
  setText('kpi-allow', data.allow ?? '—');
  setText('kpi-drop',  data.drop  ?? '—');
  setText('kpi-allow-pct', data.total ? data.allow_pct + '%' : '—%');
  setText('kpi-drop-pct',  data.total ? data.drop_pct  + '%' : '—%');

  // Donut badge
  setText('donut-label', data.total
    ? `${data.allow_pct}% safe · ${data.drop_pct}% blocked` : '—');

  renderDonut(data);
}

async function loadDecisions() {
  const res  = await fetch('/api/decisions');
  const data = await res.json();
  allRecords = data.records || [];
  renderTable(allRecords);
  renderBarChart(allRecords);
  renderLineChart(allRecords);
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// ── Pipeline Run ─────────────────────────────────────────────────────────────
function runPipeline() {
  if (sseSource) { sseSource.close(); sseSource = null; }

  const btnMain  = document.getElementById('btn-run');
  const btnPipe  = document.getElementById('btn-run-pipeline');
  const badge    = document.getElementById('pipeline-badge');
  const dot      = document.getElementById('status-dot');
  const label    = document.getElementById('status-label');
  const terminal = document.getElementById('terminal');

  [btnMain, btnPipe].forEach(b => { if (b) b.disabled = true; });
  setPipelineBadge('running', 'Running…');
  dot.className = 'status-dot running';
  label.textContent = 'Running';

  clearTerminal();
  termLog('info', '▶ Starting VoltGuard pipeline...');

  setFlowStep('gen',  'active',  'running');
  setFlowStep('phys', 'waiting', 'waiting');
  setFlowStep('dec',  'waiting', 'waiting');
  setFlowStep('out',  'waiting', 'waiting');

  sseSource = new EventSource('/api/stream');

  sseSource.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      const msg = d.msg || '';
      termLog(d.type || 'info', msg);

      // Update flow steps based on message content
      if (msg.includes('[1/2]') && msg.includes('created')) {
        setFlowStep('gen',  'success', 'done');
        setFlowStep('phys', 'active',  'running');
      }
      if (msg.includes('[2/2]') && msg.includes('Running')) {
        setFlowStep('phys', 'success', 'done');
        setFlowStep('dec',  'active',  'running');
      }
      if (d.type === 'done') {
        setFlowStep('dec', 'success', 'done');
        setFlowStep('out', 'success', 'done');
        sseSource.close(); sseSource = null;
        [btnMain, btnPipe].forEach(b => { if (b) b.disabled = false; });
        setPipelineBadge('done', 'Done');
        dot.className = 'status-dot done';
        label.textContent = 'Done';
        loadAll();
        showToast('Pipeline completed successfully');
      }
    } catch (_) {}
  };

  sseSource.onerror = () => {
    if (sseSource) { sseSource.close(); sseSource = null; }
    [btnMain, btnPipe].forEach(b => { if (b) b.disabled = false; });
    setPipelineBadge('error', 'Error');
    dot.className = 'status-dot error';
    label.textContent = 'Error';
    termLog('error', 'Connection lost or pipeline error.');
    setFlowStep('gen',  'error', 'error');
  };
}

function setPipelineBadge(state, text) {
  const badge = document.getElementById('pipeline-badge');
  if (!badge) return;
  badge.className = 'pbadge pbadge-' + state;
  badge.innerHTML = `<span class="pbadge-dot"></span>${text}`;
}

function setFlowStep(key, cls, statusText) {
  const step = document.getElementById('flow-' + key);
  const stat = document.getElementById('flow-' + key + '-status');
  if (step) step.className = 'flow-step ' + cls;
  if (stat) stat.textContent = statusText;
}

function clearTerminal() {
  const t = document.getElementById('terminal');
  if (t) t.innerHTML = '';
}

function termLog(type, text) {
  const t = document.getElementById('terminal');
  if (!t) return;
  const span = document.createElement('span');
  span.className = 'term-line ' + type;
  span.textContent = text;
  t.appendChild(span);
  t.appendChild(document.createTextNode('\n'));
  t.scrollTop = t.scrollHeight;
}

// ── Table ────────────────────────────────────────────────────────────────────
function renderTable(records) {
  const tbody = document.getElementById('table-body');
  const meta  = document.getElementById('table-meta');
  if (!tbody) return;

  if (meta) meta.textContent = records.length
    ? `Showing ${records.length} record${records.length !== 1 ? 's' : ''}`
    : 'No records';

  if (!records.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="empty-state">
      <div class="empty-icon">📋</div>
      <div>No data yet — click <strong>Run Pipeline</strong> to get started</div>
    </td></tr>`;
    return;
  }

  tbody.innerHTML = records.map((r, i) => {
    const isFail = r.state === 'Catastrophic Failure';
    const isWarn = r.pressure_bar > 12;
    return `<tr>
      <td class="col-num">${i + 1}</td>
      <td>${r.device_id ?? '—'}</td>
      <td>${r.register ?? '—'}</td>
      <td><strong>${r.value ?? '—'}</strong></td>
      <td class="${isWarn ? 'pressure-warn' : ''}">${fmt(r.pressure_bar)}</td>
      <td>${fmt(r.flow_rate)}</td>
      <td class="${isFail ? 'state-fail' : 'state-safe'}">${r.state ?? '—'}</td>
      <td><span class="${r.action === 'ALLOW' ? 'badge-allow' : 'badge-drop'}">${r.action ?? '—'}</span></td>
      <td>${r.reason ?? '—'}</td>
    </tr>`;
  }).join('');
}

function filterTable() {
  const text   = (document.getElementById('filter-input')?.value || '').toLowerCase();
  const action = document.getElementById('filter-action')?.value || '';
  const filtered = allRecords.filter(r => {
    const matchAction = !action || r.action === action;
    const matchText   = !text   || JSON.stringify(r).toLowerCase().includes(text);
    return matchAction && matchText;
  });
  renderTable(filtered);
}

function exportCSV() {
  if (!allRecords.length) { showToast('No data to export'); return; }
  const headers = ['#','device_id','register','value','pressure_bar','flow_rate','state','action','reason'];
  const rows = allRecords.map((r, i) =>
    [i+1, r.device_id, r.register, r.value, r.pressure_bar, r.flow_rate,
     `"${r.state}"`, r.action, `"${r.reason}"`].join(',')
  );
  const csv  = [headers.join(','), ...rows].join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = 'voltguard_decisions.csv';
  a.click(); URL.revokeObjectURL(url);
  showToast('CSV exported');
}

function fmt(v) {
  return (v !== undefined && v !== null) ? Number(v).toFixed(3) : '—';
}

// ── Donut Chart ───────────────────────────────────────────────────────────────
function renderDonut(stats) {
  const canvas = document.getElementById('donutChart');
  if (!canvas) return;
  const ctx   = canvas.getContext('2d');
  const allow = stats.allow || 0;
  const drop  = stats.drop  || 0;
  const total = stats.total || 0;

  setText('donut-total', total || '—');

  const chartData = {
    labels: ['ALLOW', 'DROP'],
    datasets: [{
      data: [allow, drop],
      backgroundColor: ['#3fb950', '#f85149'],
      borderColor:     ['#12261e', '#2d1214'],
      borderWidth: 2,
      hoverOffset: 8,
    }]
  };

  if (donutChart) {
    donutChart.data = chartData;
    donutChart.update('none');
    return;
  }

  donutChart = new Chart(ctx, {
    type: 'doughnut',
    data: chartData,
    options: {
      cutout: '70%',
      animation: { duration: 600 },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.label}: ${ctx.parsed}  (${total ? ((ctx.parsed/total)*100).toFixed(1) : 0}%)`
          }
        }
      }
    }
  });
}

// ── Bar Chart ─────────────────────────────────────────────────────────────────
function renderBarChart(records) {
  const canvas = document.getElementById('barChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  if (!records.length) {
    if (barChart) { barChart.destroy(); barChart = null; }
    return;
  }

  const labels    = records.map((r, i) => `#${i+1} · v=${r.value}`);
  const pressures = records.map(r => r.pressure_bar);
  const flows     = records.map(r => r.flow_rate);
  const bgColors  = records.map(r => r.action === 'DROP' ? '#f8514944' : '#3fb95044');
  const bdColors  = records.map(r => r.action === 'DROP' ? '#f85149'   : '#3fb950');

  const chartData = {
    labels,
    datasets: [
      {
        label: 'Pressure (bar)',
        data: pressures,
        backgroundColor: bgColors,
        borderColor: bdColors,
        borderWidth: 1.5,
        borderRadius: 3,
        yAxisID: 'y',
      },
      {
        label: 'Flow Rate',
        type: 'line',
        data: flows,
        borderColor: '#388bfd',
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 3,
        pointBackgroundColor: '#388bfd',
        tension: 0.4,
        yAxisID: 'y1',
      }
    ]
  };

  const opts = {
    responsive: true,
    maintainAspectRatio: true,
    animation: { duration: 500 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { labels: { color: '#636e7b', font: { size: 11 } } },
      tooltip: {
        callbacks: {
          afterBody: (items) => {
            const r = records[items[0]?.dataIndex];
            return r ? [`Action: ${r.action}`, `State: ${r.state}`] : [];
          }
        }
      }
    },
    scales: {
      x:  { ticks: { color: '#636e7b', font: { size: 10 }, maxRotation: 40 }, grid: { color: '#2d333b' } },
      y:  { ticks: { color: '#636e7b' }, grid: { color: '#2d333b' },
            title: { display: true, text: 'Pressure (bar)', color: '#636e7b', font: { size: 11 } } },
      y1: { position: 'right', ticks: { color: '#388bfd' }, grid: { drawOnChartArea: false },
            title: { display: true, text: 'Flow Rate', color: '#388bfd', font: { size: 11 } } }
    }
  };

  if (barChart) {
    barChart.data    = chartData;
    barChart.options = opts;
    barChart.update('none');
    return;
  }

  barChart = new Chart(ctx, { type: 'bar', data: chartData, options: opts });
}

// ── Line Chart ────────────────────────────────────────────────────────────────
function renderLineChart(records) {
  const canvas = document.getElementById('lineChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  if (!records.length) {
    if (lineChart) { lineChart.destroy(); lineChart = null; }
    return;
  }

  const labels    = records.map((_, i) => `#${i+1}`);
  const pressures = records.map(r => r.pressure_bar);
  const ptColors  = records.map(r => r.action === 'DROP' ? '#f85149' : '#3fb950');

  const chartData = {
    labels,
    datasets: [{
      label: 'Pressure (bar)',
      data: pressures,
      borderColor: '#388bfd',
      backgroundColor: 'rgba(56,139,253,0.08)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 4,
      pointBackgroundColor: ptColors,
      pointBorderColor: ptColors,
    }]
  };

  const opts = {
    responsive: true,
    maintainAspectRatio: true,
    animation: { duration: 500 },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (ctx) => ` Pressure: ${ctx.parsed.y} bar`,
          afterLabel: (ctx) => ` Action: ${records[ctx.dataIndex]?.action}`
        }
      },
      // draw threshold line via annotation-free approach
    },
    scales: {
      x: { ticks: { color: '#636e7b', font: { size: 10 } }, grid: { color: '#2d333b' } },
      y: {
        ticks: { color: '#636e7b' }, grid: { color: '#2d333b' },
        title: { display: true, text: 'Pressure (bar)', color: '#636e7b', font: { size: 11 } },
      }
    }
  };

  // Custom plugin: draw threshold line at 12 bar
  const thresholdPlugin = {
    id: 'threshold',
    afterDraw(chart) {
      const { ctx: c, scales: { y, x } } = chart;
      if (!y || !x) return;
      const yPx = y.getPixelForValue(12);
      c.save();
      c.beginPath();
      c.moveTo(x.left, yPx);
      c.lineTo(x.right, yPx);
      c.strokeStyle = '#f8514988';
      c.lineWidth = 1.5;
      c.setLineDash([5, 4]);
      c.stroke();
      c.fillStyle = '#f85149';
      c.font = '10px sans-serif';
      c.fillText('Limit 12 bar', x.right - 64, yPx - 5);
      c.restore();
    }
  };

  if (lineChart) {
    lineChart.data    = chartData;
    lineChart.options = opts;
    lineChart.update('none');
    return;
  }

  lineChart = new Chart(ctx, { type: 'line', data: chartData, options: opts, plugins: [thresholdPlugin] });
}

// ── API Explorer ──────────────────────────────────────────────────────────────
function selectEndpoint(el, name) {
  document.querySelectorAll('.endpoint-item').forEach(e => e.classList.remove('active'));
  document.querySelectorAll('.ep-detail').forEach(e => e.classList.remove('active'));
  el.classList.add('active');
  const detail = document.getElementById('ep-' + name);
  if (detail) detail.classList.add('active');
}

async function testEndpoint(name) {
  const el = document.getElementById('response-' + name);
  if (!el) return;
  el.textContent = 'Loading…';

  try {
    let res;
    if (name === 'run') {
      el.textContent = 'Sending POST /api/run …';
      res = await fetch('/api/run', { method: 'POST' });
    } else {
      res = await fetch('/api/' + name);
    }

    const data = await res.json();

    // Truncate decisions for readability
    if (name === 'decisions' && Array.isArray(data.records) && data.records.length > 3) {
      const preview = {
        records: data.records.slice(0, 3),
        _note: `... ${data.records.length - 3} more records (showing first 3 of ${data.records.length})`
      };
      el.textContent = JSON.stringify(preview, null, 2);
    } else {
      el.textContent = JSON.stringify(data, null, 2);
    }

    if (name === 'run' && data.status === 'success') {
      loadAll();
      showToast('Pipeline complete — dashboard refreshed');
    }
  } catch (err) {
    if (el) el.textContent = 'Error: ' + err.message;
  }
}

function openStream() {
  const el = document.getElementById('response-stream');
  if (!el) return;

  // Close any existing SSE to avoid duplicate connections
  if (apiSseSource) { apiSseSource.close(); apiSseSource = null; }

  el.textContent = '';

  // Use fetch instead of EventSource so we fully control reconnection
  apiSseSource = new EventSource('/api/stream');

  apiSseSource.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      el.textContent += d.msg + '\n';
      el.scrollTop = el.scrollHeight;

      if (d.type === 'done' || d.type === 'error') {
        apiSseSource.close();
        apiSseSource = null;
        if (d.type === 'done') loadAll();
      }
    } catch (_) {}
  };

  // onerror fires when stream closes normally too — don't treat as fatal
  apiSseSource.onerror = () => {
    if (apiSseSource) { apiSseSource.close(); apiSseSource = null; }
    if (el.textContent && !el.textContent.includes('[stream closed]')) {
      el.textContent += '\n[stream closed]';
    }
  };
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function copyUrl(path) {
  const url = window.location.origin + path;
  navigator.clipboard.writeText(url)
    .then(() => showToast('Copied: ' + url))
    .catch(() => showToast('Copy failed — ' + url));
}

function showToast(msg) {
  const toast = document.getElementById('toast');
  if (!toast) return;
  toast.textContent = msg;
  toast.classList.add('show');
  clearTimeout(toast._tid);
  toast._tid = setTimeout(() => toast.classList.remove('show'), 2400);
}
