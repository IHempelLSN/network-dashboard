let authToken = '';
let selectedNodeId = null;
let selectedLinkId = null;

const views = {
  main: document.getElementById('view-main'),
  nodes: document.getElementById('view-nodes'),
  alerts: document.getElementById('view-alerts'),
};

const map = L.map('map').setView([39.5, -98.35], 4);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { attribution: '&copy; OSM & CARTO' }).addTo(map);
let layerControl;
const classGroups = {};

async function api(path, opts = {}) {
  const headers = { ...(opts.headers || {}) };
  if (authToken) headers.Authorization = `Bearer ${authToken}`;
  const res = await fetch(path, { ...opts, headers });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function switchView(name) {
  Object.values(views).forEach((v) => v.classList.remove('active'));
  views[name].classList.add('active');
}

function utilColor(pct) {
  if (pct >= 80) return '#ff5c69';
  if (pct >= 70) return '#ff8c42';
  if (pct >= 50) return '#ffcc00';
  return '#34d17b';
}

function renderTiles(stats) {
  const el = document.getElementById('tiles');
  el.innerHTML = `
    <div class="tile" data-view="main"><div class="k">NETWORK HEALTH</div><div class="v">${stats.network_health_pct}%</div><div class="k">Last 24h</div></div>
    <div class="tile" data-view="nodes"><div class="k">ACTIVE NODES</div><div class="v">${stats.active_nodes}</div><div class="k">Click for node list</div></div>
    <div class="tile"><div class="k">TOTAL TRAFFIC</div><div class="v">${stats.total_traffic_gb_last_hour}</div><div class="k">GB last hour</div></div>
    <div class="tile"><div class="k">MONITORED</div><div class="v">${stats.monitored_nodes}</div><div class="k">SNMP Polled</div></div>
    <div class="tile"><div class="k">INTERFACES</div><div class="v">${stats.interfaces_total}</div><div class="k">Total Ports</div></div>
    <div class="tile warn" data-view="alerts"><div class="k">ALERTS</div><div class="v">${stats.alerts_open}</div><div class="k">Requires Attention</div></div>
  `;
  [...el.querySelectorAll('.tile[data-view]')].forEach((t) => t.addEventListener('click', () => switchView(t.dataset.view)));
}

async function renderTopology() {
  const data = await api('/api/topology');
  const details = document.getElementById('selection-details');
  Object.values(classGroups).forEach((g) => g.clearLayers());
  if (layerControl) map.removeControl(layerControl);

  const byId = Object.fromEntries(data.nodes.map((n) => [n.node_id, n]));
  data.nodes.forEach((n) => {
    const className = data.class_labels[n.node_class] || n.node_class;
    if (!classGroups[className]) classGroups[className] = L.layerGroup().addTo(map);
    const marker = L.circleMarker([n.latitude, n.longitude], { radius: 7, color: '#00ffd0', fillColor: '#00a3ff', fillOpacity: 0.8 }).bindPopup(`<b>${n.hostname}</b><br/>${n.ip_address}<br/>${className}`);
    marker.on('click', async () => {
      selectedNodeId = n.node_id;
      const rows = await api(`/api/nodes/${n.node_id}/interfaces`);
      details.innerHTML = `<h4>${n.hostname}</h4><p>${n.ip_address} (${className}/${n.node_type})</p><p>Interfaces: ${rows.length}</p>`;
      renderInterfaceRows(rows);
      switchView('nodes');
    });
    marker.addTo(classGroups[className]);
  });

  data.links.forEach((l) => {
    const a = byId[l.a_node_id]; const z = byId[l.z_node_id];
    if (!a || !z) return;
    const avg = (l.tx_util_pct + l.rx_util_pct) / 2;
    const line = L.polyline([[a.latitude, a.longitude], [z.latitude, z.longitude]], { color: utilColor(avg), weight: 3 + (l.capacity_gbps >= 100 ? 3 : l.capacity_gbps >= 40 ? 2 : 1) }).addTo(map);
    const mid = [(a.latitude + z.latitude) / 2, (a.longitude + z.longitude) / 2];
    L.marker(mid, { icon: L.divIcon({ className: 'link-label', html: `${l.lag_label} ${l.capacity_gbps}G` }) }).addTo(map);
    line.on('click', async () => {
      selectedLinkId = l.link_id;
      details.innerHTML = `<h4>${l.link_id}</h4><p>${l.lag_label} • ${l.capacity_gbps}G</p><p>Transmit from NODE: ${l.tx_util_pct}% | Receive from NODE: ${l.rx_util_pct}%</p>`;
      await renderLinkHistory('1h');
    });
  });

  layerControl = L.control.layers(null, classGroups, { collapsed: false }).addTo(map);
  document.getElementById('class-toggles').textContent = `${data.nodes.length} nodes`; 

  const csel = document.getElementById('node-class');
  const classes = [...new Set(data.nodes.map((n) => n.node_class))];
  csel.innerHTML = '<option value="">All classes</option>' + classes.map((c) => `<option value="${c}">${data.class_labels[c] || c}</option>`).join('');
  renderNodeRows(data.nodes);
}

function renderNodeRows(nodes) {
  const table = document.getElementById('node-table');
  table.innerHTML = nodes.map((n) => `<tr data-node="${n.node_id}"><td>${n.hostname}</td><td>${n.ip_address}</td><td>${n.node_class}</td><td>${n.node_type}</td><td>${n.environment}</td></tr>`).join('');
  [...table.querySelectorAll('tr[data-node]')].forEach((r) => r.addEventListener('click', async () => {
    selectedNodeId = r.dataset.node;
    const rows = await api(`/api/nodes/${selectedNodeId}/interfaces`);
    renderInterfaceRows(rows);
  }));
}

function naturalPortSort(a, b) {
  const ax = a.interface_name.match(/\d+|\D+/g) || [a.interface_name];
  const bx = b.interface_name.match(/\d+|\D+/g) || [b.interface_name];
  const len = Math.max(ax.length, bx.length);
  for (let i = 0; i < len; i++) {
    const av = ax[i] || "";
    const bv = bx[i] || "";
    const an = /^\d+$/.test(av) ? Number(av) : av.toLowerCase();
    const bn = /^\d+$/.test(bv) ? Number(bv) : bv.toLowerCase();
    if (an < bn) return -1;
    if (an > bn) return 1;
  }
  return 0;
}

function renderInterfaceRows(rows) {
  const q = (document.getElementById('if-search').value || '').toLowerCase();
  const table = document.getElementById('if-table');
  const showAdminDown = document.getElementById('show-admin-down').checked;
  const filt = rows
    .filter((r) => showAdminDown || r.admin_up)
    .filter((r) => `${r.interface_name} ${r.description}`.toLowerCase().includes(q))
    .sort(naturalPortSort);
  table.innerHTML = filt.map((r) => `<tr><td>${r.interface_name}</td><td>${r.description}</td><td>${r.lag_label || ''}</td><td>${r.speed_gbps}G</td><td>${r.admin_up ? 'Admin Up' : 'Admin Down'} / ${r.oper_up ? 'Oper Up' : 'Oper Down'}</td><td>${r.out_util_pct}%</td><td>${r.in_util_pct}%</td><td>${r.error_count}</td></tr>`).join('');
}

async function renderLinkHistory(window) {
  if (!selectedLinkId) return;
  const rows = await api(`/api/links/${selectedLinkId}/history?window=${window}`);
  const sampled = rows.length > 30 ? rows.filter((_, idx) => idx % Math.ceil(rows.length / 30) === 0) : rows;
  const out = sampled.map((r) => `${new Date(r.ts).toLocaleString()} | TX ${r.tx_util_pct}% | RX ${r.rx_util_pct}%`).join('\n');
  document.getElementById('history-output').textContent = out || 'No samples';
}

async function renderTopErrors() {
  const nodes = await api('/api/nodes');
  const all = [];
  for (const n of nodes.slice(0, 8)) {
    const rows = await api(`/api/nodes/${n.node_id}/interfaces`);
    all.push(...rows.map((r) => ({ ...r, hostname: n.hostname })));
  }
  const top = all.sort((a, b) => b.error_count - a.error_count).slice(0, 10);
  document.getElementById('top-errors').innerHTML = top.map((i) => `<div class='error-item'><span>${i.hostname} ${i.interface_name}</span><span>${i.error_count}</span></div>`).join('');
}

async function renderAlerts() {
  const severity = document.getElementById('alert-severity').value;
  const status = document.getElementById('alert-status').value;
  const includeLab = document.getElementById('include-lab').checked;
  const q = new URLSearchParams(); if (severity) q.set('severity', severity); if (status) q.set('status', status); if (includeLab) q.set('include_lab', 'true');
  const rows = await api(`/api/alerts${q.toString() ? `?${q}` : ''}`);
  const table = document.getElementById('alerts-table');
  table.innerHTML = rows.map((a) => `<tr data-alert='${a.alert_id}'><td>${a.alert_id}</td><td class='sev-${a.severity}'>${a.severity}</td><td>${a.hostname}</td><td>${a.interface_name}</td><td>${a.trap_description}</td><td>${new Date(a.last_triggered).toLocaleString()}</td><td><span class='badge'>${a.status}</span></td><td><button data-act='ack' data-id='${a.alert_id}'>Ack</button> <button data-act='clear' data-id='${a.alert_id}'>Clear</button></td></tr>`).join('');
  [...table.querySelectorAll('tr[data-alert]')].forEach((tr) => tr.addEventListener('click', () => {
    const id = Number(tr.dataset.alert);
    const obj = rows.find((r) => r.alert_id === id);
    document.getElementById('alert-details').textContent = JSON.stringify(obj, null, 2);
  }));
  [...table.querySelectorAll('button[data-act]')].forEach((b) => b.addEventListener('click', async (e) => {
    e.stopPropagation();
    const id = b.dataset.id; const action = b.dataset.act;
    await api(`/api/alerts/${id}/${action}`, { method: 'POST' });
    await renderAlerts();
  }));
}

async function loginDev() {
  const res = await fetch('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: 'admin', password: 'admin' }) });
  const data = await res.json();
  authToken = data.access_token;
}

document.getElementById('node-filter-apply').addEventListener('click', async () => {
  const p = new URLSearchParams();
  if (document.getElementById('node-search').value) p.set('hostname', document.getElementById('node-search').value);
  if (document.getElementById('node-type').value) p.set('node_type', document.getElementById('node-type').value);
  if (document.getElementById('node-class').value) p.set('node_class', document.getElementById('node-class').value);
  const rows = await api(`/api/nodes${p.toString() ? `?${p}` : ''}`);
  renderNodeRows(rows);
});

document.getElementById('if-search').addEventListener('input', async () => {
  if (!selectedNodeId) return;
  renderInterfaceRows(await api(`/api/nodes/${selectedNodeId}/interfaces`));
});
document.getElementById('show-admin-down').addEventListener('change', async () => {
  if (!selectedNodeId) return;
  renderInterfaceRows(await api(`/api/nodes/${selectedNodeId}/interfaces`));
});
document.getElementById('alert-filter-apply').addEventListener('click', renderAlerts);
[...document.querySelectorAll('.window-buttons button')].forEach((b) => b.addEventListener('click', () => renderLinkHistory(b.dataset.window)));

(async function init() {
  await loginDev();
  const stats = await api('/api/stats');
  renderTiles(stats);
  await renderTopology();
  await renderTopErrors();
  await renderAlerts();
})();
