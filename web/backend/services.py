from __future__ import annotations

import csv
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from web.backend.models import AlertEvent, DashboardStats, InterfaceStat, Link, LinkHistoryPoint, Node, ProgramUtilization

ROOT = Path(__file__).resolve().parents[2]
CORE_CSV_FILE = ROOT / "nodes_template.csv"
DB_FILE = ROOT / "data" / "nmp.db"

CLASS_LABELS = {
    "core": "Core Backbone",
    "aggregation": "Aggregation",
    "edge": "Edge/Access",
    "cpe": "Customer Premise",
}

LINKS = [
    ("lnk-1", "1", "2", "Lag-10", 100.0),
    ("lnk-2", "2", "3", "Lag-20", 40.0),
    ("lnk-3", "3", "4", "Lag-30", 20.0),
]

_INITIALIZED = False

WINDOW_MAP = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "1d": timedelta(days=1),
    "1w": timedelta(weeks=1),
    "1m": timedelta(days=30),
    "1y": timedelta(days=365),
}


def _coerce_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_nodes() -> list[Node]:
    if not CORE_CSV_FILE.exists():
        return []
    nodes: list[Node] = []
    with CORE_CSV_FILE.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            nodes.append(
                Node(
                    node_id=row["node_id"],
                    hostname=row["hostname"],
                    ip_address=row["ip_address"],
                    latitude=_coerce_float(row["latitude"]),
                    longitude=_coerce_float(row["longitude"]),
                    environment=row["environment"],
                    node_class=row["node_class"],
                    node_type=row["node_type"],
                )
            )
    return nodes


def _conn() -> sqlite3.Connection:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    con = _conn()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS link_samples (
            link_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            tx_util_pct REAL NOT NULL,
            rx_util_pct REAL NOT NULL,
            PRIMARY KEY (link_id, ts)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id INTEGER PRIMARY KEY,
            severity TEXT NOT NULL,
            node_id TEXT NOT NULL,
            hostname TEXT NOT NULL,
            interface_name TEXT NOT NULL,
            trap_oid TEXT NOT NULL,
            trap_description TEXT NOT NULL,
            status TEXT NOT NULL,
            acknowledged_by TEXT,
            cleared_by TEXT,
            last_triggered TEXT NOT NULL,
            raw_payload TEXT NOT NULL
        )
        """
    )

    count = cur.execute("SELECT COUNT(*) c FROM link_samples").fetchone()["c"]
    if count == 0:
        _seed_link_samples(cur)
    acount = cur.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"]
    if acount == 0:
        _seed_alerts(cur)

    con.commit()
    con.close()
    _INITIALIZED = True


def _seed_link_samples(cur: sqlite3.Cursor) -> None:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(days=365)
    t = start
    while t <= now:
        for idx, (lid, _, _, _, _) in enumerate(LINKS):
            # 1-minute resolution recent week; hourly historical to 1 year.
            step_recent = t >= now - timedelta(days=7)
            base = 35 + (idx * 10)
            tx = max(1.0, min(98.0, random.gauss(base, 8)))
            rx = max(1.0, min(98.0, random.gauss(base - 3, 8)))
            cur.execute(
                "INSERT OR IGNORE INTO link_samples(link_id, ts, tx_util_pct, rx_util_pct) VALUES(?,?,?,?)",
                (lid, t.isoformat(), round(tx, 2), round(rx, 2)),
            )
        t += timedelta(minutes=1 if step_recent else 60)


def _seed_alerts(cur: sqlite3.Cursor) -> None:
    now = datetime.now(timezone.utc)
    seed = [
        (1, "critical", "2", "phx-cr1", "xe-0/1/5", "1.3.6.1.6.3.1.1.5.3", "ifDown: Interface operationally down", "open", None, None, now.isoformat(), '{"trap":"ifDown","community":"noaor"}'),
        (2, "warning", "3", "dal-agg1", "ae20", "1.3.6.1.6.3.1.1.5.4", "linkUp flapping detected", "acknowledged", "admin", None, (now - timedelta(minutes=7)).isoformat(), '{"trap":"linkUp"}'),
        (3, "warning", "5", "lab-core1", "xe-0/0/7", "1.3.6.1.6.3.1.1.5.3", "lab ifDown test trap", "open", None, None, (now - timedelta(minutes=2)).isoformat(), '{"trap":"ifDown","lab":true}'),
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO alerts(alert_id,severity,node_id,hostname,interface_name,trap_oid,trap_description,status,acknowledged_by,cleared_by,last_triggered,raw_payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        seed,
    )


def filter_nodes(nodes: list[Node], node_class: str | None, node_type: str | None, hostname: str | None) -> list[Node]:
    out = nodes
    if node_class:
        out = [n for n in out if n.node_class == node_class]
    if node_type:
        out = [n for n in out if n.node_type == node_type]
    if hostname:
        out = [n for n in out if hostname.lower() in n.hostname.lower()]
    return out


def current_links(nodes: list[Node]) -> list[Link]:
    init_db()
    con = _conn()
    cur = con.cursor()
    links: list[Link] = []
    for lid, a, z, lag, cap in LINKS:
        row = cur.execute("SELECT tx_util_pct, rx_util_pct FROM link_samples WHERE link_id=? ORDER BY ts DESC LIMIT 1", (lid,)).fetchone()
        tx = float(row["tx_util_pct"]) if row else 0.0
        rx = float(row["rx_util_pct"]) if row else 0.0
        links.append(Link(link_id=lid, a_node_id=a, z_node_id=z, lag_label=lag, capacity_gbps=cap, tx_util_pct=tx, rx_util_pct=rx))
    con.close()
    node_ids = {n.node_id for n in nodes}
    return [l for l in links if l.a_node_id in node_ids and l.z_node_id in node_ids]


def link_history(link_id: str, window: str) -> list[LinkHistoryPoint]:
    init_db()
    duration = WINDOW_MAP.get(window, WINDOW_MAP["1h"])
    since = datetime.now(timezone.utc) - duration
    con = _conn()
    rows = con.execute(
        "SELECT ts, tx_util_pct, rx_util_pct FROM link_samples WHERE link_id=? AND ts>=? ORDER BY ts ASC",
        (link_id, since.isoformat()),
    ).fetchall()
    con.close()
    return [LinkHistoryPoint(ts=r["ts"], tx_util_pct=float(r["tx_util_pct"]), rx_util_pct=float(r["rx_util_pct"])) for r in rows]


def interfaces_for_node(node: Node, links: list[Link]) -> list[InterfaceStat]:
    stats: list[InterfaceStat] = []
    for l in links:
        if node.node_id in (l.a_node_id, l.z_node_id):
            stats.append(
                InterfaceStat(
                    node_id=node.node_id,
                    hostname=node.hostname,
                    interface_name=f"ae{l.lag_label.split('-')[-1]}",
                    description=f"Core uplink to link {l.link_id}",
                    lag_label=l.lag_label,
                    speed_gbps=l.capacity_gbps,
                    admin_up=True,
                    oper_up=(l.tx_util_pct < 95.0),
                    in_util_pct=l.rx_util_pct,
                    out_util_pct=l.tx_util_pct,
                    error_count=int((l.tx_util_pct + l.rx_util_pct) // 10),
                )
            )
    # add access interfaces for list realism
    for i in range(1, 9):
        stats.append(
            InterfaceStat(
                node_id=node.node_id,
                hostname=node.hostname,
                interface_name=f"xe-0/0/{i}",
                description="Customer/access handoff",
                speed_gbps=10,
                admin_up=(i % 7 != 0),
                oper_up=(i % 6 != 0),
                in_util_pct=round(random.uniform(1, 60), 2),
                out_util_pct=round(random.uniform(1, 60), 2),
                error_count=random.randint(0, 20),
            )
        )
    return stats


def dashboard_stats(nodes: list[Node], links: list[Link]) -> DashboardStats:
    init_db()
    all_if: list[InterfaceStat] = []
    by_id = {n.node_id: n for n in nodes}
    for n in nodes:
        all_if.extend(interfaces_for_node(n, links))
    admin_up = [i for i in all_if if i.admin_up]
    healthy = [i for i in admin_up if i.oper_up]
    health_pct = (len(healthy) / len(admin_up) * 100.0) if admin_up else 100.0
    prog = program_utilization(links)
    total_traffic = (prog.total_tx_gbps + prog.total_rx_gbps) * 3600 / 8  # rough GB in last hour

    con = _conn()
    open_alerts = con.execute("SELECT COUNT(*) c FROM alerts WHERE status != 'cleared'").fetchone()["c"]
    con.close()

    return DashboardStats(
        network_health_pct=round(health_pct, 1),
        active_nodes=len(nodes),
        total_traffic_gb_last_hour=round(total_traffic, 1),
        monitored_nodes=len(nodes),
        interfaces_total=len(all_if),
        alerts_open=int(open_alerts),
    )


def program_utilization(links: list[Link]) -> ProgramUtilization:
    total_capacity = sum(l.capacity_gbps for l in links)
    total_tx = sum(l.capacity_gbps * l.tx_util_pct / 100 for l in links)
    total_rx = sum(l.capacity_gbps * l.rx_util_pct / 100 for l in links)
    avg_tx = (total_tx / total_capacity * 100) if total_capacity else 0
    avg_rx = (total_rx / total_capacity * 100) if total_capacity else 0
    return ProgramUtilization(
        total_capacity_gbps=round(total_capacity, 2),
        total_tx_gbps=round(total_tx, 2),
        total_rx_gbps=round(total_rx, 2),
        avg_tx_util_pct=round(avg_tx, 2),
        avg_rx_util_pct=round(avg_rx, 2),
        accounting_method='One side per link (A-side) — displayed as Transmit and Receive from NODE perspective',
    )


def _node_environment_map() -> dict[str, str]:
    return {n.node_id: n.environment for n in load_nodes()}


def list_alerts(severity: str | None = None, status: str | None = None, include_lab: bool = False) -> list[AlertEvent]:
    init_db()
    con = _conn()
    sql = "SELECT * FROM alerts WHERE 1=1"
    args: list[str] = []
    if severity:
        sql += " AND severity=?"
        args.append(severity)
    if status:
        sql += " AND status=?"
        args.append(status)
    sql += " ORDER BY last_triggered DESC"
    rows = con.execute(sql, args).fetchall()
    con.close()
    env_map = _node_environment_map()
    out = [AlertEvent(**dict(r)) for r in rows]
    if not include_lab:
        out = [a for a in out if env_map.get(a.node_id, 'production') != 'lab']
    return out


def ingest_trap(node_id: str, hostname: str, interface_name: str, trap_oid: str, trap_description: str, severity: str = 'warning', raw_payload: str = '{}') -> AlertEvent:
    """Insert or retrigger same alert variety. Cleared alerts reopen on retrigger."""
    init_db()
    con = _conn()
    now = datetime.now(timezone.utc).isoformat()
    row = con.execute(
        "SELECT * FROM alerts WHERE node_id=? AND interface_name=? AND trap_oid=? ORDER BY last_triggered DESC LIMIT 1",
        (node_id, interface_name, trap_oid),
    ).fetchone()
    if row:
        aid = row['alert_id']
        con.execute(
            "UPDATE alerts SET status='open', acknowledged_by=NULL, cleared_by=NULL, severity=?, trap_description=?, last_triggered=?, raw_payload=? WHERE alert_id=?",
            (severity, trap_description, now, raw_payload, aid),
        )
    else:
        next_id = con.execute("SELECT COALESCE(MAX(alert_id),0)+1 AS n FROM alerts").fetchone()['n']
        con.execute(
            "INSERT INTO alerts(alert_id,severity,node_id,hostname,interface_name,trap_oid,trap_description,status,acknowledged_by,cleared_by,last_triggered,raw_payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (next_id, severity, node_id, hostname, interface_name, trap_oid, trap_description, 'open', None, None, now, raw_payload),
        )
        aid = next_id
    con.commit()
    r2 = con.execute("SELECT * FROM alerts WHERE alert_id=?", (aid,)).fetchone()
    con.close()
    return AlertEvent(**dict(r2))


def update_alert(alert_id: int, action: str, user: str) -> AlertEvent | None:
    init_db()
    con = _conn()
    if action == 'ack':
        con.execute("UPDATE alerts SET status='acknowledged', acknowledged_by=? WHERE alert_id=?", (user, alert_id))
    elif action == 'clear':
        con.execute("UPDATE alerts SET status='cleared', cleared_by=? WHERE alert_id=?", (user, alert_id))
    con.commit()
    row = con.execute("SELECT * FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
    con.close()
    return AlertEvent(**dict(row)) if row else None
