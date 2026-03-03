# network-dashboard

NMP dashboard scaffold with topology map, node/interface workflows, and SNMP trap operations view.

## Implemented features

- NOC-style main page with functional top tiles.
- Active Nodes tile opens node list with sortable/filterable/searchable core nodes.
- Node drill-in to interface table with utilization, descriptions, admin/oper state, and error counts.
- Leaflet topology with layer toggles by node class and clickable links/nodes.
- Link utilization history windows: 1H, 6H, 1D, 1W, 1M, 1Y.
- Program utilization shown as **Transmit and Receive from NODE wording**.
- Alerts tile opens SNMP trap dashboard with filtering, row details, acknowledge, and clear actions.
- SQLite persistence under `data/nmp.db` for link history and alerts (non-ephemeral across app updates).

## CSV schema

`nodes_template.csv` columns:

- `node_id`
- `hostname`
- `ip_address`
- `latitude`
- `longitude`
- `environment`
- `node_class`
- `node_type`

## Local run

```bash
pip install -r requirements.txt
uvicorn web.backend.main:app --reload --host 0.0.0.0 --port 8080
```
