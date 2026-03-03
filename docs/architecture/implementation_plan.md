# NMP Receiver Framework — Updated UI + Data Plan

## Delivered in this revision

- Top-level NOC tiles are functional and navigate between:
  - Main topology dashboard
  - Active nodes + interfaces view
  - SNMP alerts operations page
- Link utilization persistence in SQLite (`data/nmp.db`) with timestamped samples.
- History windows for selected link: 1H, 6H, 1D, 1W, 1M, 1Y.
- Program-wide utilization wording standardized to **Transmit and Receive from NODE**.
- LAG labels normalized in UI to `Lag-10` format.
- CSV schema aligned to requested fields.

## Persistence and dedup strategy

- `link_samples` table uses composite primary key `(link_id, ts)` to prevent duplicates.
- `alerts` table stores trap events with status transitions (`open`, `acknowledged`, `cleared`).

## Next integration step

- Replace seeded time-series with real 1-minute SNMP v2c (`noaor`) poller writes for all nodes.
- Populate topology links from live LLDP adjacency instead of static seed definitions.
