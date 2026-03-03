# Network Monitoring Platform
## Receiver Framework — Architecture & Design Plan
**Version 1.1 | Phase 1: Receiver Tier**

> **Status:** Draft — For Review
> **Audience:** Development Team
> **Classification:** Confidential — Internal Use Only

---

# Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Scope & Objectives](#2-scope--objectives)
3. [Architecture Decisions](#3-architecture-decisions)
4. [Receiver Microservice Specifications](#4-receiver-microservice-specifications)
5. [SNMP Polling & Node Discovery](#5-snmp-polling--node-discovery)
6. [Web UI & Role-Based Access Control](#6-web-ui--role-based-access-control)
7. [Kafka Message Bus Design](#7-kafka-message-bus-design)
8. [Infrastructure & Docker Swarm Deployment](#8-infrastructure--docker-swarm-deployment)
9. [Vendor Compatibility Matrix](#9-vendor-compatibility-matrix)
10. [Repository Structure](#10-repository-structure)
11. [Implementation Sequence](#11-implementation-sequence)
12. [Open Items](#12-open-items)

---

# 1. Executive Summary

This document defines the architecture and implementation plan for **Phase 1** of the Network Monitoring Platform (NMP): the **Receiver Tier**.

The receiver tier is the ingestion layer responsible for accepting telemetry, event, and flow data from **10,000+ managed network devices** and routing that data into a central message bus for downstream processing.

### Design Priorities

- 🚀 **High throughput** — 10,000–50,000 messages/sec at peak
- 🔧 **Operational simplicity** — easy to run, easy to debug
- 📦 **Horizontal scalability** — via Docker Swarm
- 🆓 **Open source** — no licensing costs

### Golden Rule of the Receiver Tier

> **Receivers are deliberately dumb.** They catch, stamp, and forward. That's it.
> No parsing, no business logic, no alerting — ever. This is what lets them handle 50k msg/sec without choking.

RESTCONF has been deferred to a future phase.

---

# 2. Scope & Objectives

## ✅ In Scope — Phase 1

- Four receiver microservices: **Syslog, SNMP Trap, Netflow/IPFIX, gRPC (Nokia MDT)**
- **Outbound SNMP Polling** microservice for active device interrogation (OID walks, interface stats, etc.)
- **Node Discovery** service for automated detection and onboarding of managed devices
- **Apache Kafka** as the central message bus
- **Docker Swarm** deployment across multiple server nodes
- Kafka topic schema for all raw and parsed data streams
- Vendor-specific receiver config for Nokia 7750 SR, Nokia 7250 IXR, Calix, Adtran/ADVA
- Basic message envelope — timestamp, source IP, receiver ID stamped on every message
- **Web-based UI** accessible to multiple teams, with role-based access control (3 permission levels — see Section 6)

## 🚫 Out of Scope — Deferred

- RESTCONF receiver *(future phase)*
- Event processing, correlation, and alerting *(Phase 2)*
- Dashboard and visualization layer *(Phase 3)*
- Machine learning / anomaly detection *(Phase 4)*

---

# 3. Architecture Decisions

## 3.1 Technology Stack

| Component | Technology | Why |
|---|---|---|
| Language | **Python 3.12+** | Team preference |
| Containerization | **Docker + Swarm** | Scalable, manageable |
| Message Bus | **Apache Kafka** | Handles 50k msg/sec; durable; battle-tested |
| Event Storage | **OpenSearch** | OSS, full-text search for syslog/SNMP events |
| Metric Storage | **InfluxDB** | OSS time-series DB for Netflow & telemetry |
| Dashboards | **Grafana** | Connects to both OpenSearch and InfluxDB |
| Log Enrichment | **Device inventory DB** | Maps source IP → hostname/device role |
| Web UI | **TBD (React or Vue + Python backend)** | Browser-based interface for all teams; RBAC enforced |

## 3.2 Why Separate Microservices?

Each protocol has fundamentally different transport characteristics that make combining them into one service a bad idea:

| Protocol | Why it needs its own service |
|---|---|
| Syslog | UDP + TCP, two RFC formats (3164 and 5424) |
| SNMP Traps | MIB resolution, v2c/v3 auth, PDU decoding |
| Netflow/IPFIX | Binary template-based encoding — **stateful** template cache per exporter |
| Nokia gRPC | Persistent streaming connections, Protobuf + YANG models, proto compilation at build time |

A spike in Netflow will not crash your SNMP receiver. Each service can be scaled, restarted, and deployed independently.

## 3.3 Why Kafka?

- Handles 50,000+ msg/sec on commodity hardware with proper partitioning
- **Durable** — messages survive processor restarts (disk-backed)
- Consumer groups allow multiple processor workers to consume in parallel
- Topic-based routing cleanly separates data by protocol type
- Strong Python client: `confluent-kafka`
- Battle-tested at ISP scale

## 3.4 System Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        DEVICE FLEET                              │
│   Nokia 7750/7250    Calix OLT/ONT    Adtran/ADVA Optical        │
└────────┬──────────────────┬────────────────────┬─────────────────┘
         │ gRPC             │ Syslog/SNMP         │ Syslog/SNMP
         │ :57400           │ :514 / :162         │ :514 / :162
         │             Netflow/IPFIX from all
         │                  │ :2055 / :9995
         ▼                  ▼                     ▼
┌──────────────────────────────────────────────────────────────────┐
│                       RECEIVER TIER                              │
│  ┌────────────────┐ ┌──────────────┐ ┌─────────────────────────┐│
│  │ grpc-receiver  │ │syslog-       │ │ snmp-receiver           ││
│  │ TCP :57400     │ │receiver      │ │ UDP :162                ││
│  │ Nokia MDT      │ │ UDP/TCP :514 │ │ v2c + v3                ││
│  └───────┬────────┘ └──────┬───────┘ └───────────┬─────────────┘│
│          │                 │                      │              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              netflow-receiver  UDP :2055/:9995            │  │
│  └───────────────────────────────────┬───────────────────────┘  │
└──────────────────────────────────────┼───────────────────────────┘
                                       │ Kafka publish
                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                        APACHE KAFKA                              │
│                                                                  │
│   raw.syslog   raw.snmp   raw.netflow   raw.telemetry           │
│   parsed.events           parsed.metrics           alerts        │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                    ┌───────────┴──────────┐
                    ▼                      ▼
          ┌─────────────────┐   ┌──────────────────┐
          │   OpenSearch    │   │    InfluxDB        │
          │  (events/logs)  │   │  (metrics/flows)  │
          └────────┬────────┘   └────────┬───────────┘
                   └──────────┬──────────┘
                              ▼
                         ┌─────────┐
                         │ Grafana │
                         └─────────┘
```

---

# 4. Receiver Microservice Specifications

## 4.1 Receiver Summary

| Microservice | Port | Source Devices | Replicas | Kafka Topic |
|---|---|---|---|---|
| `syslog-receiver` | UDP/TCP `:514` | Calix, Adtran/ADVA | 1–3 | `raw.syslog` |
| `snmp-receiver` | UDP `:162` | Nokia, Calix, Adtran/ADVA | 1–2 | `raw.snmp` |
| `netflow-receiver` | UDP `:2055` / `:9995` | All flow-capable devices | 1–2 | `raw.netflow` |
| `grpc-receiver` | TCP `:57400` | Nokia 7750 SR, 7250 IXR | 1–2 | `raw.telemetry` |

---

## 4.2 Common Message Envelope

Every message published to Kafka — regardless of protocol — gets wrapped in this JSON envelope. Downstream processors get consistent metadata on every single message.

```json
{
  "envelope": {
    "receiver_id":    "syslog-receiver-01",
    "receiver_type":  "syslog",
    "received_at":    "2025-03-02T14:23:01.482Z",
    "source_ip":      "10.10.5.44",
    "source_port":    52341,
    "protocol":       "UDP",
    "schema_version": "1.0"
  },
  "payload": "<raw message content — format varies by receiver type>"
}
```

> **Rule:** The receiver stamps the envelope and publishes. It does NOT parse `payload`. That's the processor's job.

---

## 4.3 Syslog Receiver

### Protocols
- **UDP port 514** — primary, best-effort, high volume
- **TCP port 514** — optional, for devices configured for reliable delivery
- Supports **RFC 3164** (legacy BSD) and **RFC 5424** (modern structured syslog)

### Source Devices
- Calix (ONTs, OLTs) — syslog only
- Adtran/ADVA optical transport — syslog + SNMP

### Implementation Notes

- Use `asyncio.DatagramProtocol` for UDP (non-blocking, high throughput)
- For TCP, use `asyncio.start_server` with per-connection coroutines
- Maintain an internal `asyncio.Queue` between socket receipt and Kafka publish to buffer bursts
- **Do NOT parse syslog content** — publish raw bytes in envelope
- **Batch Kafka publishes:** accumulate up to 500 messages OR 100ms — whichever comes first
- Log a counter metric every 60s: messages received, messages published, queue depth

### Environment Variables

```bash
SYSLOG_UDP_PORT=514
SYSLOG_TCP_PORT=514
SYSLOG_TCP_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_RAW=raw.syslog
KAFKA_BATCH_SIZE=500
KAFKA_BATCH_TIMEOUT_MS=100
RECEIVER_ID=syslog-receiver-01
```

### Python Libraries
```
asyncio (stdlib)
confluent-kafka
```

---

## 4.4 SNMP Trap Receiver

### Protocols
- **UDP port 162** — standard SNMP trap port
- **SNMPv2c** (primary) and **SNMPv3** (required for Nokia)
- MIB resolution happens in the **processor, NOT here**

### Source Devices
- Nokia 7750 SR / 7250 IXR — SNMPv3 with authentication
- Calix — SNMPv2c with community strings
- Adtran/ADVA — SNMPv2c; ADVA enterprise OIDs need MIBs loaded at processor stage

### Implementation Notes

- Use `pysnmp` (async-capable) for trap receipt and basic PDU decoding
- Receiver decodes enough to extract: OID, source IP, community/credentials, varbinds
- **Full MIB resolution (OID → human-readable name) is the processor's job**
- SNMPv3 engine IDs and credentials stored as a config map, loaded at startup
- Community strings in environment variables — **never hardcoded**
- Publish raw PDU data + decoded varbinds (as key-value pairs) in envelope

### Environment Variables

```bash
SNMP_PORT=162
SNMP_V2C_COMMUNITY=public
SNMP_V3_ENABLED=true
SNMP_V3_USERNAME=nmsuser
SNMP_V3_AUTH_PROTOCOL=SHA
SNMP_V3_AUTH_KEY=<secret>
SNMP_V3_PRIV_PROTOCOL=AES
SNMP_V3_PRIV_KEY=<secret>
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_RAW=raw.snmp
RECEIVER_ID=snmp-receiver-01
```

### Python Libraries
```
pysnmp
confluent-kafka
```

---

## 4.5 Netflow / IPFIX Receiver

### Protocols
- **UDP port 2055** — Netflow v5/v9 (legacy)
- **UDP port 9995** — IPFIX (preferred for newer devices)
- Netflow v9 and IPFIX use **templates** — receiver must maintain a template cache per source IP

### Implementation Notes

- Use `python-netflow` or `nfstream` as the base; wrap in asyncio
- ⚠️ **CRITICAL: Template caching is stateful.** The receiver must cache flow templates per exporter IP. This is the only stateful part of the receiver tier.
- If a template is missing (flow record arrives before its template), hold the record in a short buffer (30s) and retry
- Flows are high volume — use **larger Kafka batch sizes** (1000 msgs, 200ms timeout)
- UDP only — loss is acceptable per requirements; no TCP fallback needed

### Environment Variables

```bash
NETFLOW_PORT=2055
IPFIX_PORT=9995
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_RAW=raw.netflow
KAFKA_BATCH_SIZE=1000
KAFKA_BATCH_TIMEOUT_MS=200
TEMPLATE_CACHE_TTL_SECONDS=3600
RECEIVER_ID=netflow-receiver-01
```

### Python Libraries
```
python-netflow  (or nfstream)
confluent-kafka
```

---

## 4.6 gRPC Receiver (Nokia MDT)

### ⚠️ Heads Up — This One Is Different

The gRPC receiver is **significantly more complex** than the other three. Read this section carefully before you start.

### Protocols
- **TCP port 57400** — Nokia Model-Driven Telemetry (MDT) over gRPC
- Nokia **7750 SR-OS** and **7250 IXR** in **gRPC dial-out mode**
- Protobuf encoding with Nokia SR-OS YANG model schemas

### Things the Dev Team Must Know Before Writing a Single Line

- Nokia SR-OS uses its own YANG data models. You need to **obtain the `.proto` files from Nokia** and compile them to Python Protobuf stubs using `protoc` — this happens at Docker **build time**
- The 7750 SR and 7250 IXR **may have different proto definitions** — get both and test both
- gRPC uses **persistent streaming connections** — the receiver must handle full connection lifecycle: connect, stream close, and reconnect with exponential backoff
- **Dial-out means Nokia initiates the connection** to your receiver. Make sure firewall rules allow inbound TCP 57400 to receiver nodes from Nokia device management IPs
- Use `grpc.aio` (asyncio-native gRPC) — **not** the synchronous gRPC client

### Build-Time Proto Compilation (goes in Dockerfile)

```dockerfile
# In Dockerfile for grpc-receiver:
COPY nokia_sr_os.proto /proto/
COPY nokia_ixr.proto   /proto/

RUN python -m grpc_tools.protoc \
    -I/proto \
    --python_out=/app/generated \
    --grpc_python_out=/app/generated \
    /proto/nokia_sr_os.proto \
    /proto/nokia_ixr.proto
```

> The `/app/generated` directory should be **gitignored** — it's built at image build time, not committed.

### Implementation Notes

- Use `grpc.aio` (asyncio-native) — never the synchronous gRPC client
- Implement a gRPC servicer class that maps to the Nokia telemetry service definition
- Decode each Protobuf message and wrap in the standard JSON envelope
- Include the YANG path and Nokia device model in the envelope metadata (processor needs this)
- Implement a **gRPC health check endpoint** (`grpc.health.v1`) for Docker Swarm health checks

### Nokia Device-Side Config (reference)

This is what gets configured on the Nokia routers to make them dial out to your receiver. Actual subscription paths (what telemetry data to stream) will be defined in Phase 2.

```
# Nokia SR-OS — gRPC dial-out telemetry (reference CLI)
/configure system telemetry
    persistent-subscriptions
        subscription "to-nmp"
            description "NMP gRPC telemetry"
            destination-group "nmp-receivers"
            sensor-group "base-metrics"
        exit
    exit
    destination-group "nmp-receivers"
        destination <receiver-ip> port 57400
            protocol grpc
            encoding gpb
        exit
    exit
exit
```

### Environment Variables

```bash
GRPC_PORT=57400
GRPC_MAX_CONCURRENT_STREAMS=500
GRPC_KEEPALIVE_TIME_MS=30000
GRPC_KEEPALIVE_TIMEOUT_MS=10000
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_RAW=raw.telemetry
RECEIVER_ID=grpc-receiver-01
```

### Python Libraries
```
grpcio
grpcio-tools      (for proto compilation)
grpcio-health-checking
protobuf
confluent-kafka
```

---

# 5. SNMP Polling & Node Discovery

## 5.1 Overview

In addition to passively receiving inbound traps and telemetry, the NMP will **actively poll devices via SNMP** to collect metrics, interface statistics, and operational state data on a scheduled basis. A companion **Node Discovery** service will automate the detection and onboarding of new devices into the managed inventory.

> **Design Note:** Polling is active/outbound — the NMP initiates these queries. This is distinct from the passive receiver tier, which only listens for inbound data. Poll results are published to Kafka like everything else, maintaining the same pipeline architecture.

---

## 5.2 SNMP Polling Microservice (`snmp-poller`)

### Responsibilities

- Execute scheduled SNMP GET / GETBULK / WALK requests against the managed device inventory
- Support **SNMPv2c** (community string) and **SNMPv3** (auth + priv) per device
- Publish poll results into Kafka topic `raw.snmp.poll` using the standard message envelope
- Support configurable polling intervals per device group or individual device (e.g., 1-min for interfaces, 5-min for system stats)
- Handle timeouts and retry logic gracefully — a non-responsive device should never block the polling queue
- Maintain a lightweight poll queue with per-device scheduling state

### Key OID Categories to Poll (initial set — expand in Phase 2)

| Category | OID Base | Notes |
|---|---|---|
| Interface stats | `IF-MIB::ifTable` / `ifXTable` | In/out octets, errors, discards, status |
| System info | `SNMPv2-MIB::sysDescr`, `sysUpTime` | Basic device health |
| CPU / Memory | Vendor-specific | Nokia, Calix, Adtran MIBs differ |
| BGP / OSPF state | `BGP4-MIB`, `OSPF-MIB` | Routing protocol health *(Phase 2)* |

### Implementation Notes

- Use `pysnmp` (async) or `easysnmp` as the SNMP library
- Device inventory (IP, credentials, poll interval, OID profile) loaded from the central device DB
- Use `asyncio` with a semaphore-controlled worker pool — limit concurrent outstanding polls to avoid flooding devices
- Publish poll results to Kafka with `receiver_type: "snmp-poll"` in the envelope
- Log per-device poll latency and success/failure counters for operational visibility

### Environment Variables

```bash
POLLER_WORKER_CONCURRENCY=50
POLLER_DEFAULT_INTERVAL_SECONDS=60
SNMP_V2C_COMMUNITY=public
SNMP_V3_USERNAME=nmsuser
SNMP_V3_AUTH_PROTOCOL=SHA
SNMP_V3_AUTH_KEY=<secret>
SNMP_V3_PRIV_PROTOCOL=AES
SNMP_V3_PRIV_KEY=<secret>
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_POLL=raw.snmp.poll
DEVICE_INVENTORY_DB_URL=<connection string>
RECEIVER_ID=snmp-poller-01
```

### Python Libraries
```
pysnmp (or easysnmp)
confluent-kafka
asyncio (stdlib)
apscheduler  (or custom asyncio scheduler)
```

---

## 5.3 Node Discovery Service (`node-discovery`)

### Responsibilities

- Proactively scan defined IP ranges or subnets for new SNMP-responsive devices
- Attempt SNMP GET against candidate IPs using the configured community strings / v3 credentials
- On successful response: extract `sysDescr`, `sysName`, `sysLocation`, `sysContact`, and vendor OID
- Classify the device by vendor (Nokia, Calix, Adtran/ADVA, unknown) based on the enterprise OID
- Register newly discovered devices into the central device inventory DB with a `pending_review` status
- Optionally notify via the alerts pipeline so operators can review and approve new devices in the Web UI
- Re-scan on a configurable schedule (e.g., nightly) to catch newly provisioned devices

### Discovery Flow

```
Define scan ranges (CIDR blocks)
        │
        ▼
SNMP probe each IP (GET sysDescr, sysObjectID)
        │
   Responds?
   ├── No  → skip, log as unreachable
   └── Yes → extract sysDescr / sysObjectID
                  │
                  ▼
         Classify vendor + device type
                  │
                  ▼
         Already in inventory?
         ├── Yes → skip (or update last-seen timestamp)
         └── No  → insert with status = 'pending_review'
                       │
                       ▼
                 Publish discovery event → Kafka `events.discovery`
                       │
                       ▼
                 Alert sent to Web UI for operator review
```

### Implementation Notes

- Scan ranges defined via config file or Web UI (Admin only — see Section 6)
- Rate-limit probes to avoid triggering IDS/IPS — configurable max probes/sec
- Discovery results feed directly into the device inventory DB; this DB is the source of truth for the polling service

### Environment Variables

```bash
DISCOVERY_SCAN_SCHEDULE_CRON="0 2 * * *"   # nightly at 02:00
DISCOVERY_PROBE_RATE_PER_SEC=20
DISCOVERY_SCAN_RANGES=10.10.0.0/16,10.20.0.0/16
SNMP_V2C_COMMUNITIES=public,private
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_DISCOVERY=events.discovery
DEVICE_INVENTORY_DB_URL=<connection string>
```

---

# 6. Web UI & Role-Based Access Control (RBAC)

## 6.1 Overview

The NMP is a **web-based application** accessible via a standard browser. There is no fat client. Multiple teams will access the platform simultaneously — NOC, engineering, and platform admins — each with a different scope of what they should be able to view and change.

Access is enforced through **three permission levels**. Every action in the UI is gated against the authenticated user's role. The backend API must enforce these checks server-side — the UI restriction alone is not sufficient.

---

## 6.2 Permission Levels

### Level 1 — Read Only (`role: viewer`)

**Who:** Stakeholders, reporting users, teams who need visibility but must not touch anything.

**Can do:**
- View all dashboards, event feeds, alert lists, device inventory, and topology maps
- View current alert status (acknowledged, active, cleared)
- Export data / reports

**Cannot do:**
- Acknowledge, assign, or clear alerts
- Modify any device, configuration, or system setting
- Trigger any action (manual polls, discoveries, etc.)

---

### Level 2 — Operator (`role: operator`)

**Who:** NOC staff and first-line engineers who work alerts day-to-day.

**Can do everything in Level 1, plus:**
- **Acknowledge alerts** — mark that a human has seen and is working an alert
- **Clear alerts** — mark an alert as resolved
- **Add notes / comments** to alerts and events
- Minor device record updates (e.g., update a device description or location field)
- Trigger an **on-demand SNMP poll** for a specific device
- Suppress/snooze alerts for a defined maintenance window

**Cannot do:**
- Change global system configuration
- Add/remove devices from inventory
- Modify polling profiles, OID assignments, or discovery scan ranges
- Manage users or change permission levels
- Modify Kafka topics, receiver config, or any infrastructure-level settings

---

### Level 3 — Administrator (`role: admin`) — "God Mode"

**Who:** Platform owners and senior engineers who build and maintain the NMP itself.

**Can do everything in Levels 1 and 2, plus:**
- Full control over **device inventory** — add, edit, decommission devices
- Define and edit **SNMP polling profiles** (intervals, OID sets, credential assignment)
- Configure **node discovery scan ranges** and schedules
- Manage **alert rules** — thresholds, correlation logic, escalation paths
- **User management** — create accounts, assign roles, revoke access
- Modify **receiver configuration** without redeploying containers (where config is externalized)
- Access **system health dashboards** for NMP infrastructure itself (Kafka lag, receiver throughput, poller queue depth)
- Perform **bulk operations** (mass-onboard devices, bulk alert clear, etc.)
- Full access to **audit logs** — who did what and when

---

## 6.3 Authentication

- All users must authenticate before accessing any part of the UI
- Authentication method TBD — recommend **LDAP/AD integration** or OIDC (SSO) to avoid managing local credentials
- Sessions expire after a configurable idle timeout
- All actions by Level 2 and Level 3 users are **audit logged**: timestamp, user, action, target object

---

## 6.4 UI Technology Decisions (TBD)

Technology stack for the Web UI frontend and backend API is not finalized. Requirements:

- Browser-based, no client install required
- RESTful or GraphQL API backend (Python preferred, consistent with rest of stack)
- RBAC enforced at the API layer — frontend role enforcement is cosmetic only
- Responsive enough for NOC operators working on standard 1080p monitors

Recommended starting point: **FastAPI** (Python backend) + **React** or **Vue** (frontend). Final decision deferred to Phase 3 design.

> **Open Item:** Add to Section 12 — Web UI framework selection, authentication/SSO integration approach.

---

# 7. Kafka Message Bus Design

## 7.1 Topic Schema

| Topic | Partitions | Retention | Producer |
|---|---|---|---|
| `raw.syslog` | 3 | 24 hours | `syslog-receiver` |
| `raw.snmp` | 3 | 24 hours | `snmp-receiver` |
| `raw.snmp.poll` | 3 | 24 hours | `snmp-poller` |
| `raw.netflow` | 6 | 6 hours | `netflow-receiver` |
| `raw.telemetry` | 3 | 24 hours | `grpc-receiver` |
| `events.discovery` | 2 | 7 days | `node-discovery` |
| `parsed.events` | 3 | 7 days | Event processor |
| `parsed.metrics` | 6 | 2 days | Metric processor |
| `alerts` | 3 | 30 days | Alert engine |

## 7.2 Partition Keys

Partition keys keep messages from the same device in order and allow parallel processing:

| Topic | Partition Key | Why |
|---|---|---|
| `raw.syslog` | Source IP address | Groups all messages from a device together |
| `raw.snmp` | Source IP address | Same reason |
| `raw.snmp.poll` | Target device IP | Groups all poll results from a device together |
| `raw.netflow` | Exporter IP | Groups flows from same router |
| `raw.telemetry` | Nokia device hostname | From gRPC metadata |
| `events.discovery` | Source IP | Groups discovery events by device |

## 7.3 Kafka Cluster Config

- **3-node Kafka cluster** — minimum for fault tolerance in production
- **Replication factor: 3** for all topics
- **Use KRaft mode** (Kafka 3.x+) — eliminates ZooKeeper dependency, simpler to operate
- Enable **log compaction** on `parsed.events` for deduplication
- **JVM heap:** 6GB per broker node minimum
- **Dedicated SSD storage** for Kafka log directories

---

# 8. Infrastructure & Docker Swarm Deployment

## 8.1 Node Layout

| Node Role | Count | Min Spec | What Runs There |
|---|---|---|---|
| Receiver Nodes | 2–3 | 8 core / 16GB RAM | All 4 receiver microservices, load balanced |
| Poller / Discovery Nodes | 1–2 | 8 core / 16GB RAM | `snmp-poller`, `node-discovery` |
| Kafka Cluster | 3 | 16 core / 32GB RAM / SSD | Kafka brokers (KRaft mode) |
| Processor Nodes | 2–3 | 16 core / 32GB RAM | Event, metric, alert processor workers |
| Storage Nodes | 2–3 | 16 core / 64GB RAM / SSD | OpenSearch cluster + InfluxDB cluster |
| Management Node | 1 | 4 core / 8GB RAM | Grafana, Web UI app, Swarm manager, monitoring |

## 8.2 Docker Stack Deploy (excerpt)

This is a representative extract from `docker-compose.yml`. Full compose file is a separate deliverable.

```yaml
version: '3.8'

services:

  syslog-receiver:
    image: nmp/syslog-receiver:1.0
    ports:
      - "514:514/udp"
      - "514:514/tcp"
    deploy:
      replicas: 2
      placement:
        constraints:
          - node.role == worker
          - node.labels.tier == receiver
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
      - KAFKA_TOPIC_RAW=raw.syslog
    networks:
      - nmp-net

  snmp-receiver:
    image: nmp/snmp-receiver:1.0
    ports:
      - "162:162/udp"
    deploy:
      replicas: 2
      placement:
        constraints:
          - node.role == worker
          - node.labels.tier == receiver
    networks:
      - nmp-net

  netflow-receiver:
    image: nmp/netflow-receiver:1.0
    ports:
      - "2055:2055/udp"
      - "9995:9995/udp"
    deploy:
      replicas: 2
      placement:
        constraints:
          - node.role == worker
          - node.labels.tier == receiver
    networks:
      - nmp-net

  grpc-receiver:
    image: nmp/grpc-receiver:1.0
    ports:
      - "57400:57400/tcp"
    deploy:
      replicas: 2
      placement:
        constraints:
          - node.role == worker
          - node.labels.tier == receiver
    networks:
      - nmp-net

networks:
  nmp-net:
    driver: overlay
    attachable: true
```

## 8.3 Firewall Rules

| Port | Protocol | Direction | Source | Purpose |
|---|---|---|---|---|
| 514 | UDP + TCP | Inbound → receiver nodes | All managed device subnets | Syslog |
| 162 | UDP | Inbound → receiver nodes | All managed device subnets | SNMP Traps |
| 161 | UDP | **Outbound → managed devices** | Poller nodes | SNMP Polling (active queries) |
| 2055, 9995 | UDP | Inbound → receiver nodes | Flow-exporting device subnets | Netflow/IPFIX |
| 57400 | TCP | Inbound → receiver nodes | Nokia device management IPs **only** | gRPC MDT |
| 80 / 443 | TCP | Inbound → management node | Internal team networks | Web UI (HTTP/HTTPS) |
| 9092 | TCP | Internal only | Swarm overlay network | Kafka broker |
| 2377 | TCP | Internal | Swarm nodes | Swarm manager |
| 7946 | TCP + UDP | Internal | Swarm nodes | Swarm gossip |
| 4789 | UDP | Internal | Swarm nodes | Overlay network |

---

# 9. Vendor Compatibility Matrix

| Vendor | Protocol | Receiver / Service | Notes |
|---|---|---|---|
| Nokia 7750 SR | gRPC dial-out (MDT) | `grpc-receiver :57400` | SR-OS YANG models; proto compilation required at build time |
| Nokia 7250 IXR | gRPC dial-out (MDT) | `grpc-receiver :57400` | May need separate proto definitions from 7750 — verify with Nokia |
| Nokia 7750 SR / 7250 IXR | SNMP Polling (outbound) | `snmp-poller` | SNMPv3 required; Nokia enterprise MIBs needed for CPU/memory OIDs |
| Calix | Syslog | `syslog-receiver :514` | Standard RFC 3164/5424; tag parsing for ONT events in processor |
| Calix | SNMP Traps | `snmp-receiver :162` | Standard MIB + Calix enterprise OIDs |
| Calix | SNMP Polling (outbound) | `snmp-poller` | SNMPv2c; Calix enterprise MIBs for ONT/OLT-specific stats |
| Adtran / ADVA | Syslog | `syslog-receiver :514` | Optical-specific severity mapping handled in processor |
| Adtran / ADVA | SNMP Traps | `snmp-receiver :162` | ADVA enterprise MIBs must be loaded in processor MIB store |
| Adtran / ADVA | SNMP Polling (outbound) | `snmp-poller` | SNMPv2c; confirm ADVA enterprise OIDs for optical layer metrics |

---

# 10. Repository Structure

```
nmp/
├── receivers/
│   ├── syslog/
│   │   ├── Dockerfile
│   │   ├── main.py               # entrypoint
│   │   ├── receiver.py           # asyncio UDP/TCP listener
│   │   ├── kafka_publisher.py    # batching + Kafka publish
│   │   ├── envelope.py           # builds the standard envelope
│   │   ├── config.py             # reads env vars
│   │   ├── requirements.txt
│   │   └── tests/
│   │
│   ├── snmp/
│   │   └── (same structure as syslog)
│   │
│   ├── netflow/
│   │   ├── (same base structure)
│   │   └── template_cache.py     # stateful template store — unique to netflow
│   │
│   └── grpc/
│       ├── Dockerfile             # includes proto compilation step — see 4.6
│       ├── proto/                 # Nokia .proto files (obtain from Nokia)
│       ├── generated/             # compiled Python stubs — GITIGNORE THIS
│       ├── servicer.py            # gRPC servicer implementation
│       ├── main.py
│       ├── kafka_publisher.py
│       ├── envelope.py
│       ├── config.py
│       └── requirements.txt
│
├── poller/
│   ├── snmp-poller/
│   │   ├── Dockerfile
│   │   ├── main.py               # entrypoint + scheduler loop
│   │   ├── poller.py             # async SNMP GET/GETBULK/WALK logic
│   │   ├── poll_queue.py         # per-device scheduling state
│   │   ├── kafka_publisher.py
│   │   ├── envelope.py
│   │   ├── config.py
│   │   ├── requirements.txt
│   │   └── tests/
│   │
│   └── node-discovery/
│       ├── Dockerfile
│       ├── main.py               # entrypoint + discovery scheduler
│       ├── scanner.py            # subnet scan + SNMP probe logic
│       ├── classifier.py         # vendor/device type from sysObjectID
│       ├── inventory_client.py   # writes new devices to device DB
│       ├── kafka_publisher.py
│       ├── config.py
│       ├── requirements.txt
│       └── tests/
│
├── web/
│   ├── backend/                  # FastAPI (or equivalent) REST API
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── auth/                 # authentication + session management
│   │   ├── rbac/                 # role enforcement middleware
│   │   ├── routers/              # API route definitions
│   │   ├── models/               # data models
│   │   ├── requirements.txt
│   │   └── tests/
│   │
│   └── frontend/                 # React/Vue SPA (TBD)
│       ├── src/
│       ├── public/
│       └── package.json
│
├── shared/
│   ├── envelope.py               # shared envelope builder (imported by all receivers)
│   ├── kafka_client.py           # shared Kafka producer wrapper
│   └── health.py                 # HTTP health check endpoints for Swarm
│
├── infra/
│   ├── docker-compose.yml        # full stack definition
│   ├── kafka/
│   │   ├── create-topics.sh      # topic init script
│   │   └── server.properties     # Kafka broker config
│   └── swarm/
│       └── label-nodes.sh        # assigns tier labels to swarm nodes
│
└── docs/
    └── architecture/
        └── NMP_Receiver_Framework_Design_v1.1.md   # this document
```

---

# 11. Implementation Sequence

Build in this order. Each step depends on the previous one.

### Step 1 — Kafka + Shared Library *(do this first)*
Stand up the 3-node Kafka cluster in Docker Swarm. Create all topics (including `raw.snmp.poll` and `events.discovery`). Write and unit test `shared/envelope.py` and `shared/kafka_client.py`. Everything else imports from here.

### Step 2 — Syslog Receiver
Simplest protocol. Use this to validate the full pipeline: device → receiver → Kafka → consumer. Test with real Calix syslog output before moving on.

### Step 3 — SNMP Trap Receiver
Build on the syslog pattern. Add `pysnmp`, SNMPv3 config. Test with Calix and Adtran trap generators. Confirm varbinds are landing correctly in Kafka.

### Step 4 — Netflow / IPFIX Receiver
Introduce the template cache (the only new stateful concept). Test with a flow generator tool like `nflow-generator` **before** connecting live devices.

### Step 5 — gRPC Receiver (Nokia MDT) *(start Nokia conversation now)*
Most complex. You **cannot start coding this until you have the Nokia `.proto` files.** Contact Nokia TAC or your account team in parallel with Steps 1–4 to get YANG/proto definitions for both the 7750 SR and 7250 IXR.

### Step 6 — SNMP Poller
Build the `snmp-poller` service. Start with a small set of standard OIDs (IF-MIB, sysUpTime). Validate poll results flowing into `raw.snmp.poll`. Confirm per-device scheduling and concurrency controls work before expanding OID coverage.

### Step 7 — Node Discovery
Build the `node-discovery` scanner. Test against a lab subnet. Confirm new devices land in the inventory DB with `pending_review` status and a discovery event appears in Kafka.

### Step 8 — Web UI (Phase 3)
Web UI design and build is a Phase 3 deliverable. RBAC model is defined here (Section 6) so the backend data models and API contract can be planned early. Do not block Phase 1 on this.

---

# 12. Open Items

These need answers before or during Phase 2 design. Flag them early.

| # | Question | Who Owns It | Blocks |
|---|---|---|---|
| 1 | What Nokia YANG telemetry paths/subscriptions do we want to subscribe to? | Network team | gRPC receiver Nokia device config |
| 2 | Does Adtran/ADVA use any proprietary syslog extensions beyond RFC 3164/5424? | Vendor / Lab test | Processor parsing rules |
| 3 | What is the device inventory / CMDB system? | Ops team | Processor enrichment (IP → hostname), poller device DB |
| 4 | Kafka auth model — SASL/PLAIN or mTLS between receivers and brokers? | Security team | Kafka cluster config |
| 5 | UDP load balancing — HAProxy in front of receivers, or DNS round-robin? | Infra team | Receiver scaling design |
| 6 | What monitors the NMP itself? (Prometheus + Alertmanager recommended) | Infra team | Operational readiness |
| 7 | What enterprise MIBs need to be loaded for Nokia, Calix, and ADVA polling? | Network team | SNMP poller OID profile design |
| 8 | What are the initial SNMP polling intervals and OID sets per device class? | Network team | `snmp-poller` config and poll profile design |
| 9 | What IP ranges/CIDR blocks should node discovery scan? | Network team | `node-discovery` scan range config |
| 10 | Web UI authentication method — LDAP/AD integration or OIDC/SSO? | IT / Security team | Web UI auth design, user onboarding |
| 11 | Web UI framework selection — FastAPI + React, or alternative? | Dev team | Phase 3 Web UI build |
| 12 | Should newly discovered devices auto-enroll into polling, or require Admin approval? | Ops / Network team | `node-discovery` → inventory → poller workflow |

---

*This is a living document. It will be updated as open items are resolved. Phase 2 (Processor Tier) design begins once Phase 1 receivers are validated in staging. Web UI (Phase 3) design begins in parallel once the RBAC model and API contract are agreed upon.*
