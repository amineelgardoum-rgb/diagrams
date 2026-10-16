# Logs Observability Platform

A self-hosted, container-native **logs observability platform** built on the Grafana stack, with
S3-compatible object storage and an Airflow control plane on top.

**Docker containers → Grafana Alloy → Grafana Loki → MinIO (S3) → Grafana**

![Architecture](<Untitled Diagram.gif>)

---

## Table of contents

- [What this is](#what-this-is)
- [Architecture at a glance](#architecture-at-a-glance)
- [How it works, step by step](#how-it-works-step-by-step)
- [Components](#components)
- [Repository layout](#repository-layout)
- [Quick start](#quick-start)
- [Deploy to a VM & onboard developers](#deploy-to-a-vm--onboard-developers)
- [Ports and credentials](#ports-and-credentials)
- [LogQL cookbook](#logql-cookbook)
- [Alerting](#alerting)
- [The Airflow control plane](#the-airflow-control-plane)
- [Configuration reference](#configuration-reference)
- [Day-2 operations](#day-2-operations)
- [Troubleshooting](#troubleshooting)
- [Regenerating the diagrams](#regenerating-the-diagrams)
- [Recommended architecture (target state)](#recommended-architecture-target-state)
- [Status and roadmap](#status-and-roadmap)
- [Security notes](#security-notes)
- [License](#license)

---

## What this is

Instead of installing a logging agent on every machine and running an ELK cluster, this project
gives you a **single-host, labels-first log platform** out of a couple of Compose files:

| You get | How |
| --- | --- |
| Automatic log collection for **every** container | Alloy reads `/var/run/docker.sock` (read-only) and discovers containers itself — nothing to configure per container |
| Structured, queryable logs | Alloy parses JSON / logfmt / plain text and extracts a normalised `level` label |
| Cheap long-term storage | Loki stores chunks + index in **MinIO (S3)**, not on the local filesystem |
| Search & dashboards | Grafana with a pre-provisioned Loki data source |
| Alerting | Provisioned Grafana alert rules on `level=ERROR` / `level=WARN`, emails caught by **Mailpit** |
| Ops automation | Apache **Airflow**: 5-minute health checks + restarts, and a daily Parquet archive of the previous day's logs into S3 |
| A demo log source | `error-gen` spits a `level=error` line every 5 seconds so you always have data |

---

## Architecture at a glance

```
                    ┌────────────────────── Docker network: monitoring ──────────────────────┐
                    │                                                                        │
 container stdout   │   ┌───────────┐   push /loki/api/v1/push     ┌────────┐                │
 ──────────────────►│   │  Alloy    │ ────────────────────────────► │  Loki  │                │
 (error-gen, grafana,│  │  :12345   │                              │  :3100 │                │
  loki, minio, ...) │   │ discover  │   discovery.docker            └───┬────┘                │
 docker.sock (ro) ─►│   │ parse     │   via /var/run/docker.sock        │ S3 (chunks+index)   │
                    │   │ normalise │                                   ▼                     │
                    │   └───────────┘                           ┌──────────────┐              │
                    │                                           │ MinIO :9000  │              │
 admin ────────────►│   ┌───────────┐   LogQL                   │ buckets:     │              │
 :3000 ────────────►│   │ Grafana   │ ─────────────────────────►│  loki-data   │              │
                    │   │ :3000     │   alert rules             │  loki-archive│              │
                    │   │ Explore + │   (ERROR / WARN bursts)   └──────▲───────┘              │
                    │   │ alerting  │                                  │ S3 put (daily)      │
                    │   └─────┬─────┘                                  │                     │
                    │         │ SMTP :1025              ┌──────────────┴──────────┐           │
 admin ────────────►│         ▼                         │ Airflow (overlay)       │           │
 :8025 ────────────►│   ┌───────────┐                    │ api-server :8080        │           │
                    │   │ Mailpit   │                    │ scheduler, dag-processor│           │
                    │   │ :1025/    │                    │ + postgres:16          │           │
                    │   │ :8025     │                    │ healthcheck + archive   │           │
                    │   └───────────┘                    └─────────────────────────┘           │
                    └────────────────────────────────────────────────────────────────────────┘
```

**Data flow (happy path):**

1. An app writes a line to **stdout/stderr** → Docker's `json-file` driver stores it.
2. **Alloy** discovers the container through the Docker socket, attaches labels
   (`job=docker`, `container`, `service`), reads the raw records.
3. **Alloy's `loki.process`** unwraps the JSON envelope, drops entries older than 160h,
   stitches multi-line stack traces, extracts `level` from JSON/logfmt/regex, normalises it to
   `TRACE|DEBUG|INFO|WARN|ERROR|OTHER`, and promotes it to a **Loki label**.
4. **Loki** indexes by label and writes chunks + index to the **`loki-data`** bucket in MinIO
   (schema `tsdb` v13, 24h index period, structured metadata on).
5. **Grafana** queries with LogQL, evaluates the two provisioned alert rules, and sends SMTP
   notifications to **Mailpit** (a local catch-all inbox you can open at `:8025`).
6. **Airflow** runs in the background: every 5 minutes it health-checks the services and restarts
   any that are down, and asserts that logs actually reached Loki; every night at 01:00 it exports
   the previous day's logs to `s3://loki-archives/dt=YYYY-MM-DD/logs.parquet`.

---

## How it works, step by step

### 1. Collection (Alloy)

`config/alloy/config.alloy` defines a five-stage pipeline:

```text
discovery.docker → discovery.relabel → loki.source.docker → loki.process("normalize") → loki.write
```

- **`discovery.docker`** talks to `unix:///var/run/docker.sock` and lists running containers.
- **`discovery.relabel`** maps Docker metadata to clean labels and strips the leading `/` from
  container names:
  - `container=<name>` from `__meta_docker_container_name`
  - `service=<name>` from `com.docker.compose.service`
- **`loki.source.docker`** tails each container's log file and stamps `job=docker`.
- **`loki.process "normalize"`** is where the real work happens:

  | Stage | Purpose |
  | --- | --- |
  | `stage.docker` | Unwraps the `{log, stream, time}` json-file envelope |
  | `stage.drop` (160h) | Prevents Loki's `400 timestamp too old` when Alloy replays history |
  | `stage.multiline` | Merges indented continuations (stack traces) onto the first line — `firstline=^[^\s]`, 3s wait, 500 max lines |
  | `stage.json` | Pulls `level` / `severity` / `lvl` out of JSON payloads |
  | `stage.logfmt` | Same for `level=info msg="..."` style logs |
  | `stage.regex` ×2 | Fallbacks for plain text: `level=ERROR`, `[ERROR]`, leading `ERROR ...` |
  | `stage.template` | Collapses everything into a stable vocabulary: `TRACE DEBUG INFO WARN ERROR OTHER` |
  | `stage.labels` | Promotes `level` to a **label** so queries and alerts can filter on it |

- **`loki.write`** pushes to `http://loki:3100/loki/api/v1/push`.

### 2. Storage (Loki → MinIO)

`config/loki/loki.yaml`:

- `object_store: s3` → endpoint `minio:9000`, bucket **`loki-data`** (path-style, insecure, local).
- `store: tsdb`, schema **v13**, index prefix `index_` with a **24h** index period.
- `replication_factor: 1`, in-memory ring — single-binary, single-node mode.
- `allow_structured_metadata: true` — you can attach fields to a log line without making them
  labels (the right place for high-cardinality data).
- `auth_enabled: false` — the platform assumes it is only reachable on the local network.

Startup order is enforced in Compose:

```text
minio (healthy) → minio-init (creates buckets, must complete) → loki → alloy / grafana
```

`minio-init` is a one-shot `amazon/aws-cli` container that creates `loki-data` and `loki-archive`
if they don't exist.

### 3. Querying and alerting (Grafana)

Grafana is provisioned from `config/grafana/provisioning/`:

- **Data source** (`datasources/loki.yaml`): Loki at `http://loki:3100`, default, 5000 max lines.
- **Alert rules** (`alerting/alerting.yaml`), folder `Observability`, group `logs-alerting`:

  | UID | Rule | Query | Threshold | For |
  | --- | --- | --- | --- | --- |
  | `log-error-rate` | Logs: ERROR burst | `sum by (service) (count_over_time({level="ERROR"}[5m]))` | `> 3` | 1m |
  | `log-warn-rate` | Logs: WARN burst | `sum by (service) (count_over_time({level="WARN"}[5m]))` | `> 20` | 5m |

- SMTP is pointed at `mailpit:1025`, so every alert email lands in the Mailpit UI at
  <http://localhost:8025> instead of needing a real mail server.

### 4. Demo traffic (`error-gen`)

An Alpine container that echoes `level=error msg="test failure"` every 5 seconds — guaranteeing
the ERROR alert has something to fire on and giving you data on first launch.

---

## Components

| Service | Image | Ports | Role |
| --- | --- | --- | --- |
| `minio` | `minio/minio:latest` | 9000 (S3), 9001 (console) | S3 object store for Loki chunks/index + archives |
| `minio-init` | `amazon/aws-cli:latest` | — | One-shot: creates the `loki-data` / `loki-archive` buckets |
| `loki` | `grafana/loki:3.4.2` | 3100 | Log store: indexes streams by label, stores in S3 |
| `alloy` | `grafana/alloy:latest` | 12345 | Collector: discovery → relabel → parse → normalise → push |
| `grafana` | `grafana/grafana:latest` | 3000 | UI: Explore, dashboards, alert rules, SMTP alerts |
| `mailpit` | `axllent/mailpit:latest` | 8025 (UI), 1025 (SMTP) | Local catch-all inbox for alert emails |
| `error-gen` | `alpine` | — | Demo log source (`level=error` every 5s) |
| *(overlay)* `airflow-api-server` | `apache/airflow:3.3.2` | 8080 | Airflow UI + API |
| *(overlay)* `airflow-scheduler` | `apache/airflow:3.3.2` | — | Runs DAG runs |
| *(overlay)* `airflow-dag-processor` | `apache/airflow:3.3.2` | — | Parses `airflow/dags/*.py` |
| *(overlay)* `airflow-db` | `postgres:16` | — | Airflow metadata database |

Named volumes: `minio-data`, `loki-data`, `grafana-data`, `airflow-db`, `airflow-logs`.
Network: a single user-defined bridge network called **`monitoring`**.

---

## Repository layout

```text
.
├── docker-compose.yml                 # Core stack (7 services) — the platform
├── docker-compose.airflow.yml         # Optional Airflow overlay (health checks + archive DAGs)
├── config/
│   ├── alloy/config.alloy             # Collector pipeline: discovery → relabel → read → parse → write
│   ├── loki/loki.yaml                 # Schema/storage: tsdb v13 on S3 (MinIO), structured metadata
│   └── grafana/provisioning/
│       ├── datasources/loki.yaml      # Loki data source, auto-created on boot
│       └── alerting/alerting.yaml     # ERROR / WARN burst rules on the `level` label
├── airflow/
│   └── dags/logs_dag.py               # 2 DAGs: logs_stack_healthcheck, logs_daily_archive
├── docs/
│   ├── generate_architecture.py       # Source for the PNG diagram (diagrams + graphviz)
│   ├── generate_best_architecture.py  # Source for the target-state diagram
│   ├── logs_platform_architecture.png
│   └── best_architecture.png
├── Untitled Diagram.gif               # ← the animated architecture diagram used above
├── .env.example                       # Copy to .env (currently optional; see Security notes)
├── LICENSE                            # MIT
└── README.md
```

---

## Quick start

Requirements: **Docker** with the Compose plugin.

### Core platform

```bash
docker compose up -d
docker compose ps
```

### Platform + Airflow control plane

```bash
# get your docker group id so the DAGs can talk to the socket
export DOCKER_GID=$(getent group docker | cut -d: -f3)

docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

### Smoke test

```bash
# 1. push some obvious logs from a throwaway container
docker run --rm --name hello alpine \
  sh -c 'for i in $(seq 5); do echo "hello from alpine $i"; sleep 1; done'

# 2. Grafana → Explore → Loki, then run:
#    {container="hello"}
```

You should also see `error-gen` producing `level=ERROR` lines immediately, which trips the
`log-error-rate` alert within a few minutes — check the **Alerting → Alert rules** page, then the
Mailpit inbox at <http://localhost:8025>.

---

## Deploy to a VM & onboard developers

One command pushes the core stack to a remote VM over SSH and renders a ready-to-share
developer agent:

```bash
./deploy/deploy-vm.sh user@vm-host              # core stack only
./deploy/deploy-vm.sh user@vm-host --with-airflow
ALLOY_PASSWORD=xxx ./deploy/deploy-vm.sh user@vm-host   # custom push password
```

The script syncs the repo to `~/logs-platform`, writes a VM-specific `caddy/Caddyfile`
(Grafana at `https://<vm-ip>` with a Caddy `tls internal` cert, Loki API under
`https://<vm-ip>/loki/*` behind basic auth), runs `docker compose up -d`, and creates
`agents/agent.alloy` with the VM IP + password already filled in.

**Onboard a developer machine** — nothing to change in their apps, logs come from stdout:

```bash
# 1. copy the agents/ folder to the dev machine, then:
cd agents
docker compose -f docker-compose.alloy.yml up -d
# 2. start any container — its logs appear in Grafana within seconds
```

Every container on that host is shipped to the central Loki. In Grafana, filter with
`{job="docker"}`, `{container="<name>"}`, `{service="<compose service>"}` or
`{project="<compose project>"}`.

---

## Ports and credentials

| Service | URL | Credentials |
| --- | --- | --- |
| Grafana | <http://localhost:3000> | `admin / admin` |
| Loki API | <http://localhost:3100> | none (`auth_enabled: false`) |
| Alloy debug UI | <http://localhost:12345> | none |
| MinIO S3 API | <http://localhost:9000> | `admin / adminpassword` |
| MinIO console | <http://localhost:9001> | `admin / adminpassword` |
| Mailpit UI | <http://localhost:8025> | none |
| Mailpit SMTP | `localhost:1025` | — |
| Airflow UI *(overlay)* | <http://localhost:8080> | `admin / admin` |

> These are development defaults. See [Security notes](#security-notes) before exposing anything.

---

## LogQL cookbook

```logql
{job="docker"}                                    # everything Alloy collects
{container="hello"}                               # one container
{service="grafana"}                               # one compose service
{service="grafana"} |= "error"                    # substring filter
{level="ERROR"}                                   # the normalised level label
{job="docker"} | json | level=~"warn|error"       # parse lines as JSON at query time
sum by (service) (count_over_time({level="ERROR"}[5m]))   # error rate per service
{job="docker"} |~ "(?i)timeout|refused"           # regex over the raw line
```

**Label vs. line:** labels are indexed and define *streams* — keep the cardinality low
(`service`, `container`, `level`, `job`). Anything high-cardinality (user ids, request ids,
timestamps) belongs in the log line or in structured metadata, never in a label.

---

## Alerting

Rules live in `config/grafana/provisioning/alerting/alerting.yaml` — pure config, recreated on
every boot (edits made in the UI are **not** persisted). To change a threshold, edit the YAML and
restart Grafana:

```bash
docker compose up -d --force-recreate grafana
```

Both rules are grouped under folder **Observability**, group `logs-alerting`, evaluated every
minute, and route to Grafana's default contact point over SMTP → Mailpit. To send real alerts,
replace `GF_SMTP_*` in `docker-compose.yml` with a real SMTP server and configure a contact point
in **Alerting → Contact points**.

---

## The Airflow control plane

`docker-compose.airflow.yml` is an **overlay** — it is layered on top of the core file, joins the
same external `monitoring` network, and builds an Airflow image with
`docker`, `requests`, `boto3`, `pandas`, `pyarrow` preinstalled.

Two DAGs in `airflow/dags/logs_dag.py`:

### `logs_stack_healthcheck` — cron `*/5 * * * *`

| Task | What it does |
| --- | --- |
| `check_services()` | HTTP GETs Alloy `/-/ready`, Loki `/ready`, Grafana `/api/health`, MinIO `/minio/health/live`; collects anything with status ≥ 400 |
| `restart_unhealthy()` | Uses the Docker SDK to `restart()` containers matched by `com.docker.compose.service=<svc>` |
| `verify_logs_flowing()` | Queries Loki `query_range` for `{container=~".+"}` over the last 10 minutes; **fails the run** if nothing arrived |
| `alert_if_needed()` | Hook point — currently a `print()` where you'd plug Slack/PagerDuty/email |

### `logs_daily_archive` — cron `0 1 * * *`

| Task | What it does |
| --- | --- |
| `ensure_bucket()` | Creates `loki-archives` in MinIO if missing |
| `export_day()` | Pages through Loki `query_range` (limit 5000, cursor = max ts + 1) for the previous day → pandas DataFrame → Parquet in memory → `put_object` to `s3://loki-archives/dt=YYYY-MM-DD/logs.parquet` |
| `validate()` | `head_object` and fails the run if the object is empty |

Both DAGs use the Airflow 3 `airflow.sdk` API (`@dag` / `@task`).

> **Naming note:** `minio-init` creates `loki-archive`, while the DAG writes to `loki-archives`
> (it self-creates the bucket). They are two different buckets today — pick one name and use it
> everywhere.

---

## Configuration reference

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Images, ports, volume mounts, health checks, startup order, network |
| `config/alloy/config.alloy` | Discovery → relabel → read → parse/normalise → push |
| `config/loki/loki.yaml` | `tsdb` v13 on S3/MinIO, 24h index period, structured metadata |
| `config/grafana/provisioning/datasources/loki.yaml` | Loki data source, created on boot |
| `config/grafana/provisioning/alerting/alerting.yaml` | ERROR / WARN burst alert rules |
| `docker-compose.airflow.yml` | Airflow services, env (MinIO/Loki endpoints), socket mount |
| `airflow/dags/logs_dag.py` | Health-check + daily-archive DAGs |
| `.env.example` | Template for `.env` (see Security notes — currently not interpolated) |

Editing a config requires a restart of the affected container:

```bash
docker compose up -d --force-recreate loki     # or: alloy / grafana
```

Data survives restarts; wipe it only when you mean to:

```bash
docker compose down        # stop, keep volumes
docker compose down -v     # delete minio-data, loki-data, grafana-data, airflow volumes
```

---

## Day-2 operations

```bash
docker compose logs -f alloy            # watch the collection pipeline
curl -s http://localhost:3100/ready     # Loki readiness
curl -s http://localhost:3000/api/health
open http://localhost:12345             # Alloy debug UI → Targets (are containers discovered?)
docker compose config                   # render the effective Compose file
docker compose ps                       # status + health
```

Useful admin commands:

```bash
# list everything on the monitoring network
docker network inspect monitoring

# jump into MinIO's client
docker exec -it minio mc

# trigger a DAG manually (overlay)
docker compose exec airflow-api-server airflow dags trigger logs_stack_healthcheck
```

---

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Cannot connect to the Docker daemon` | Keep `/var/run/docker.sock:/var/run/docker.sock:ro` mounted on `alloy` — the whole discovery step depends on it |
| Grafana: "connection refused" on the data source | Inside the network Loki is `http://loki:3100`; `localhost` means the Grafana container itself |
| Grafana shows no data | Open the Alloy debug UI (`:12345`) → **Targets**. If the container isn't there, it's a discovery/label problem, not a Loki problem |
| Loki answers `400 timestamp too old` | The `stage.drop older_than=160h` stage exists exactly for this — don't remove it |
| High cardinality / huge index | Never label container IDs, request IDs, or timestamps; keep them in the line or in structured metadata |
| Airflow DAG can't restart containers | Set `DOCKER_GID` (`getent group docker \| cut -d: -f3`) so `group_add` matches your host's docker group |
| Alert emails missing | Check Mailpit at `:8025`; Grafana SMTP logs are visible with `GF_LOG_FILTERS: provisioning:debug` |

---

## Regenerating the diagrams

Both diagrams are generated, not drawn by hand.

```bash
python3 -m venv venv
venv/bin/pip install diagrams
sudo apt install graphviz        # brew install graphviz on macOS

venv/bin/python docs/generate_architecture.py              # writes logs_platform_v2.png
venv/bin/python docs/generate_best_architecture.py         # writes best_architecture.png
```

The animated `Untitled Diagram.gif` was authored in a diagramming tool and is committed as-is.

---

## Recommended architecture (target state)

The current stack is an excellent **single-node** foundation. Below is the shape it should grow
into as it moves from "laptop/home lab" to "production". Same components — just hardened and
split into layers.

![Recommended architecture](docs/best_architecture.png)

```text
                                ┌───────────────────────────────────────────────┐
  APP CONTAINERS / HOSTS        │              EDGE (optional)                 │
  ┌──────────────────┐          │  reverse proxy (Caddy/Traefik) + mTLS        │
  │ app logs → stdout│          │  /grafana  /loki  (SSO, no public raw S3)    │
  └────────┬─────────┘          └───────────────────────────────────────────────┘
           │
           ▼
┌───────────────────────────  COLLECTION LAYER  ─────────────────────────────────┐
│  Grafana Alloy  (DaemonSet / one per host)                                    │
│   discovery → relabel → process(docker, drop, multiline, json/logfmt, regex)  │
│   → level label + structured metadata        [docker.sock mounted :ro only]   │
│   + self-monitoring: ship alloy's own metrics to Prometheus/Mimir             │
└──────────────┬────────────────────────────────────────────────────────────────┘
               │  push /loki/api/v1/push  (over mTLS, with an auth token)
               ▼
┌───────────────────────────  STORAGE LAYER  ───────────────────────────────────┐
│  Grafana Loki (read path: query-frontend + querier)                           │
│  Grafana Loki (write path: distributor + ingester)                            │
│   schema v13 tsdb / s3 / 24h index / structured metadata / RF=3               │
│        │ chunks + index                                                       │
│        ▼                                                                      │
│  Object store:  MinIO (S3)  →  real S3 / GCS / Azure in cloud                 │
│   buckets: loki-data (hot)   loki-archive/cold (Parquet, 30d+)                │
│   lifecycle rule: transition → cold bucket / Glacier after 7d                 │
│   + bucket versioning + encryption at rest                                    │
└──────────────┬────────────────────────────────────────────────────────────────┘
               │  LogQL
               ▼
┌───────────────────────────  VISIBILITY LAYER  ────────────────────────────────┐
│  Grafana                                                                       │
│   Explore · dashboards (ingest rate, error ratio, top services)                │
│   Alert rules → contact points: Email / Slack / PagerDuty / webhook            │
│   SSO (OIDC), viewer vs editor roles                                           │
└──────────────┬────────────────────────────────────────────────────────────────┘
               │ SMTP / webhook
               ▼
┌───────────────────────────  NOTIFICATION LAYER  ──────────────────────────────┐
│  Mailpit (dev)  →  real SMTP / Slack / PagerDuty (prod)                        │
└────────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────  CONTROL PLANE  ────────────────────────────────────┐
│  Airflow                                                                       │
│   */5  healthcheck + restart + "logs still flowing?" assertion                │
│   01:00 daily Parquet export → cold bucket (partition dt=YYYY-MM-DD)           │
│   weekly retention/lifecycle audit, alert-rule smoke tests                     │
│  + Prometheus/Mimir scraping Alloy & Loki meta-metrics                        │
│  + loki-canary or synthetic probe for end-to-end verification                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

### Why this is the right target for *this* stack

| Concern | Choice | Reason |
| --- | --- | --- |
| Collector | **Grafana Alloy** (already used) | It's the vendor-recommended successor to Promtail/Fluent Bit, one binary for logs + metrics + traces, and config-as-code |
| Store | **Loki** (already used) | Labels-first, cheap, no separate index cluster; `tsdb` + S3 is the modern, recommended schema |
| Object store | **MinIO now → S3/GCS later** | S3-compatible API means the switchover is a config edit, not a migration |
| Hot/cold split | `loki-data` + `loki-archive` (already scaffolded) | Loki keeps 7–30d for fast queries; Airflow's Parquet export gives you cheap, queryable history forever (readable by pandas/DuckDB/Spark) |
| Alert routing | Grafana alerting → contact points | Rules stay next to the query that defines them; provisioned as code |
| Meta-observability | Ship Alloy/Loki self-metrics + a canary | A logging platform you can't monitor is a black box |
| Scale-out path | Loki read/write split + RF=3 + external object store | Only needed past ~single-node traffic; the config shape stays the same |

### Hardening checklist (do before exposing anything)

- [ ] Fix `GF_SECURITY_PASSWORD` → `GF_SECURITY_ADMIN_PASSWORD` in `docker-compose.yml`
- [ ] Move every credential (Grafana, MinIO, Airflow JWT/API secrets) into `.env`, and wire the
      variables into the compose file (`.env.example` exists but is currently not referenced)
- [ ] Set `auth_enabled: true` in Loki (or put it behind the reverse proxy) and give Alloy a token
- [ ] Keep `/var/run/docker.sock` **read-only** on `alloy`; scope Airflow's socket access or
      replace restarts with a docker-socket proxy
- [ ] Pin images to digests instead of `:latest`
- [ ] Add bucket lifecycle rules + encryption on the MinIO buckets
- [ ] Enable Prometheus meta-monitoring for Alloy/Loki (drops, push lag, `parsed=false` rate)

---

## Status and roadmap

| Capability | Status |
| --- | --- |
| Docker discovery + relabelling (`container`, `service`) | ✅ done |
| Parse / normalise / multiline / level label | ✅ done |
| Ship to Loki, query in Grafana (provisioned data source) | ✅ done |
| Loki storage on S3 (MinIO) | ✅ done |
| Provisioned ERROR / WARN alert rules + SMTP via Mailpit | ✅ done |
| Airflow health-check + restart DAG | ✅ done |
| Airflow daily Parquet archive to S3 | ✅ done |
| Demo `error-gen` log source | ✅ done |
| One-command startup, `minio-init` bucket bootstrap | ✅ done |
| Fix bucket-name inconsistency (`loki-archive` vs `loki-archives`) | ⬜ todo |
| Secrets via `.env` (currently dead config) | ⬜ todo |
| Prometheus meta-monitoring (drops, lag, `parsed=false`) | ⬜ planned |
| Loki canary / end-to-end synthetic probe | ⬜ planned |
| Reverse proxy + auth in front of Grafana/Loki | ⬜ planned |
| n8n / Slack webhook alert routing | ⬜ out of scope |

---

## Security notes

This stack ships with **development defaults** and is intended for a trusted local network:

- Grafana `admin/admin`, MinIO `admin/adminpassword`, Airflow `admin/admin` are hardcoded.
- Loki has `auth_enabled: false`.
- `.env.example` exists but the compose files do not interpolate it yet — copying it to `.env`
  currently has no effect.
- Airflow runs with `/var/run/docker.sock` mounted **read-write** so its DAGs can restart
  containers; that is root-equivalent on the host. Gate it behind a socket proxy or drop the
  archive/healthcheck DAGs' socket access in anything non-local.
- Airflow's JWT/API secrets default to `change-this-...` placeholders — override them with
  `AIRFLOW_JWT_SECRET` / `AIRFLOW_API_SECRET_KEY`.

---

## License

MIT — see [LICENSE](LICENSE).
