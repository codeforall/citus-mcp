<div align="center">

<img src="docs/images/logo.png" alt="citus-mcp logo" width="180"/>

# Citus MCP Server

**An AI-powered MCP server for managing Citus distributed PostgreSQL clusters**

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Citus](https://img.shields.io/badge/Citus-11.x--14.x-336791?logo=postgresql)](https://www.citusdata.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13--17-336791?logo=postgresql)](https://www.postgresql.org)

[Quick Start](#-quick-start) •
[Features](#-features) •
[Installation](#-installation) •
[Configuration](#-configuration) •
[Tools Reference](#-tools-reference) •
[Examples](#-usage-examples)

</div>

---

## 📖 What is Citus MCP?

Citus MCP is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that enables AI assistants like GitHub Copilot to interact with your Citus distributed PostgreSQL cluster. It provides:

| Feature | Description |
|---------|-------------|
| 🔍 **Read-only Inspection** | Safely explore distributed tables, shards, nodes, and colocation groups |
| 🤖 **Intelligent Advisors** | Get recommendations for rebalancing, skew analysis, configuration, and operational health |
| 🛡️ **Guarded Operations** | Execute dangerous operations only with explicit approval tokens |
| 📊 **Real-time Monitoring** | View cluster activity, locks, background jobs, and hot shards |

### How It Works

```
┌─────────────────┐                      ┌──────────────┐                ┌─────────────────┐
│  GitHub Copilot │     MCP Protocol     │  citus-mcp   │      SQL       │  Citus Cluster  │
│  (VS Code/CLI)  │ <──────────────────> │    server    │ <────────────> │  (Coordinator)  │
└─────────────────┘      stdio/SSE       └──────────────┘                └─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- **Go 1.23+** (for building from source)
- **PostgreSQL 13–17** with the Citus extension (11.x or later) on the coordinator
- **GitHub Copilot** with MCP support (VS Code or CLI)

### 1. Build the Server

```bash
git clone https://github.com/citusdata/citus-mcp.git
cd citus-mcp
make build
# Binary created at ./bin/citus-mcp
```

Or using Go directly:

```bash
go build -o bin/citus-mcp ./cmd/citus-mcp
```

### 2. Configure Your Connection

Create a configuration file at `~/.config/citus-mcp/config.yaml`:

```yaml
# Minimum required configuration
coordinator_dsn: postgres://username:password@localhost:5432/mydb?sslmode=disable
```

Or set the environment variable:

```bash
export CITUS_MCP_COORDINATOR_DSN="postgres://username:password@localhost:5432/mydb?sslmode=disable"
```

### 3. Set Up VS Code

Create `.vscode/mcp.json` in your workspace (or `mcp.json` at the project root):

```json
{
  "mcpServers": {
    "citus-mcp": {
      "command": "/path/to/citus-mcp/bin/citus-mcp",
      "args": [],
      "env": {
        "CITUS_MCP_COORDINATOR_DSN": "postgres://username:password@localhost:5432/mydb?sslmode=disable"
      }
    }
  }
}
```

### 4. Test the Connection

In VS Code Copilot Chat, type:

```
@citus-mcp ping
```

You should see a "pong" response confirming the connection works.

---

## ✨ Features

71 MCP tools spanning diagnostics, forensics, capacity planning, advisors,
alarms, time-series, and gated execute operations. The single covering tool
`citus_full_report` runs ~30 read-only diagnostics in one shot and rolls
up overall health, top findings, and concrete recommendations.

> See the full **[Tools Reference](#-tools-reference)** below for parameters and details.

### 🧭 Reports & Covering Tools

| Tool | Description |
|------|-------------|
| `citus_full_report` | One-call covering tool — runs every read-only diagnostic + advisor and produces a unified health/findings/recommendations report |
| `citus_cluster_summary` | Coordinator + worker overview; `all:true` enables every section + cross-tool health rollup (memory, metadata cache 3-regime, connections, drift, MX, pooler) |

### 🔍 Cluster Inspection (Read-Only)

| Tool | Description |
|------|-------------|
| `list_nodes` / `list_distributed_tables` / `list_shards` | Legacy listings |
| `citus_list_distributed_tables` / `citus_list_reference_tables` | Paginated, filterable table listings |
| `citus_table_inspector` | Table metadata, indexes, statistics deep-dive |
| `citus_colocation_inspector` | Colocation groups and members |

### 📊 Monitoring & Activity

| Tool | Description |
|------|-------------|
| `citus_activity` | Cluster-wide active queries and connections |
| `citus_lock_inspector` | Lock waits and blocking queries |
| `citus_job_inspector` | Background-job progress (rebalance, copy) |
| `citus_proactive_health` | Long-tx, idle-in-tx, stuck 2PC, bloat, saturation dashboard |
| `citus_shard_heatmap` | Hot shards and node distribution |
| `citus_shard_skew_report` | Data skew analysis per node |
| `citus_explain_query` | EXPLAIN distributed queries |
| `citus_synthetic_probe` | End-to-end synthetic correctness probe |

### 🧠 Memory & Capacity Planning

| Tool | Description |
|------|-------------|
| `citus_metadata_cache_footprint` | Three-regime per-backend Citus metadata cache estimate (typical / hot-path / worst-case), live worst-case backend detection, partition-explosion simulator, pg_stat_statements correlation |
| `citus_pg_cache_footprint` | Per-backend PG `CacheMemoryContext` estimate |
| `citus_worker_memcontexts` | Fan-out of `pg_get_backend_memory_contexts()` |
| `citus_partition_growth_simulator` | Projects cache cost of adding partitions |
| `citus_memory_risk_report` | Per-node OOM-risk rollup with **13 consumer terms** (shared_buffers, wal_buffers, lock_table, pred_lock_table, prepared_xact_state, wal_senders, logical_decoding, bgworker_baseline, backend_process_baseline, per-backend caches, work_mem peak with `hash_mem_multiplier` + parallel workers, temp_buffers, autovacuum, Citus libpq buffers); supports `worst_case:true` |
| `citus_connection_capacity` | Effective client max per deployment mode (coord-only vs MX) |
| `citus_connection_fanout_simulator` | `max_adaptive × peers × clients` pressure simulator |
| `citus_pooler_advisor` | PgBouncer session vs transaction guidance |
| `citus_pgbouncer_inspector` | Connected PgBouncer diagnostics |
| `citus_hardware_sizer` | Sizing for current + projected load (RAM, CPU, IOPS, disk) |
| `citus_shardcount_tradeoff_chart` | Shard-count trade-off chart per table |

### 🛠️ MX & Node Addition

| Tool | Description |
|------|-------------|
| `citus_add_node_preflight` | Coordinator-side checklist for adding a worker (`max_locks_per_transaction`, `max_worker_processes`, `max_connections`, `wal_level`, replication slots, …) |
| `citus_node_prepare_advisor` | Preparation steps + optional shell script |
| `citus_metadata_sync_risk` | Estimates `citus_activate_node` work + timeout/OOM/lock risks; emits concrete `recommended_max_locks_per_transaction` and exact `ALTER SYSTEM SET …` |
| `citus_mx_readiness` | Mesh-connection budgeting before enabling MX |
| `citus_snapshot_source_advisor` | Picks best source worker for snapshot-based add |

### 🚑 Forensics & Recovery

| Tool | Description |
|------|-------------|
| `citus_2pc_recovery_inspector` | Fans out `pg_prepared_xacts` to coord + workers, parses Citus GIDs, correlates with `pg_dist_transaction` (keyed by `(groupid, gid)`) and `get_all_active_transactions()`, and classifies each as `commit_needed` / `rollback_needed` / `in_flight` / `foreign_initiator` (mirrors Citus's own `LIKE 'citus_<localGroupId>_%'` filter — prepared xacts initiated by a different worker are recovered by that worker, not the coordinator). Emits a ready-to-run per-node `COMMIT PREPARED` / `ROLLBACK PREPARED` script (standalone statements — no transaction block, since PostgreSQL forbids it) + alarms for commit backlog and orphan xacts. Read-only — never issues COMMIT/ROLLBACK itself. |
| `citus_rebalance_forensics` | Diagnoses **why** a rebalance (or any bg job) is stuck: inspects `pg_dist_background_job/_task`, correlates running tasks with `pg_stat_activity` wait events and `pg_blocking_pids` (examining each **blocker's** held lock mode and query text — not the waiter's requested lock), counts `pg_dist_cleanup` backlog, and classifies stalls as `blocked_by_ddl` / `blocked_by_lock` / `bg_worker_starvation` / `retry_backoff` (runnables with `not_before > now()`, computed server-side) / `error_with_retries_exhausted` / `finished_with_errors` / `cleanup_backlog`. Emits a concrete playbook (`citus_rebalance_stop` / `citus_cleanup_orphaned_resources` / `pg_cancel_backend`) plus alarms. Addresses citusdata/citus issues #6681, #7103, #8236, #1210. |
| `citus_placement_integrity_check` | Three-way cross-check between Citus metadata (`pg_dist_placement`), on-disk reality on workers (`pg_class` via `run_command_on_workers`, with the Citus shard-visibility hook disabled), and `pg_dist_cleanup`. Detects ghost placements (metadata references shards missing on disk — reads error), orphan tables (shard-suffix tables on workers with no placement row), inactive-with-data (shardstate != 1 but data still materialised), size drift, and `stale_stats` (metadata `shardlength=0` but data exists — a rebalancer will make bad decisions). Orphan detection matches on `(base, shardid)` pairs against live metadata to avoid false positives on user tables whose names happen to end in `_<digits>`. Workers whose fanout fails are tracked in `skipped_workers[]` and `partial_results=true` — ghost detection is suppressed for those nodes rather than emitting false-positive critical alarms. Emits a per-class reconciliation playbook (`citus_copy_shard_placement` / `citus_cleanup_orphaned_resources` / `citus_update_shard_statistics`). |

### 🔧 Metadata, Extensions & Routing

| Tool | Description |
|------|-------------|
| `citus_metadata_health` | Cross-node metadata consistency with fix hints |
| `citus_extension_drift_scanner` | Version/availability drift across nodes |
| `citus_routing_drift_detector` | Detects queries routing to unexpected shards |
| `citus_planner_overhead_probe` | Planner-time measurement |
| `citus_session_guardrails` | Active guardrail settings on the session |

### 🤖 Intelligent Advisors

| Tool | Description |
|------|-------------|
| `citus_advisor` | Top-level SRE + performance advisor |
| `citus_config_advisor` | Citus + PostgreSQL configuration analysis |
| `citus_config_deep_inspect` | Full PG + Citus GUC deep dive with drift rules |
| `citus_shard_advisor` | Per-table shard-count recommendation |
| `citus_columnar_advisor` | Columnar-storage candidates |
| `citus_tenant_risk` | Risky / hot tenants |
| `citus_query_pathology` | Pathological distributed queries |
| `citus_rebalance_cost_estimator` | Estimates rebalance cost (time, WAL, bytes) |
| `citus_validate_rebalance_prereqs` | Rebalance readiness checklist |

### 🚨 Alarms

| Tool | Description |
|------|-------------|
| `citus_alarms_list` / `citus_alarms_ack` / `citus_alarms_clear` | List, acknowledge, and bulk-clear alarms emitted by other tools |

### 📈 Time-Series & Snapshots

| Tool | Description |
|------|-------------|
| `citus_snapshot_record` / `citus_snapshot_list` | Record + list snapshots (opt-in SQLite) |
| `citus_trend` / `citus_growth_projection` | Trend lines and linear/exponential projections |
| `citus_regression_detect` / `citus_what_changed` | Compare against a baseline / diff two snapshots |

### ⚡ Execute Operations (Approval Required)

| Tool | Description |
|------|-------------|
| `citus_request_approval_token` | HMAC-signed time-limited approval token |
| `citus_rebalance_plan` / `citus_rebalance_execute` / `citus_rebalance_status` | Preview, run, monitor rebalance |
| `citus_move_shard_plan` / `citus_move_shard_execute` | Preview and execute shard move |
| `citus_isolate_tenant` | Isolate a tenant to its own shard |
| `citus_cleanup_orphaned` | Clean up orphaned placements |
| `rebalance_table_plan` / `rebalance_table_execute` | Legacy per-table variants |

---

## 📦 Installation

### Option 1: Build from Source

```bash
# Clone the repository
git clone https://github.com/citusdata/citus-mcp.git
cd citus-mcp

# Build using Make
make build

# Or build directly with Go
go build -o bin/citus-mcp ./cmd/citus-mcp

# (Optional) Install to your PATH
sudo cp bin/citus-mcp /usr/local/bin/
```

### Option 2: Go Install

```bash
go install github.com/citusdata/citus-mcp/cmd/citus-mcp@latest
```

### Verify Installation

```bash
citus-mcp --help
```

---

## ⚙️ Configuration

### Connection String (DSN)

The most important configuration is the PostgreSQL connection string to your Citus coordinator:

```
postgres://[user]:[password]@[host]:[port]/[database]?sslmode=[mode]
```

**Examples:**

```bash
# Local development (no SSL)
postgres://postgres:secret@localhost:5432/mydb?sslmode=disable

# Production with SSL
postgres://admin:secret@citus-coord.example.com:5432/production?sslmode=require

# With specific schema
postgres://user:pass@host:5432/db?sslmode=require&search_path=myschema
```

### Configuration Methods

Configuration can be provided via (in order of precedence):

1. **Command-line flags**
2. **Environment variables**
3. **Configuration file**

#### Method 1: Environment Variables

```bash
# Required
export CITUS_MCP_COORDINATOR_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable"

# Optional
export CITUS_MCP_MODE="read_only"           # read_only (default) or admin
export CITUS_MCP_ALLOW_EXECUTE="false"      # Enable execute operations
export CITUS_MCP_APPROVAL_SECRET="secret"   # Required if allow_execute=true
export CITUS_MCP_LOG_LEVEL="info"           # debug, info, warn, error
```

#### Method 2: Configuration File

Create `~/.config/citus-mcp/config.yaml`:

```yaml
# ===========================================
# Citus MCP Server Configuration
# ===========================================

# Database Connection (REQUIRED)
# -----------------------------
coordinator_dsn: postgres://user:password@localhost:5432/mydb?sslmode=disable

# Optional: Override credentials from DSN
# coordinator_user: myuser
# coordinator_password: mypassword

# Connection Mode (RECOMMENDED: true for production)
# --------------------------------------------------
# When true (default), worker data is fetched via run_command_on_workers() UDF.
# Set to false only for dev/test with direct worker access.
coordinator_only: true

# Optional: Direct worker connections (dev/test override only)
# If specified when coordinator_only=true, these take precedence (dev escape hatch)
# worker_dsns: postgres://user:pass@worker1:5432/db,postgres://user:pass@worker2:5432/db

# Server Mode
# -----------
# read_only: Only inspection tools available (default, safest)
# admin: All tools available including execute operations
mode: read_only

# Execute Operations (only if mode=admin)
# ---------------------------------------
allow_execute: false
# approval_secret: your-secret-key  # Required if allow_execute=true

# Performance Settings
# --------------------
cache_ttl_seconds: 5          # Cache duration for metadata queries
enable_caching: true          # Set to false to disable caching
max_rows: 200                 # Maximum rows returned per query
max_text_bytes: 200000        # Maximum text size in responses

# Timeouts
# --------
connect_timeout_seconds: 10   # Connection timeout
statement_timeout_ms: 30000   # Query timeout (30 seconds)

# Logging
# -------
log_level: info               # debug, info, warn, error

# Transport (NEW)
# ---------------
# stdio: Standard input/output (default, for VS Code/CLI integration)
# sse: Server-Sent Events over HTTP (for remote/network access)
# streamable: Streamable HTTP transport (for remote/network access)
transport: stdio

# HTTP Settings (only used when transport is sse or streamable)
# http_addr: 127.0.0.1        # Listen address (use 0.0.0.0 for all interfaces)
# http_port: 8080             # Listen port
# http_path: /mcp             # Endpoint path
# sse_keepalive_seconds: 30   # SSE keepalive interval
```

#### Method 3: Command-Line Flags

```bash
# Using flags (note: use underscores in flag names)
bin/citus-mcp --coordinator_dsn "postgres://..." --mode read_only

# Using positional argument for DSN
bin/citus-mcp "postgres://user:pass@localhost:5432/mydb?sslmode=disable"

# Specify config file
bin/citus-mcp --config /path/to/config.yaml

# Start with SSE transport
bin/citus-mcp --transport sse --http_port 8080 --coordinator_dsn "postgres://..."
```

### Configuration File Locations

The server searches for configuration files in this order:

1. `--config` / `-c` flag
2. `CITUS_MCP_CONFIG` environment variable
3. `$XDG_CONFIG_HOME/citus-mcp/config.yaml`
4. `~/.config/citus-mcp/config.yaml`
5. `./citus-mcp.yaml` (current directory)

Supported formats: YAML, JSON, TOML

---

## 🌐 Transport Options

Citus MCP supports three transport modes for different deployment scenarios:

### 1. Stdio Transport (Default)

Standard input/output transport — the server communicates via stdin/stdout. This is the default and is used for direct integration with VS Code and GitHub Copilot CLI.

```bash
# Default - stdio transport
bin/citus-mcp --coordinator_dsn "postgres://..."

# Explicit
bin/citus-mcp --transport stdio --coordinator_dsn "postgres://..."
```

**Use cases:**
- VS Code Copilot Chat integration
- GitHub Copilot CLI
- Local development

### 2. SSE Transport (Server-Sent Events)

HTTP-based transport using Server-Sent Events. The server runs as an HTTP daemon that clients can connect to remotely.

```bash
# Start server on HTTP with SSE
bin/citus-mcp --transport sse --http_addr 0.0.0.0 --http_port 8080 --coordinator_dsn "postgres://..."

# Or via environment variables
export CITUS_MCP_TRANSPORT=sse
export CITUS_MCP_HTTP_ADDR=0.0.0.0
export CITUS_MCP_HTTP_PORT=8080
export CITUS_MCP_COORDINATOR_DSN="postgres://..."
bin/citus-mcp
```

**Endpoints:**
- `GET /mcp` - Establish SSE connection
- `POST /mcp/session/{id}` - Send messages to session
- `GET /health` - Health check

**Use cases:**
- Remote MCP server deployment
- Docker/Kubernetes deployments
- Shared server for multiple clients
- Network-accessible MCP services

### 3. Streamable HTTP Transport

Modern HTTP transport with streaming support. Recommended for new deployments.

```bash
# Start server with streamable HTTP transport
bin/citus-mcp --transport streamable --http_addr 0.0.0.0 --http_port 8080 --coordinator_dsn "postgres://..."
```

**Endpoints:**
- `POST /mcp` - Handle MCP requests with streaming responses
- `GET /health` - Health check

**Use cases:**
- Same as SSE, with better streaming support
- Environments where SSE is not ideal

### Docker Deployment Example

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o citus-mcp ./cmd/citus-mcp

FROM alpine:latest
COPY --from=builder /app/citus-mcp /usr/local/bin/
EXPOSE 8080
CMD ["citus-mcp", "--transport", "sse", "--http-addr", "0.0.0.0", "--http-port", "8080"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  citus-mcp:
    build: .
    ports:
      - "8080:8080"
    environment:
      CITUS_MCP_TRANSPORT: sse
      CITUS_MCP_HTTP_ADDR: 0.0.0.0
      CITUS_MCP_HTTP_PORT: 8080
      CITUS_MCP_COORDINATOR_DSN: postgres://user:pass@citus-coordinator:5432/mydb?sslmode=disable
```

### Connecting to Remote Server

For SSE/Streamable transports, configure your MCP client to connect via HTTP:

```json
{
  "mcpServers": {
    "citus-mcp": {
      "type": "sse",
      "url": "http://citus-mcp-server:8080/mcp"
    }
  }
}
```

---

## 🔌 Setting Up with GitHub Copilot

### VS Code Setup

1. **Install Prerequisites**
   - VS Code with GitHub Copilot extension
   - MCP support enabled in Copilot settings

2. **Create MCP Configuration**

   Create `.vscode/mcp.json` in your workspace:

   ```json
   {
     "mcpServers": {
       "citus-mcp": {
         "command": "/absolute/path/to/bin/citus-mcp",
         "args": [],
         "env": {
           "CITUS_MCP_COORDINATOR_DSN": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
         }
       }
     }
   }
   ```

   Or for development (using `go run`):

   ```json
   {
     "mcpServers": {
       "citus-mcp": {
         "command": "go",
         "args": ["run", "./cmd/citus-mcp"],
         "cwd": "/path/to/citus-mcp",
         "env": {
           "CITUS_MCP_COORDINATOR_DSN": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
         }
       }
     }
   }
   ```

3. **Reload VS Code** and open Copilot Chat

4. **Verify Connection**
   ```
   @citus-mcp ping
   ```

### GitHub Copilot CLI Setup

1. **Create Global MCP Config**

   Create `~/.config/github-copilot/mcp.json`:

   ```json
   {
     "mcpServers": {
       "citus-mcp": {
         "command": "/usr/local/bin/citus-mcp",
         "args": [],
         "env": {
           "CITUS_MCP_COORDINATOR_DSN": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
         }
       }
     }
   }
   ```

2. **Verify Setup**
   ```bash
   copilot mcp list
   copilot mcp test citus-mcp
   ```

3. **Use in CLI**
   ```bash
   copilot -p "Show me the cluster summary"
   ```

---

## 💡 Usage Examples

> The examples below are natural-language prompts you can drop into Copilot
> Chat (VS Code / CLI). Copilot maps them to the underlying MCP tool call;
> JSON payloads are shown alongside where useful for automation.

### Basic Cluster Inspection

```
@citus-mcp Give me everything about this cluster
```
→ `citus_cluster_summary` with `{"all": true}` — returns coordinator/worker topology, counts, GUCs, configuration report, and the cross-tool health rollup (memory / connections / drift / metadata-sync / operational).

```
@citus-mcp List all distributed and reference tables
```
→ `citus_list_distributed_tables`, `citus_list_reference_tables`

```
@citus-mcp Inspect the public.orders table including shards and indexes
```
→ `citus_table_inspector` with `{"table":"public.orders","include_shards":true,"include_indexes":true}`

### Monitoring

```
@citus-mcp Show current cluster activity
@citus-mcp Are there any lock waits right now?
@citus-mcp Show background job progress
@citus-mcp Proactive health dashboard for the cluster
```
→ `citus_activity`, `citus_lock_inspector`, `citus_job_inspector`, `citus_proactive_health`

### Memory & Capacity Planning

```
@citus-mcp Project per-backend Citus metadata cache memory
```
→ `citus_metadata_cache_footprint` — size of `MetadataCacheMemoryContext` per backend based on current shape.

```
@citus-mcp Estimate PG CacheMemoryContext footprint on the coordinator and each worker
```
→ `citus_pg_cache_footprint` — relcache/catcache/plancache estimates.

```
@citus-mcp Is the cluster close to OOM? Use worst_case=true and 64 GiB per node
```
→ `citus_memory_risk_report` with `{"worst_case":true, "node_ram_bytes":68719476736}` — 13-term budget (shared_buffers, wal_buffers, lock/pred-lock tables, WAL senders, logical decoding, bgworker baselines, per-backend process baseline + plan cache, Citus metadata + PG CacheMemoryContext, `work_mem × hash_mem_multiplier × (1 + parallel_workers_per_gather) × plan_ops_per_query`, temp_buffers, autovacuum, coord↔worker libpq buffers) compared against node RAM.

```
@citus-mcp Sample PostgreSQL MemoryContextStats on every node
```
→ `citus_worker_memcontexts` — live `pg_get_backend_memory_contexts()` fan-out across the cluster.

```
@citus-mcp What will happen to memory if I double the number of monthly partitions?
```
→ `citus_partition_growth_simulator` with `{"partitions_per_parent":24}`

```
@citus-mcp How many clients can each node handle? Compare coord-only, MX, and PgBouncer
```
→ `citus_connection_capacity` — returns effective max client backends under each deployment mode plus bottleneck explanation.

```
@citus-mcp Simulate 400 concurrent multi-shard queries and tell me when max_shared_pool_size saturates
```
→ `citus_connection_fanout_simulator` with `{"concurrent_clients":400}`

```
@citus-mcp Should I use PgBouncer session or transaction mode with this workload?
```
→ `citus_pooler_advisor`

```
@citus-mcp Size hardware for 5× projected load: 50k IOPS, 2 TB data, 200 active clients
```
→ `citus_hardware_sizer` with `{"projected_data_gb":2000,"projected_active_clients":200,"projected_iops":50000}`

```
@citus-mcp Is 32 shards the right number? Chart the trade-offs
```
→ `citus_shardcount_tradeoff_chart`

### MX & Node Addition

```
@citus-mcp Run pre-flight checks for adding a worker at newworker.prod:5432
```
→ `citus_add_node_preflight` with `{"host":"newworker.prod","port":5432}` — coordinator-side checklist (extensions, types, schemas, roles, version match, `max_locks_per_transaction`/`max_worker_processes`/`max_connections` floors, `wal_level=logical`, replication slots).

```
@citus-mcp Advise how to prepare newworker.prod:5432 before I call citus_add_node and generate a shell script
```
→ `citus_node_prepare_advisor` with `{"host":"newworker.prod","port":5432,"generate_script":true}`

```
@citus-mcp Will metadata sync succeed on the new node, or should I use nontransactional mode? Target has 16 GiB RAM
```
→ `citus_metadata_sync_risk` with `{"target_node_ram_gib":16}` — DDL count, locks-in-tx, duration, timeout correlations.

```
@citus-mcp Is the cluster ready for MX with 2 concurrent queries per peer and new nodes sized at max_connections=300?
```
→ `citus_mx_readiness` with `{"concurrent_queries_per_peer":2,"new_node_max_connections":300,"expected_clients_per_node":100}`

```
@citus-mcp Which existing worker should I snapshot from when adding a new one?
```
→ `citus_snapshot_source_advisor`

### Metadata, Extensions & Routing

```
@citus-mcp Deep metadata consistency check across every node
```
→ `citus_metadata_health` with `{"check_level":"deep","include_fixes":true}`

```
@citus-mcp Are all extensions at the same version on every node?
```
→ `citus_extension_drift_scanner`

```
@citus-mcp Measure planner overhead across the cluster
```
→ `citus_planner_overhead_probe`

```
@citus-mcp Detect routing drift — are any queries landing on the wrong shard?
```
→ `citus_routing_drift_detector`

```
@citus-mcp Run a synthetic probe to validate end-to-end correctness
```
→ `citus_synthetic_probe`

```
@citus-mcp Dump the session's active guardrails (statement timeout, read-only, row caps)
```
→ `citus_session_guardrails`

### Advisors (Read-Only Recommendations)

```
@citus-mcp Run the Citus advisor focused on skew and include SQL fixes
```
→ `citus_advisor` with `{"focus":"skew","include_sql_fixes":true}`

```
@citus-mcp Analyze configuration, all categories, warn level and above
```
→ `citus_config_advisor` with `{"severity_filter":"warning"}`

```
@citus-mcp Deep inspection of PostgreSQL + Citus GUCs
```
→ `citus_config_deep_inspect`

```
@citus-mcp Advise on shard count per distributed table given current/projected load
```
→ `citus_shard_advisor`

```
@citus-mcp Which tables would benefit from columnar storage?
```
→ `citus_columnar_advisor`

```
@citus-mcp Show risky tenants (heavy shards, hot locks, outsized query share)
```
→ `citus_tenant_risk`

```
@citus-mcp Investigate which queries are pathological (cross-shard joins, broadcasts, repartitions)
```
→ `citus_query_pathology`

```
@citus-mcp Inspect the connected PgBouncer for pool health
```
→ `citus_pgbouncer_inspector`

### Shard & Skew Analysis

```
@citus-mcp Skew report for the orders table by row count
@citus-mcp Heatmap of the hottest shards in the last interval
```
→ `citus_shard_skew_report`, `citus_shard_heatmap`

```
@citus-mcp Estimate the cost (time, WAL, bytes) of rebalancing the current cluster
```
→ `citus_rebalance_cost_estimator`

### Time-Series / Snapshots

```
@citus-mcp Record a snapshot named pre-deploy-2026-04
@citus-mcp List snapshots taken in the last 30 days
@citus-mcp Trend query latency over the last 7 days
@citus-mcp Detect regressions against the pre-deploy-2026-04 snapshot
@citus-mcp Project 12-month growth from snapshots
@citus-mcp What changed between snapshots A and B?
```
→ `citus_snapshot_record`, `citus_snapshot_list`, `citus_trend`, `citus_regression_detect`, `citus_growth_projection`, `citus_what_changed`

### Alarms

```
@citus-mcp List open alarms
@citus-mcp Acknowledge alarm abc-123
@citus-mcp Clear all resolved alarms older than 7 days
```
→ `citus_alarms_list`, `citus_alarms_ack`, `citus_alarms_clear`

### Execute Operations (Approval Required)

```
@citus-mcp Plan a rebalance limited to 10 shard moves
@citus-mcp Request an approval token for rebalance
@citus-mcp Execute rebalance with token <token>
@citus-mcp Check rebalance status
```
→ `citus_rebalance_plan` → `citus_request_approval_token` → `citus_rebalance_execute` → `citus_rebalance_status`

```
@citus-mcp Move shard 102008 from 10.0.0.5:5432 to 10.0.0.6:5432
```
→ `citus_move_shard_plan` + `citus_move_shard_execute` (with approval token)

```
@citus-mcp Isolate tenant 42 to its own shard
@citus-mcp Clean up orphaned shard placements left by a failed move
```
→ `citus_isolate_tenant`, `citus_cleanup_orphaned`

---

## 📚 Tools Reference

All tool names below can be invoked directly as MCP tool calls. Most
accept an optional object with the listed parameters; `—` means the tool
takes no input.

### 🧭 Reports & Covering Tools

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_full_report` | `partition_growth_factor?`, `memory_budget_bytes?`, `include_verbose_outputs?`, `synthetic_probe?`, `pgbouncer_admin_dsn?`, `hardware_sizer_targets?` | Single covering tool. Runs every read-only diagnostic + advisor (~30 tools) and produces one consolidated report with overall health, top findings, top recommendations, and per-tool sections. Execute-class and input-specific tools are intentionally excluded. |

**Example — `citus_full_report`:**

```jsonc
// Default invocation: runs all read-only diagnostics with sensible defaults
{}

// With partition what-if and a tighter memory budget
{
  "partition_growth_factor": 2.0,
  "memory_budget_bytes": 4294967296
}

// Include optional synthetic probe + raw per-tool outputs (verbose)
{
  "synthetic_probe": true,
  "include_verbose_outputs": true
}
```

### Core Inspection (read-only)

| Tool | Parameters | Description |
|------|------------|-------------|
| `ping` | `message?` | Liveness check |
| `server_info` | — | Server metadata, build info, mode |
| `list_nodes` | `limit?`, `offset?` | Coordinator + workers |
| `list_distributed_tables` | `limit?`, `offset?` | Legacy table listing |
| `list_shards` | `limit?`, `offset?` | Shards with placements |
| `citus_cluster_summary` | `all?`, `include_workers?`, `include_gucs?`, `include_config?`, `include_health?`, `include_operational?` | Full cluster overview; `all: true` enables every section including the cross-tool health rollup |
| `citus_list_distributed_tables` | `schema?`, `table_type?`, `limit?`, `cursor?` | Paginated distributed-table list |
| `citus_list_reference_tables` | `schema?`, `limit?`, `cursor?` | Paginated reference-table list |
| `citus_table_inspector` | `table` *(req)*, `include_shards?`, `include_indexes?` | Table deep-dive |
| `citus_colocation_inspector` | `colocation_id?`, `limit?` | Colocation groups + members |

### Monitoring & Activity

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_activity` | `limit?`, `include_idle?`, `min_duration_secs?` | Cluster-wide active queries |
| `citus_lock_inspector` | `include_locks?`, `limit?` | Lock waits and blockers |
| `citus_job_inspector` | `state?`, `include_tasks?`, `limit?` | Background-job progress |
| `citus_shard_heatmap` | `table?`, `metric?`, `group_by?`, `limit?` | Hot-shard map |
| `citus_shard_skew_report` | `table?`, `metric?`, `include_top_shards?` | Skew analysis |
| `citus_explain_query` | `sql` *(req)*, `analyze?`, `verbose?`, `costs?` | Distributed EXPLAIN |
| `citus_proactive_health` | `long_tx_seconds?`, `idle_in_tx_seconds?`, `stuck_prepared_xact_seconds?`, `bloat_*?`, `include_workers?` | Long-tx / 2PC / bloat / saturation dashboard |

### Memory & Capacity Planning

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_metadata_cache_footprint` | `typical_tables_touched?`, `hot_path_tables_touched?`, `memory_budget_bytes?`, `partition_growth_factor?`, `skip_worst_case_scan?`, `skip_query_correlation?` | Three-regime per-backend Citus metadata cache estimate (typical/hot-path/worst-case), concurrent-cap headroom, live worst-case backend detection, partition-explosion simulator, pg_stat_statements query correlation |
| `citus_pg_cache_footprint` | `include_shard_rels?` | Per-backend PG CacheMemoryContext estimate |
| `citus_worker_memcontexts` | `top_n?` | `pg_get_backend_memory_contexts()` fan-out |
| `citus_partition_growth_simulator` | `partitions_per_parent`, `period_days?` | Projects cache cost of adding partitions |
| `citus_memory_risk_report` | `node_ram_bytes?`, `node_ram_bytes_by_node?`, `worst_case?`, `warn_pct?`, `crit_pct?`, `include_coordinator?`, `per_backend_app_overhead_bytes?`, `plan_ops_per_query?` | OOM risk rollup per node — 13 consumer terms: shared_buffers, wal_buffers, lock_table (`max_locks_per_transaction × MaxBackends × 270 B`), pred_lock_table, prepared_xact_state, wal_senders, logical_decoding, bgworker_baseline, backend_process_baseline, per_backend_caches (Citus MetadataCache + PG CacheMemoryContext), work_mem_peak (applies `hash_mem_multiplier` + `max_parallel_workers_per_gather` + `plan_ops_per_query`), temp_buffers, autovacuum_budget, plus coord↔worker libpq connection buffers on the coordinator. |
| `citus_connection_capacity` | `safety_fraction?`, `per_backend_override?`, `include_coordinator?` | Effective client max per deployment mode |
| `citus_connection_fanout_simulator` | `concurrent_clients`, `peer_count?` | Simulates `max_adaptive × peers × clients` pressure |
| `citus_pooler_advisor` | `expected_clients?`, `mode?` | PgBouncer session vs transaction guidance |
| `citus_hardware_sizer` | `projected_data_gb?`, `projected_active_clients?`, `projected_iops?`, `retention_days?` | Hardware sizing for current/projected load |
| `citus_shardcount_tradeoff_chart` | `table?`, `candidate_counts?` | Shard-count trade-off chart |

### MX / Node Addition

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_add_node_preflight` | `host` *(req)*, `port?`, `database?` | Coordinator-side checklist for adding a worker |
| `citus_node_prepare_advisor` | `host` *(req)*, `port?`, `database?`, `generate_script?` | Preparation steps + optional shell script |
| `citus_metadata_sync_risk` | `target_max_locks_per_transaction?`, `target_node_ram_gib?`, `assume_sync_mode?` | Estimates `citus_activate_node` work and timeout/OOM/lock risks. Returns `recommended_max_locks_per_transaction` (concrete multiple-of-64 value) when current setting would exhaust the shared lock table, plus the exact `ALTER SYSTEM SET …` statement in `recommendations[]`. |
| `citus_mx_readiness` | `expected_clients_per_node?`, `target_node_ram_gib?`, `new_node_max_connections?`, `concurrent_queries_per_peer?` | Mesh-connection budgeting before MX |
| `citus_snapshot_source_advisor` | `strategy?`, `max_candidates?`, `include_simulation?` | Picks best source worker for snapshot-based add |

### Forensics & Recovery

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_2pc_recovery_inspector` | `in_flight_threshold_seconds?` (default 60), `stuck_orphan_seconds?` (default 600), `include_non_citus?`, `suppress_recovery_script?` | Forensics for stuck 2PC / prepared-transaction state. Fans out `pg_prepared_xacts` across coord + workers, parses Citus GIDs (`citus_<group>_<pid>_<txn>_<conn>` — strict: 3 or 4 parts, non-negative group/pid, parseable conn when present), correlates with `pg_dist_transaction` keyed by `(groupid, gid)` (not gid alone — not unique across groups) and `get_all_active_transactions()`, classifies each as `commit_needed` / `rollback_needed` / `in_flight` / `foreign_initiator` / `non_citus`, and emits a ready-to-run per-node recovery script. `foreign_initiator` mirrors Citus's own `RecoverWorkerTransactions` filter (`LIKE 'citus_<localGroupId>_%'`): worker-initiated 2PCs are recovered by that worker's maintenance daemon, not the coordinator, so no COMMIT/ROLLBACK SQL is emitted for them. Generated recovery script uses standalone top-level statements (PostgreSQL forbids `COMMIT PREPARED` inside a transaction block). Emits `two_pc.commit_backlog`, `two_pc.orphans` (critical if older than `stuck_orphan_seconds`), `two_pc.slow_in_flight` alarms. Read-only — never issues COMMIT/ROLLBACK itself. |
| `citus_rebalance_forensics` | `job_id?`, `stall_threshold_seconds?` (default 300), `lookback_hours?` (default 24), `include_finished_tasks?`, `retries_exhausted_threshold?` (default 3), `cleanup_backlog_threshold?` (default 100) | Diagnoses **why** a rebalance or background job is stalled. Reads `pg_dist_background_job/_task/_depend`, correlates running tasks with `pg_stat_activity` wait events and `pg_blocking_pids` (pulls each blocker's PID + current query + **strongest granted lock mode** — the waiter's requested mode is not a reliable DDL signal), counts `pg_dist_cleanup` backlog, reads `citus.max_background_task_executors`. Computes `in_backoff` server-side as `not_before > now()` (matches Citus `metadata_utility.c` runnable semantics) so clock skew doesn't confuse classification. Stall classes: `blocked_by_ddl` (blocker query is DDL OR holds AccessExclusive/Exclusive) / `blocked_by_lock` / `bg_worker_starvation` / `retry_backoff` / `error_with_retries_exhausted` / `finished_with_errors` (evaluated BEFORE `cleanup_backlog` so a failed job that left cleanup residue is classified by its real cause) / `cleanup_backlog` / `no_stall`. Emits a concrete recovery playbook (stop + cleanup + resume SQL) and alarms. |

**Example — `citus_rebalance_forensics`:**

```jsonc
// Default: find most recent stuck/running job within last 24h
{}

// Diagnose a specific job, include finished tasks for postmortem
{"job_id": 42, "include_finished_tasks": true}

// Tighter thresholds: flag retries earlier, warn on smaller cleanup backlogs
{"retries_exhausted_threshold": 1, "cleanup_backlog_threshold": 10}
```

| `citus_placement_integrity_check` | `schemas?` (default `["public"]`), `skip_size_drift?`, `size_drift_factor?` (default 2.0), `max_rows_per_class?` (default 200), `check_inactive_placements?` (default true) | Three-way cross-check between Citus metadata (`pg_dist_placement` × `pg_dist_shard` × `pg_dist_node`), on-disk reality on every worker (`pg_class` scan via `run_command_on_workers` with the Citus shard-visibility hook disabled), and `pg_dist_cleanup`. Detects: (a) **ghost_placements** — metadata references shards missing on disk (reads WILL error); (b) **orphan_tables** — shard-suffix tables on workers with no placement row (flagged whether or not queued in `pg_dist_cleanup`); (c) **inactive_with_data** — `shardstate != 1` but data still materialised; (d) **size_drifts** — `pg_relation_size` vs `placement.shardlength` deviates > `size_drift_factor`; (e) **stale_stats** — `placement.shardlength = 0` but disk has data (silently causes rebalancer to make bad decisions; fix with `SELECT citus_update_shard_statistics(shardid)` or `citus_update_table_statistics(relname)`). Orphan classification matches on `(base_name, shardid)` pairs against metadata (not base name alone) so user tables like `events_123456` with no matching shardid are not flagged. **Trade-off:** if a shard was fully removed (placement row gone, never queued for cleanup) but its on-disk table remains, it will not be detected as an orphan by the pair matcher — use `SELECT run_command_on_workers($$SELECT oid::regclass FROM pg_class WHERE relname ~ '_[0-9]+$'$$);` as a belt-and-braces sweep for that case. Workers whose fanout fails are listed in `skipped_workers[]` with `partial_results=true`; ghost detection is suppressed for those nodes to avoid false-positive critical alarms. Emits a per-class reconciliation playbook (`citus_copy_shard_placement` / `citus_cleanup_orphaned_resources` / `citus_update_shard_statistics`) and alarms (`placement.ghost`, `placement.orphan_tables`, `placement.inactive_with_data`, `placement.stale_stats`, `placement.partial_results`). Read-only. |

**Example — `citus_placement_integrity_check`:**

```jsonc
// Default — scan public schema, detect all 4 classes
{}

// Multi-schema, skip size drift for speed
{"schemas": ["public", "tenants"], "skip_size_drift": true}

// Strict size drift (flag any > 1.5× deviation)
{"size_drift_factor": 1.5}
```


**Example — `citus_2pc_recovery_inspector`:**

```jsonc
// Default — detect commit_needed / rollback_needed / in_flight / foreign_initiator
{}

// Aggressive: treat anything >10s as orphan, escalate orphans older than 2 min
{"in_flight_threshold_seconds": 10, "stuck_orphan_seconds": 120}

// Compact output (no recovery script)
{"suppress_recovery_script": true}

// Include non-citus GIDs in the output (e.g. for third-party 2PC coordinators)
{"include_non_citus": true}
```

### Metadata / Extensions / Routing

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_metadata_health` | `check_level?` (`basic`/`thorough`/`deep`), `include_fixes?` | Cross-node metadata consistency |
| `citus_extension_drift_scanner` | `extensions?` | Version/availability drift across nodes |
| `citus_planner_overhead_probe` | `iterations?` | Planner-time measurement |
| `citus_routing_drift_detector` | `sample_size?` | Detects queries routing to unexpected shards |
| `citus_synthetic_probe` | `verbose?` | End-to-end synthetic correctness probe |
| `citus_session_guardrails` | — | Active guardrail settings on current session |

### Advisors (read-only)

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_advisor` | `focus?` (`skew`/`ops`), `max_tables?`, `include_next_steps?`, `include_sql_fixes?` | Top-level SRE advisor |
| `citus_config_advisor` | `include_all_gucs?`, `category?`, `severity_filter?`, `total_ram_gb?` | Configuration analysis |
| `citus_config_deep_inspect` | — | Full PG + Citus GUC deep dive |
| `citus_shard_advisor` | `table?`, `projected_load_multiplier?` | Shard-count recommendation |
| `citus_columnar_advisor` | — | Columnar-storage candidates |
| `citus_tenant_risk` | `top_n?` | Risky tenants |
| `citus_query_pathology` | `limit?`, `window_secs?` | Pathological distributed queries |
| `citus_pgbouncer_inspector` | `dsn?` | Connected PgBouncer diagnostics |
| `citus_rebalance_cost_estimator` | `table?`, `strategy?` | Estimates rebalance cost (time, WAL, bytes) |
| `citus_validate_rebalance_prereqs` | `table` *(req)* | Rebalance readiness checklist |

### Alarms

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_alarms_list` | `kind?`, `severity?`, `acked?`, `limit?` | List alarms emitted by other tools |
| `citus_alarms_ack` | `id` *(req)* | Acknowledge |
| `citus_alarms_clear` | `older_than_seconds?`, `kind?` | Bulk clear |

### Time-Series / Snapshots

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_snapshot_record` | `name?`, `tags?` | Record a snapshot (opt-in SQLite) |
| `citus_snapshot_list` | `limit?`, `since?` | List snapshots |
| `citus_trend` | `metric` *(req)*, `window_days?` | Trend line for a metric |
| `citus_regression_detect` | `baseline` *(req)* | Compare against a baseline snapshot |
| `citus_growth_projection` | `metric` *(req)*, `horizon_months?` | Linear/exponential projection |
| `citus_what_changed` | `a` *(req)*, `b` *(req)* | Diff between two snapshots |

### Execute Operations (require approval token)

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_request_approval_token` | `action` *(req)*, `ttl_seconds?` | HMAC-signed approval token |
| `citus_rebalance_plan` | `table?`, `threshold?`, `max_shard_moves?`, `drain_only?` | Preview rebalance |
| `citus_rebalance_execute` | `approval_token` *(req)*, `table?`, `threshold?` | Start rebalance |
| `citus_rebalance_status` | `verbose?`, `limit?`, `cursor?` | Rebalance progress |
| `citus_move_shard_plan` | `shard_id`, `source_host`, `source_port`, `target_host`, `target_port`, `colocated?` | Preview shard move |
| `citus_move_shard_execute` | `approval_token` *(req)*, `shard_id`, `source_*`, `target_*`, `colocated?`, `drop_method?` | Execute shard move |
| `citus_isolate_tenant` | `approval_token` *(req)*, `table`, `tenant_id` | Isolate tenant to its own shard |
| `citus_cleanup_orphaned` | `approval_token` *(req)*, `dry_run?` | Clean up orphaned placements |
| `rebalance_table_plan` | `table` *(req)* | Legacy per-table rebalance plan |
| `rebalance_table_execute` | `table` *(req)*, `approval_token` *(req)* | Legacy per-table rebalance execute |

---

## 📋 Built-in Prompts

Use these prompts in Copilot Chat for guided workflows:

| Prompt | Description |
|--------|-------------|
| `/citus.health_check` | Cluster health checklist |
| `/citus.rebalance_workflow` | Step-by-step rebalance guide |
| `/citus.skew_investigation` | Skew investigation playbook |
| `/citus.ops_triage` | Operational triage workflow |

---

## 🔐 Security

### Read-Only Mode (Default)

By default, citus-mcp runs in **read-only mode**. This means:
- ✅ All inspection and monitoring tools work
- ✅ Advisors provide recommendations
- ❌ Execute operations are disabled
- ❌ No data can be modified

### Admin Mode with Approval Tokens

To enable execute operations:

1. **Set admin mode** in configuration:
   ```yaml
   mode: admin
   allow_execute: true
   approval_secret: your-secret-key-here
   ```

2. **Request an approval token** before executing:
   ```
   @citus-mcp Request approval token for rebalance
   ```

3. **Use the token** in the execute command:
   ```
   @citus-mcp Execute rebalance with token: <token>
   ```

Tokens are time-limited and action-specific (HMAC-signed).

---

## 🔧 Troubleshooting

### Connection Issues

**Error: `connection refused`**
- Verify the coordinator host and port are correct
- Check that PostgreSQL is running and accepting connections
- Ensure firewall rules allow the connection

**Error: `authentication failed`**
- Verify username and password in DSN
- Check that the user has permissions on the database
- For SSL issues, try `sslmode=disable` for local testing

### MCP Issues

**Copilot doesn't see citus-mcp**
- Ensure `mcp.json` is in the correct location
- Check that the command path is absolute
- Reload VS Code after changing configuration

**Tools return errors**
- Check logs: `CITUS_MCP_LOG_LEVEL=debug bin/citus-mcp`
- Verify Citus extension is installed: `SELECT * FROM pg_extension WHERE extname = 'citus'`

### Testing Connection

```bash
# Test directly
CITUS_MCP_COORDINATOR_DSN="postgres://..." bin/citus-mcp

# Then send a ping via stdin
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | bin/citus-mcp
```

---

## 🛠️ Development

### Running Tests

```bash
# Unit tests
make test

# With verbose output
go test -v ./...

# Integration tests (requires Docker)
make docker-up
make integration
make docker-down
```

### Linting

```bash
make lint
```

### Project Structure

```
citus-mcp/
├── cmd/citus-mcp/       # Main entry point
├── internal/
│   ├── mcpserver/       # MCP server implementation
│   │   ├── tools/       # Tool implementations (71 tools)
│   │   ├── prompts/     # Prompt templates
│   │   └── resources/   # Static resources
│   ├── db/              # Database layer and worker management
│   ├── citus/           # Citus-specific logic and queries
│   │   ├── advisor/     # Advisor implementations
│   │   └── guc/         # GUC (configuration) analysis
│   ├── cache/           # Query result caching
│   ├── config/          # Configuration management
│   ├── errors/          # Error types and codes
│   ├── fanout/          # Parallel query execution
│   ├── logging/         # Structured logging
│   └── safety/          # Guardrails and approval tokens
├── docker/              # Docker Compose setup for testing
├── docs/                # Additional documentation
└── tests/               # Integration tests
```

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**[⬆ Back to Top](#citus-mcp-server)**

Made with ❤️ for the Citus community

</div>
