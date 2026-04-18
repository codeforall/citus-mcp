# 📣 Citus MCP — Your AI Co-Pilot for Citus Clusters

**TL;DR** — Citus MCP plugs into any MCP-compatible AI client (Copilot, Claude
Desktop, Cursor, VS Code, …) and turns plain English questions into deep,
read-only diagnostics against your live Citus cluster. **No SQL knowledge
required.** It catches problems *before* they page you, and tells you exactly
what hardware and configuration the cluster needs for the load you actually
have — and the load you're planning for.

---

## 🎯 What it does for you

| I want to… | Just ask |
|---|---|
| Know if my cluster is healthy right now | *"Run a full health report on the Citus cluster."* |
| Find out why the cluster feels slow today | *"What's wrong with my Citus cluster?"* |
| Prevent an OOM tomorrow | *"Is my cluster at risk of running out of memory?"* |
| Size a new cluster | *"I expect 10 TB and 2 000 clients — what hardware do I need?"* |
| Add capacity safely | *"I want to add a worker node. What should I check first?"* |
| Pick the right shard count for a new table | *"How many shards should `orders` have?"* |
| Decide if I need PgBouncer | *"Will a connection pooler help my workload?"* |
| Recover from a stuck rebalance | *"Why is my rebalance stuck and how do I unstick it?"* |
| Find stuck 2PC transactions | *"Are there any hung distributed transactions?"* |
| Check metadata integrity | *"Is every shard actually on disk where Citus thinks it is?"* |

**You don't run SQL. You don't read `pg_catalog`. You ask a question — Citus MCP runs
~35 specialized diagnostics (up to ~30 on a typical cluster; tools that can't run
surface a structured skip reason, never silence), correlates the answers, and tells
you in plain English what's wrong and what to do about it.**

---

## 🚨 The problems it catches

Citus MCP was built to answer the three questions that wake up database
operators at 3 AM:

### 1. **"Why did the cluster OOM?"**

One real-world trigger: a team doubled the number of partitions on their
distributed tables. Data volume didn't change. Query pattern didn't change.
Nodes started OOM-killing anyway.

**Cause:** every backend caches Citus metadata + Postgres catalog entries
per-partition per-shard. Double the partitions → double the metadata cache
per connection → multiply by `max_connections` → OOM.

**Citus MCP catches this before it happens:**

> **You:** *"I'm planning to double the partition count. Is that safe?"*
>
> **MCP runs:** `citus_partition_growth_simulator` + `citus_memory_risk_report`
>
> **Answer:** *"With 250 concurrent backends, adding 1 000 partitions will push
> each backend's metadata cache from 180 MB to 420 MB. Projected per-node RAM
> usage: 127 GB (you have 96 GB). This will OOM under load."*

### 2. **"How many clients can this cluster actually handle?"**

Every client connection is not just a Postgres backend — on Citus, each backend
opens N×M fan-out connections to workers. MX changes the math again. Pooling
changes it a third time.

> **You:** *"How many client connections can my cluster safely handle?"*
>
> **MCP runs:** `citus_connection_capacity` + `citus_connection_fanout_simulator`
>
> **Answer:** *"In coordinator-only mode: 240 clients. In MX mode: 800 clients.
> Your current `max_connections=200` is already the bottleneck in MX mode.
> Adding PgBouncer (transaction mode) raises effective ceiling to 3 200."*

### 3. **"Why is X stuck?"**

Rebalances stall. 2PC transactions hang. Shards end up on disk without
metadata rows (or vice versa). Citus MCP has a dedicated forensics tool for
each of these — with ready-to-run recovery SQL.

> **You:** *"My rebalance has been running for 4 hours. What's happening?"*
>
> **MCP runs:** `citus_rebalance_forensics`
>
> **Answer:** *"Task #42 has been running for 4h12m, blocked by PID 8341 which
> is executing `ALTER TABLE events ADD COLUMN ...` (holds AccessExclusiveLock).
> Classification: `blocked_by_ddl`. Playbook: cancel the DDL, then resume the
> rebalance. Copy-paste recovery SQL below."*

---

## 🧠 The four things it's best at

### A. 🩺 **Cluster health in one command**

> **You:** *"Run a full report."*
>
> **MCP tool:** `citus_full_report` — runs ~35 diagnostics, rolls up overall
> health (🟢 healthy / 🟡 warning / 🔴 critical), prioritized findings, and
> concrete recommendations with exact SQL. Tools that can't run on your
> cluster (e.g. `pg_stat_statements` not installed) surface a structured skip
> reason — you never get silent coverage gaps.

You get:
- Every mis-configuration (max_connections, max_locks_per_transaction, wal_level, …)
- Memory/OOM risk per node
- Data skew, hot shards, hot tenants
- Stuck transactions, lock waits, bg-worker starvation
- Metadata drift between coordinator + workers
- Extension version drift
- Routing drift (queries going to unexpected shards)

### B. 🧮 **Hardware sizing — current and projected**

> **You:** *"How big should my Citus cluster be if I expect 20 TB of data,
> 1 500 concurrent clients, and a write-heavy OLTP workload?"*
>
> **MCP tool:** `citus_hardware_sizer`
>
> **Answer:** RAM, CPU cores, disk IOPS, disk GB per node, worker count,
> shard count — with the math shown.

Plus:
- `citus_memory_risk_report` — per-node OOM risk with **13 memory consumers**
  accounted for (shared_buffers, wal_buffers, lock table, prepared-xact state,
  WAL senders, logical decoding, bgworker baseline, per-backend caches,
  `work_mem × hash_mem_multiplier × parallel_workers`, temp_buffers,
  autovacuum, Citus libpq buffers).
- `citus_metadata_cache_footprint` — three-regime estimate (typical /
  hot-path / worst-case) of the hidden per-backend cache growth.
- `citus_shardcount_tradeoff_chart` — pick the right shard count for each table.

### C. 🔍 **Advisors that teach you Citus as they answer**

All of these return explanations, not just raw numbers:

- `citus_advisor` — top-level SRE + performance advice
- `citus_config_advisor` — Postgres/Citus GUC review
- `citus_shard_advisor` — per-table shard-count recommendation
- `citus_columnar_advisor` — which tables should use columnar storage
- `citus_pooler_advisor` — PgBouncer session vs. transaction mode
- `citus_tenant_risk` — hot / risky / noisy tenants
- `citus_query_pathology` — identifies pathological distributed queries

### D. 🚑 **Forensics when things are already broken**

Three tools that go deep when something's wrong:

- `citus_2pc_recovery_inspector` — classifies every prepared transaction
  (`commit_needed` / `rollback_needed` / `in_flight` / `foreign_initiator` /
  `non_citus`), emits a ready-to-run recovery script. Read-only — it never
  fixes things without your approval.
- `citus_rebalance_forensics` — explains *why* a rebalance is stuck
  (blocked_by_ddl / blocked_by_lock / bg_worker_starvation / retry_backoff /
  cleanup_backlog / finished_with_errors) with exact remediation SQL.
- `citus_placement_integrity_check` — three-way cross-check between Citus
  metadata, on-disk reality, and pg_dist_cleanup. Finds ghost placements
  (reads will fail!), orphan tables, stale statistics (rebalancer will make
  bad decisions), partial-scan warnings when a worker is unreachable.

---

## 🧑‍💻 Example conversations

### Example 1 — The Monday-morning check-in

> **You:** *"Give me a full health report of the Citus cluster."*
>
> **MCP:** Overall: 🟡 WARNING.
>
> Top findings:
> 1. Node `worker-2` memory pressure: 87% RSS utilization; `citus_memory_risk_report` projects OOM within ~48h at current connection growth rate.
> 2. Shard skew: colocation group #11 max/avg = 124× — hot shard `public.events:102200` on worker-0. Consider `SELECT isolate_tenant_to_new_shard('public.events', <tenant_value>)`.
> 3. `max_locks_per_transaction=64` on coordinator — below the recommended ≥256 for your partition count (will block `citus_activate_node`).
>
> Exact fixes below (3 SQL snippets).

### Example 2 — Planning a traffic spike

> **You:** *"We expect 3× more clients for Black Friday. Can the cluster take it?"*
>
> **MCP runs:** `citus_connection_capacity` + `citus_connection_fanout_simulator` + `citus_memory_risk_report` + `citus_pooler_advisor`.
>
> **Answer:** *At 3× clients you'll exceed `max_connections` on the coordinator
> (current 200, need 580). Per-backend metadata cache plus PG catalog cache
> will push per-node RSS to 118 GB (you have 96 GB) → OOM is likely. Three
> options, ranked:*
> *  1. Add PgBouncer in **transaction mode** — effective capacity 6× with zero hardware change. ⚠️ Requires app to be stateless at the transaction boundary: no session GUCs, no `SET LOCAL` across statements, no prepared statements outside a tx; Citus cached worker connections become useless; 2PC affinity is lost. MCP prints this full caveat list — not just the speedup.
> *  2. Enable MX — doubles capacity but requires `max_locks_per_transaction ≥ 512`.
> *  3. Vertical scale to 128 GB RAM per worker — expensive, slowest to deploy.

### Example 3 — New table, what shard count?

> **You:** *"I'm about to create an `events` table, expected to grow to 2 TB with 50 M rows/day, distributed by `tenant_id`. How many shards?"*
>
> **MCP runs:** `citus_shard_advisor` + `citus_shardcount_tradeoff_chart`.
>
> **Answer:** *Recommended `shard_count = 64`. Reasoning: 2 TB / 64 = 32 GB
> per shard (within 10–50 GB sweet spot). Co-locate with your existing
> `users` table (also 64 shards). Trade-off chart shows 128 shards would
> over-fragment (metadata cache cost) and 32 shards would hot-spot under
> your projected tenant distribution.*

### Example 4 — Adding a worker node

> **You:** *"I want to add `worker-3` to the cluster. Am I ready?"*
>
> **MCP runs:** `citus_add_node_preflight` + `citus_metadata_sync_risk`.
>
> **Answer:** *3 pre-flight issues:*
> *  1. `max_locks_per_transaction=64` on new worker → raise to 512 before `citus_add_node` (metadata sync will fail with "out of shared memory").*
> *  2. `wal_level=replica` → set to `logical` (required: Citus shard moves and the rebalancer use logical replication by default).*
> *  3. Projected sync payload ~34 MB / 18 000 metadata objects — allow 2–4 min of elevated coordinator CPU.*
>
> *Exact `ALTER SYSTEM SET …` commands below.*

### Example 5 — The OOM post-mortem

> **You:** *"`worker-1` OOM'd last night. Could Citus metadata cache be the cause?"*
>
> **MCP runs:** `citus_metadata_cache_footprint` + `citus_worker_memcontexts` + `citus_memory_risk_report worst_case:true`.
>
> **Answer:** *Yes — very likely. You have 1 200 partitions × 32 shards = 38 400 metadata entries. Under worst-case query patterns (cross-partition DML), each backend caches ~540 MB. With 120 active backends, that's 65 GB in caches alone on top of shared_buffers — which matches the OOM kill timing in your logs.*
>
> *Fixes:*
> *  1. Reduce `max_connections` or add PgBouncer (highest leverage).*
> *  2. Consolidate rarely-queried partitions.*
> *  3. Increase RAM.*

### Example 6 — Stuck rebalance

> **You:** *"My rebalance job 42 has been running since yesterday — help."*
>
> **MCP runs:** `citus_rebalance_forensics` with `job_id: 42`.
>
> **Answer:** *Classification: `bg_worker_starvation`. Cause: `citus.max_background_task_executors=1`, you have 6 runnable tasks. Playbook:*
>
> ```sql
> ALTER SYSTEM SET citus.max_background_task_executors = 4;
> SELECT pg_reload_conf();
> -- rebalance will pick up the new limit within 15s
> ```

---

## 🛡️ Safety model

- **Read-only by default.** The inspection, forensics, and advisor tools
  never mutate anything.
- **Destructive operations (rebalance, shard move, tenant isolate, cleanup)
  require a signed approval token** — no accidental changes.
- **Everything it does is auditable** — every tool call is logged with
  arguments and results.
- **Works in coordinator-only mode** — no need to open direct connections
  to every worker; fan-out happens via `run_command_on_workers`.

## 🎯 Precision over volume

- **Every formula cites the Citus/PG source line it's derived from.** See
  `docs/methodology.md` for per-rule provenance (e.g. `max_replication_slots`
  is derived from `citus.max_background_task_executors_per_node` +
  `pg_replication_slots` count + a fixed 2-slot headroom, not a magic number).
- **Citus product defaults are never flagged as CRITICAL.** A setting that
  ships enabled by default (e.g. `citus.multi_shard_modify_mode=parallel`)
  only becomes a finding when combined with a known-dangerous context.
- **Severity of a finding is separate from the class of the GUC it touches.**
  A `critical-class` GUC with no drift reports `drifted=false, severity=""`,
  not `severity=critical`. Counting "critical findings" means what you think
  it means.
- **Silent tools are a bug.** When a diagnostic can't run (e.g.
  `pg_stat_statements` not installed, coordinator unreachable, input
  missing), it returns a structured `{tool, reason, detail}` in
  `tools_skipped` — never an empty response.

---

## 🏁 Getting started in 2 minutes

```bash
# 1. Build
git clone <repo-url> && cd citus_mcp && make build

# 2. Point at your coordinator
export CITUS_MCP_COORDINATOR_DSN="postgres://user@coord-host:5432/db?sslmode=require"

# 3. Wire into your MCP client (VS Code / Copilot / Claude Desktop / Cursor)
#    See README.md → Quick Start.

# 4. Ask questions. In English.
```

Then open your AI client and try:

> *"Run a full health report on my Citus cluster and tell me the top 3 things to fix."*

That's it. No SQL. No dashboards. No alphabet-soup of `pg_stat_*` views.

---

## 📚 Where to go next

- **Full tool reference:** `README.md` → *Tools Reference* section (64 tools).
- **Feature list:** `README.md` → *Features*.
- **Methodology & formula provenance:** `docs/methodology.md`.
- **How MCP itself works:** <https://modelcontextprotocol.io>.
- **Feedback / bugs:** open an issue in this repo — feature ideas welcome.

---

**Try this first:** *"Run `citus_full_report` and give me the top findings
in plain English."* — if that one prompt doesn't immediately save you an
hour, I'll buy you coffee. ☕
