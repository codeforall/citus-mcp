# citus-mcp methodology

This document captures, per rule, **what** each advisor measures, **which
cluster facts drive it**, and **which assumptions are exposed as inputs**.
When a formula or threshold uses a constant, the constant is justified
here with a Citus or PostgreSQL source citation (file:line) so that a
reviewer can independently validate it against the running engine.

The goal is reproducibility: if two people run the same tool on the same
cluster they must get the same verdict, and the verdict must be defensible
from first-principles rather than authority.

---

## citus_shard_skew_report

### What it measures

Whether data is unevenly distributed across data-holding nodes **and**
whether any single shard carries a disproportionate slice of its table's
bytes (a hot-tenant indicator).

### Cluster facts that drive it

- `pg_dist_node(nodename, nodeport, noderole, shouldhaveshards, isactive)`
- `pg_dist_shard` ⨝ `pg_dist_shard_placement` ⨝ `pg_class` ⨝
  `pg_namespace`
- `pg_dist_partition(logicalrelid, partmethod, colocationid)` — partmethod
  `'n'` means reference table.
- `pg_catalog.citus_shards` (view) or `citus_shard_sizes()` function for
  per-shard byte sizes.

### Who is a "data-holding node"?

The Citus rebalancer itself uses `shouldhaveshards=true` as the
authoritative filter:

- `src/backend/distributed/operations/shard_rebalancer.c:567` — counts
  `shardAllowedNodeCount` only over `workerNode->shouldHaveShards`.
- `shard_rebalancer.c:651` and `:672` — early-return when
  `!shouldHaveShards`.
- `src/backend/distributed/operations/shard_transfer.c:1348` — a
  `SHARD_TRANSFER_MOVE` refuses to target a node with
  `!shouldHaveShards` and suggests
  `citus_set_node_property(..., 'shouldhaveshards', true)`.

So citus-mcp's skew tool considers a node "data-holding" iff
`shouldhaveshards=true AND isactive=true AND noderole='primary'`.
Coordinators, drained nodes, and secondaries are reported in `per_node`
for visibility but excluded from the cluster-wide skew metric.

### Degenerate topologies

When fewer than 2 data-holding nodes exist, max == min by construction,
so the cluster-wide skew metric is undefined. The tool returns
`skew.warning_level = "single_data_node"` and points the operator at
`per_colocation` / `hot_shards`, which remain meaningful regardless of
topology.

### Why per-colocation matters more than per-node

A skewed distribution key does not necessarily produce unequal per-node
bytes — Citus's hash distribution spreads shards evenly even if the
data inside each shard is unequal. The per-colocation `max/avg` ratio
captures this directly. The `critical >= 5x` / `warn >= 2x` thresholds
are chosen to flag 1-in-200 shard outliers (≥5σ for a uniform
hash-distributed dataset with typical row counts) while tolerating the
±20% wobble from real-world row-size variance. These are tunable via
input if the cluster's workload characteristics warrant.

### Remediation

`hot_shards[].remediation` emits a literal
`SELECT isolate_tenant_to_new_shard('schema.table', <tenant_value>, 'CASCADE');`
string. `CASCADE` is required whenever the table is in a colocation
group (see `src/backend/distributed/operations/shard_split.c` — the
function refuses without CASCADE when sibling tables exist).

### Inputs exposed

- `table` — optional schema-qualified name to narrow the analysis to one
  table.
- `metric` — `"bytes"` (default) or `"shard_count"`; the latter is a
  fallback when `citus_shards` / `citus_shard_sizes()` are unavailable.
- `include_top_shards` — include a global top-N shard list.

---

## citus_metadata_sync_risk

### What it measures

Likelihood that `citus_activate_node()` or a metadata-sync resync times
out, OOMs the worker, or exhausts the lock-hash table.

### Lock-hash capacity formula

PostgreSQL sizes its lock-hash table at server start with the
`NLOCKENTS` macro:

- `src/backend/storage/lmgr/lock.c` — `#define NLOCKENTS()
  mul_size(max_locks_per_xact, add_size(MaxBackends,
  max_prepared_xacts))`.
- `src/backend/utils/init/postinit.c`, `InitializeMaxBackends()`:
  `MaxBackends = MaxConnections + autovacuum_max_workers + 1 +
  max_worker_processes + max_wal_senders`.

So:

```
lock_table_capacity = max_locks_per_transaction
                    × (MaxBackends + max_prepared_transactions)
```

The tool fetches every input GUC from the coordinator and returns both
`max_backends` and `lock_table_capacity` under `coordinator_gucs` so the
operator can reproduce the math. The prior implementation used
`max_connections` instead of `MaxBackends` and under-reported capacity by
a factor of ~3.2 on a default PG17 install.

### Lock-in-tx demand

A Citus `activate_node` transaction acquires locks on every distributed
table and every auxiliary object it touches. An index is a separate
lockable object, so:

```
estimated_locks = distributed_tables × (1 + avg_indexes_per_table)
                + distributed_objects + 16
```

`avg_indexes_per_table` is measured live from
`pg_dist_partition ⨝ pg_index`. The old formula used a crude
`(tables + objects + colocations) × 1.5` with an arbitrary `min 64`
floor; the new one derives from object-lock semantics (one lock per
relation, one per index, one per function/sequence/etc).

### Soft cap / recommended lpt

- `hard_cap = lock_table_capacity` (exactly the NLOCKENTS number).
- `soft_cap = hard_cap / 2` with a floor of `lpt × 16`; we keep activate
  under half the lock table so other backends keep working.
- Recommended lpt when demand > soft:
  `ceil_to_64(2.5 × demand / (MaxBackends + max_prepared_transactions))`
  with a floor of 256.

---

## citus_connection_capacity

### What it measures

How many concurrent client connections each deployment shape
(coordinator-only, MX, PgBouncer session/transaction) can sustain before
some upstream limit is hit.

### Coord-only: two numbers, not one

Each distributed query fans out up to
`citus.max_adaptive_executor_pool_size` worker connections
(`src/backend/distributed/executor/adaptive_executor.c` GUC
registration). The **hard ceiling** assumes every coord backend does
this at the same instant:

```
hard = citus.max_shared_pool_size_worker / max_adaptive_executor_pool_size
```

That's the honest worst case. But it is not the realistic steady-state
number: under typical workloads only a fraction of backends are
actively fanning out at any moment (the rest are idle, running
single-shard queries that reuse cached connections, or serving plan
cache hits). The **sustainable** cap credits this:

```
sustainable = min_worker_shared_pool / (max_adaptive × fanout_concurrency)
```

`fanout_concurrency` is an input, default 0.5. Both numbers are
returned, with the assumption surfaced in `explanation`.

---


---

## citus_session_guardrails

### What it measures

Cluster-level GUCs that are risky in production. Rule severities follow
a strict discipline: a rule MUST NOT fire at `warning` or `critical` on
a shipped product default. Doing so erodes operator trust.

### Reading cluster-level values correctly

The obvious query `SELECT current_setting($1)` is **wrong** for this
tool. The citus-mcp pool itself injects per-connection RuntimeParams
(see `internal/db/connection.go` line 37:
`pcfg.ConnConfig.RuntimeParams["statement_timeout"] = ...`), so both
`current_setting()` and `pg_settings.setting` / `reset_val` reflect the
pool's override, not the cluster-level value a regular client backend
would observe.

The tool opens a one-shot `pgx.Conn` with `RuntimeParams = {}` and
reads GUCs through it. The resulting value reflects `postgresql.conf`
+ `ALTER SYSTEM` + role/db defaults, i.e. what a normal application
backend sees.

### Rule severities

Source citations for shipped defaults:

- `work_mem` default = 4 MB: `src/backend/utils/misc/guc_tables.c`
  (`"work_mem"` entry, `boot_val = 4096` kB).
- `statement_timeout` default = 0 (unbounded): same file.
- `idle_in_transaction_session_timeout` default = 0: same file.
- `citus.multi_shard_modify_mode` default = `parallel`:
  `src/backend/distributed/shared_library_init.c:2210-2218`
  (`DefineCustomEnumVariable` with `boot_val = PARALLEL_CONNECTION`).
- `citus.max_adaptive_executor_pool_size` default = 16: same file,
  `:1986-2001`.

Accordingly:

- `work_mem > 512 MiB` → **warning** (not a default; signal).
- `statement_timeout = 0` → **info** (product default).
- `idle_in_transaction_session_timeout = 0` → **info** (default).
- `citus.multi_shard_modify_mode = parallel` **alone** → NO alarm.
  This is the product default and the recommended mode for throughput.
- `citus.multi_shard_modify_mode = parallel` **AND**
  `citus.max_adaptive_executor_pool_size > adaptive_pool_high` (default
  32) → **warning**. Only in this combination does a single multi-shard
  write fan out enough connections per worker to risk exhausting
  `max_connections`.

### Inputs exposed

- `work_mem_mib_high` — default 512.
- `adaptive_pool_high` — default 32.


## config_deep_inspect — severity semantics (P0-#3)

**The bug.** Before this fix, every row in `guc_drifts` carried a
`severity` field that was actually the GUC's *class* (how bad drift
WOULD be on that GUC). Rows where `drifted=false` still reported
`severity=critical`, which made "count of critical findings" in any
consumer unreliable — a cluster with zero drift could appear to have
a dozen critical items.

**The fix.** Split the two concepts explicitly on `GucDriftReport`:

* `class` — the inherent criticality of drift on this GUC. Always
  populated. `wal_level` is critical because mismatch breaks logical
  replication; `timezone` is info because it only affects log
  timestamps.
* `severity` — the severity of THIS FINDING. Empty string when
  `drifted=false` (there IS no finding). Equals `class` when
  `drifted=true`.

Callers counting findings must look at `severity`; callers sorting
by inherent criticality (e.g. "show the critical-class GUCs first
regardless of whether they drifted") should look at `class`. The
stable sort in `ConfigDeepInspectTool` now keys on `class` for this
reason.

**Cluster facts that drive this.** Only the `driftRule` table and the
observed per-node `pg_settings.setting` values. No timing, no memory
probes. Pure function `buildGucDriftReport` is unit-tested (three
cases: non-drift, drift, unreachable-node).

**Inputs exposed.** None new. The refactor is backward-compatible in
JSON shape: `severity` is now empty-string on non-drifted rows, and a
new `class` field is added.

## full_report — "tools produced no actionable output" contract (P0-#4)

**The bug.** The aggregator's `run()` wrapper unconditionally added
every section that didn't return a hard error to `tools_run`, even
sections that bailed early with empty output (`cluster_summary` when
citus is not installed; `query_pathology`/`routing_drift_detector` when
pg_stat_statements is missing or empty; `proactive_health` when the
coordinator pool acquire fails). `tools_skipped` was populated only
from the SkipSections input and a couple of manual `append` calls.
Readers could not distinguish "tool ran and had no findings" from
"tool silently did nothing".

**The fix.** A uniform contract:

1. `tools_skipped` is now `[]SkipReason { tool, reason, detail }` — a
   structured list, not `[]string`.
2. Sections that cannot produce actionable output return
   `skipSection(reason, detail)`, which produces a sentinel `*skipError`.
3. The aggregator's `run()` wrapper inspects the error with
   `errors.As`: a `*skipError` routes the section to `tools_skipped`
   with the structured reason; a real error routes to `section_errors`;
   nil routes to `tools_run`.
4. `tools_skipped` is therefore the **single source of truth** for
   non-produced output.

Known skip reasons today:

| reason                             | emitted by                               |
|------------------------------------|------------------------------------------|
| `user_requested`                   | aggregator, on SkipSections match        |
| `citus_extension_not_installed`    | cluster_summary path in aggregator       |
| `coordinator_unreachable`          | proactive_health pool-acquire failure    |
| `pg_stat_statements_not_installed` | query_pathology, routing_drift_detector  |
| `no_statements_tracked`            | query_pathology, routing_drift_detector when pg_stat_statements is installed but empty/below thresholds |
| `no_admin_dsn`                     | pgbouncer_inspector without admin DSN    |
| `opt_in`                           | synthetic_probe not explicitly enabled   |
| `needs_target_inputs`              | hardware_sizer without target inputs     |

**Cluster facts that drive this.** None — it is a pure structural
change to the aggregator. The only per-cluster inputs that matter are
the boolean "is pg_stat_statements loaded" / "did coordinator pool
acquire succeed" etc., all already probed by the respective tool.

**Tested.** `full_report_skip_test.go` covers the error-routing table:
nil → tools_run, `*skipError` → tools_skipped with its reason
preserved, any other error → section_errors. Verified live on the
local test cluster: `pg_stat_statements` not installed →
`query_pathology` and `routing_drift` both appear in `tools_skipped`
with reason `pg_stat_statements_not_installed`, and neither appears
in `tools_run`.

## config_advisor — replication & worker-process formulas (P0-#5)

**The bug.** Three rules used magic numbers with no source backing:

* `max_worker_processes`: hardcoded floor `8 + WorkerCount*2`, with a
  magic fallback of 16 when WorkerCount=0 and a critical threshold at
  the magic value 8. Nothing in Citus or PG says "8".
* `max_replication_slots`: floor `10 + bg_executors*WorkerCount`. The
  "10" was unsourced; the multiplication-by-WorkerCount was wrong —
  each node has its own `max_replication_slots` cap, so multiplying
  double-counts across the cluster.
* `max_wal_senders`: `max(10, WorkerCount*3)`. Same two problems.

**The fix.** Replace every constant with a value derived from either
a cluster fact (a measurable GUC, a `pg_replication_slots` count) or
a two-slot headroom that is explicitly called out in the evidence.

### max_worker_processes

Derived from what actually registers a background worker at startup:

```
min = autovacuum_max_workers
    + max_parallel_workers
    + max_logical_replication_workers
    + citus.max_background_task_executors_per_node
    + 1                                     // citus maintenance daemon
    + 2                                     // headroom
```

Source lines:

* PG `src/backend/postmaster/bgworker.c` — one worker slot per registered
  bgworker; `max_worker_processes` is the global cap.
* Citus `src/backend/distributed/utils/maintenanced.c` — 1 maintenance
  daemon per citus-enabled DB.
* Citus `src/backend/distributed/utils/background_jobs.c:129` —
  `MaxBackgroundTaskExecutorsPerNode` default 1, range 1..128
  (`shared_library_init.c:2028`).

Severity: `warning` by default; upgrades to `critical` only if the
setting doesn't leave room even for `(citus_maint + bg_executors + 1)`.

### max_replication_slots

Slot allocation during shard moves is documented in
`src/backend/distributed/replication/multi_logical_replication.c:
CreateReplicationSlots`. Each shard move creates one LOGICAL slot per
`LogicalRepTarget` on the source. Concurrent moves originating from a
single node are capped by
`citus.max_background_task_executors_per_node`. Therefore:

```
min = citus.max_background_task_executors_per_node
    + existing_active_slots                 // count(*) from pg_replication_slots
    + 2                                     // headroom
```

The advisor now measures `existing_active_slots` on the coordinator
and adds it to the floor, so we advise ON TOP of what is already
allocated instead of assuming a clean slate.

### max_wal_senders

Each active slot held by a subscriber consumes one `wal_sender`
(PG `src/backend/replication/walsender.c`). Uses the same formula as
`max_replication_slots`.

**Cluster facts that drive this.**

* All six GUCs are read from `pg_settings` on the coordinator.
* `pg_replication_slots` row count is measured in the advisor.
* No worker-count multiplier — each node has its own caps.

**Tested.** `internal/citus/guc/rules_magicnumber_test.go` covers:

* Replication-slot floor scales with `bg_executors` AND with
  `existing_active_slots` (regression against the double-count bug).
* No false positive when the budget is already adequate.
* `max_wal_senders` follows the same formula as slots.
* `max_worker_processes` sums the individual GUCs and scales when the
  user raises `citus.max_background_task_executors_per_node`.

Live-verified on the test cluster: the rule now emits evidence
`{autovacuum_max_workers=3, max_parallel_workers=8,
max_logical_replication_workers=4, citus.max_background_task_executors_per_node=1,
citus_maintenance_daemons=1, min_required=19}`, every term traceable
to a GUC or a cited Citus source line.
