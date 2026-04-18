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

