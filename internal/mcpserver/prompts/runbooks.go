// citus-mcp: B23 runbook prompts -- static guided-workflow prompts that
// tell the LLM which citus-mcp tools to call and in what order to triage
// specific incident classes (OOM, partition-doubling, cannot-connect,
// slow query, rebalance stuck, capacity plan).

package prompts

import (
	"context"

	"citus-mcp/internal/mcpserver/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type runbook struct {
	name, title, desc, body string
}

var runbooks = []runbook{
	{
		name:  "/citus.oom_triage",
		title: "Citus OOM triage",
		desc:  "Diagnose a recent or imminent OOM on a Citus node.",
		body: `## OOM Triage Runbook

When a Citus node OOMs, memory is almost always the sum of **per-backend caches** × **backends**.
Work through these steps in order. Stop as soon as a tool fires a critical alarm — that is likely the cause.

1. **Snapshot the current memory picture.** Call ` + "`citus_memory_risk_report`" + ` with node_ram_gib override if ` + "`/proc/meminfo`" + ` isn't readable. Look at the RAM headroom % per node.
2. **Per-backend Citus metadata.** ` + "`citus_metadata_cache_footprint`" + `. Projects cache size × max_connections.
3. **Per-backend PG relcache/catcache/plancache.** ` + "`citus_pg_cache_footprint`" + `. Often 2–5× the Citus piece when many partitions are in play.
4. **Real backend snapshot.** ` + "`citus_worker_memcontexts`" + ` — fans out ` + "`pg_log_backend_memory_contexts(pid)`" + ` on sampled backends. Cross-check the estimated total against a live backend's biggest contexts (CacheMemoryContext, MessageContext, TopTransactionContext).
5. **What changed?** If you have snapshots enabled: ` + "`citus_what_changed`" + ` (kind=shards or gucs). Partition count doubled? Shard count grew? max_connections raised? Cache grew with it.
6. **Connection pressure.** ` + "`citus_connection_capacity`" + `. If the memory-derived cap is below max_connections, the OOM is a connections problem dressed up as a memory problem.
7. **Fixes (no one-shot remediation here — propose, don't apply):**
   - Lower ` + "`max_connections`" + `; put PgBouncer session-mode in front (see ` + "`citus_pooler_advisor`" + `).
   - Trim partitions you don't read often.
   - Raise node RAM to the value ` + "`citus_hardware_sizer`" + ` suggests for your current shape.

Emit your plan with concrete numbers from the tools above. Do **not** propose restarts or GUC changes without citing the evidence line.`,
	},
	{
		name:  "/citus.partition_doubled_triage",
		title: "Citus partition-doubled triage",
		desc:  "A user doubled (or is about to double) partition count on a distributed table and something broke.",
		body: `## Partition-Doubled Triage Runbook

Doubling partitions on a **native PG partitioned distributed table** roughly doubles:
- Citus ` + "`MetadataCacheMemoryContext`" + ` (one ` + "`CitusTableCacheEntry`" + ` per partition).
- PG relcache (one entry per touched shard relation × partitions).
- PG plancache for prepared statements that reference the partitioned table.

Run, in order:

1. ` + "`citus_partition_growth_simulator`" + ` with the before/after partition counts. This is the most important tool — it will tell you in advance whether the new shape fits.
2. ` + "`citus_memory_risk_report`" + ` after the change to see current headroom.
3. ` + "`citus_planner_overhead_probe`" + ` on 2–3 representative queries. Planner time grows super-linearly with partitions.
4. ` + "`citus_what_changed`" + ` (kind=shards) if snapshots are enabled — confirms exactly which tables grew.
5. If OOM actually happened, follow ` + "`/citus.oom_triage`" + ` starting from step 3.`,
	},
	{
		name:  "/citus.cannot_connect_triage",
		title: "Citus cannot-connect triage",
		desc:  "Clients are being rejected with 'too many connections' or new connections hang.",
		body: `## Cannot-Connect Triage Runbook

1. ` + "`citus_connection_capacity`" + ` — tells you the effective cap in each deployment shape (coordinator-only, MX, PgBouncer session/txn). Compare to current connection count.
2. ` + "`citus_connection_fanout_simulator`" + ` — for a given workload, is the coordinator's outbound-to-workers saturated? ` + "`max_shared_pool_size`" + ` × ` + "`max_adaptive_executor_pool_size`" + ` is the cap.
3. ` + "`citus_pgbouncer_inspector`" + ` (if pooled) — pool exhaustion shows here as ` + "`cl_waiting`" + ` > 0.
4. ` + "`citus_pooler_advisor`" + ` — if you don't have a pooler, this gives you a tuned PgBouncer config.
5. ` + "`citus_config_deep_inspect`" + ` — catch drift on ` + "`max_connections`" + `, ` + "`citus.max_client_connections`" + `, ` + "`citus.max_shared_pool_size`" + `.
6. ` + "`citus_proactive_health`" + ` — idle-in-transaction sessions and stuck 2PC eat connections.`,
	},
	{
		name:  "/citus.slow_query_triage",
		title: "Citus slow-query triage",
		desc:  "One or more queries regressed in latency.",
		body: `## Slow-Query Triage Runbook

1. ` + "`citus_query_pathology`" + ` — classify top queries by category (router/adaptive/fanout/repartition/recursive).
2. ` + "`citus_routing_drift_detector`" + ` — catches queries whose router shape no longer prunes to one shard (distribution column wrapped in cast/function, type mismatch, etc.).
3. ` + "`citus_planner_overhead_probe`" + ` on the slow queries — if the planner itself is slow, the fix is different from runtime-slow.
4. ` + "`citus_tenant_risk`" + ` — on 13.1+, hot tenants show up in ` + "`citus_stat_tenants`" + `. Isolate with ` + "`isolate_tenant_to_new_shard`" + `.
5. ` + "`citus_trend`" + ` kind=queries (if snapshots enabled) — was the regression sudden or gradual?`,
	},
	{
		name:  "/citus.rebalance_stuck_triage",
		title: "Citus rebalance-stuck triage",
		desc:  "A rebalance job has been running too long or aborted.",
		body: `## Rebalance-Stuck Triage Runbook

1. ` + "`citus_rebalance_cost_estimator`" + ` on the table in question — often the *reason* it's stuck is the estimate exceeds ` + "`citus.logical_replication_timeout`" + `, so each shard move aborts and retries forever.
2. ` + "`citus_proactive_health`" + ` — stuck 2PC or background tasks will block the rebalance.
3. ` + "`citus_config_deep_inspect`" + ` — confirm coordinator and workers agree on ` + "`citus.logical_replication_timeout`" + `, ` + "`max_replication_slots`" + `, ` + "`max_wal_senders`" + `, ` + "`wal_level`" + `.
4. ` + "`citus_dr_health`" + ` (when available) — WAL/archive backpressure can stall the move.
5. If shard is too big for the timeout, split it first with ` + "`citus_split_shard_by_split_points`" + ` before retrying the rebalance.`,
	},
	{
		name:  "/citus.add_node_triage",
		title: "Citus add-node triage",
		desc:  "Pre-flight a new worker before running citus_add_node / citus_activate_node.",
		body: `## Add-Node Triage Runbook

Adding a worker fails in predictable ways: environment drift, metadata-sync memory blowup, MX mesh pressure. Run these in order. **All three are advisory — none actually adds the node.**

1. ` + "`citus_add_node_preflight`" + ` — emits a checklist of what the new node must satisfy (PG major version, exact citus version, installable extensions, collation/encoding, superuser access, shared_preload_libraries). Pass ` + "`node_role=\"mx-worker\"`" + ` to include MX-specific checks (reverse auth, hasmetadata).
2. ` + "`citus_metadata_sync_risk`" + ` — estimates DDL commands, locks in tx, worker memory, and duration citus_activate_node will require. Correlates with max_locks_per_transaction, max_connections, statement_timeout, idle_in_transaction_session_timeout, citus.metadata_sync_mode. If it returns ` + "`verdict=\"blocked\"`" + ` or ` + "`nontransactional_recommended`" + `, apply the recommended ` + "`SET citus.metadata_sync_mode='nontransactional';`" + ` BEFORE ` + "`citus_activate_node`" + ` — and remember non-transactional mode REFUSES to run inside a BEGIN block.
3. ` + "`citus_mx_readiness`" + ` (only if MX) — models mesh connection pressure after add: (N_after-1) × citus.max_adaptive_executor_pool_size + clients_per_node vs max_connections on every node. Warns when citus.max_client_connections=0 (flood risk) or max_shared_pool_size=0.
4. Only after all three are clean:
   ` + "```sql\n" +
			`-- On coordinator, outside any BEGIN block:
SET citus.metadata_sync_mode = 'nontransactional';  -- only if step 2 recommended
SET statement_timeout = 0;
SET idle_in_transaction_session_timeout = 0;

SELECT citus_add_node('<new-host>', <port>);  -- add + auto-activate
-- or, add-then-activate separately:
-- SELECT citus_add_inactive_node('<new-host>', <port>);
-- SELECT citus_activate_node('<new-host>', <port>);
` + "```\n" +
			`5. Verify post-add:
   ` + "```sql\nSELECT nodename, nodeport, hasmetadata, metadatasynced, isactive\nFROM pg_dist_node ORDER BY nodeid;\n```\n" +
			`   All rows should show ` + "`hasmetadata=t, metadatasynced=t, isactive=t`" + `. If ` + "`metadatasynced=f`" + ` on the new node, re-run ` + "`citus_metadata_sync_risk`" + ` — you likely hit the transactional-mode limit mid-sync. Retry with ` + "`citus.metadata_sync_mode='nontransactional'`" + `.`,
	},
	{
		name:  "/citus.capacity_plan",
		title: "Citus capacity plan",
		desc:  "Plan hardware and shard counts for current + projected load.",
		body: `## Capacity Plan Runbook

1. ` + "`citus_hardware_sizer`" + ` with your target data size, client count, workload_mode, deployment_mode. Use ` + "`fit_to_ram`" + ` first, ` + "`fit_to_nodes`" + ` if node count is fixed.
2. ` + "`citus_shard_count_advisor`" + ` — per colocation group, reconcile against the hardware plan.
3. ` + "`citus_shardcount_tradeoff_chart`" + ` for the biggest table — visualize the cost at several candidate shard counts.
4. ` + "`citus_partition_growth_simulator`" + ` if you plan to add partitions as part of the plan.
5. ` + "`citus_growth_projection`" + ` (needs snapshots) — project when current trajectory hits the planned hardware.
6. Cross-check: ` + "`citus_connection_capacity`" + ` at the proposed hardware shape — make sure clients × fan-out fits.`,
	},
}

// RegisterRunbooks registers all runbook prompts.
func RegisterRunbooks(server *mcp.Server, deps tools.Dependencies) {
	for _, rb := range runbooks {
		rb := rb
		server.AddPrompt(&mcp.Prompt{
			Name: rb.name, Title: rb.title, Description: rb.desc,
		}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: rb.desc,
				Messages: []*mcp.PromptMessage{{
					Role:    "user",
					Content: &mcp.TextContent{Text: rb.body},
				}},
			}, nil
		})
	}
}
