// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Tool registration and core tool implementations (ping, server_info, list_*).

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/cache"
	"citus-mcp/internal/citus"
	advisorpkg "citus-mcp/internal/citus/advisor"
	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"citus-mcp/internal/safety"
	"citus-mcp/internal/snapshot"
	"citus-mcp/internal/version"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

type Dependencies struct {
	Pool          *pgxpool.Pool
	Logger        *zap.Logger
	Guardrails    *safety.Guardrails
	Config        config.Config
	WorkerManager *db.WorkerManager
	Capabilities  *db.Capabilities
	Cache         *cache.Cache
	Alarms        *diagnostics.Sink
	Snapshot      *snapshot.Store // optional; nil when snapshot store is disabled
	Fanout        *db.Fanout
}

func RegisterAll(server *mcp.Server, deps Dependencies) {
	mcp.AddTool(server, &mcp.Tool{Name: "ping", Description: "ping the server"}, func(ctx context.Context, req *mcp.CallToolRequest, input PingInput) (*mcp.CallToolResult, PingOutput, error) {
		return Ping(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "server_info", Description: "returns server metadata"}, func(ctx context.Context, req *mcp.CallToolRequest, input ServerInfoInput) (*mcp.CallToolResult, ServerInfoOutput, error) {
		return ServerInfo(ctx, deps)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "list_nodes", Description: "lists coordinator and worker nodes"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListNodesInput) (*mcp.CallToolResult, ListNodesOutput, error) {
		return ListNodes(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "list_distributed_tables", Description: "lists distributed tables"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListDistributedTablesInput) (*mcp.CallToolResult, ListDistributedTablesOutput, error) {
		return ListDistributedTables(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "list_shards", Description: "lists shards with table names and node placements"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListShardsInput) (*mcp.CallToolResult, ListShardsOutput, error) {
		return ListShards(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "rebalance_table_plan", Description: "plan rebalance_table_shards"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceTableInput) (*mcp.CallToolResult, RebalanceTablePlanOutput, error) {
		return RebalanceTablePlan(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "rebalance_table_execute", Description: "execute rebalance_table_shards (approval required)"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceTableExecuteInput) (*mcp.CallToolResult, RebalanceTableExecuteOutput, error) {
		return RebalanceTableExecute(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_cluster_summary", Description: "cluster summary"}, func(ctx context.Context, req *mcp.CallToolRequest, input ClusterSummaryInput) (*mcp.CallToolResult, ClusterSummaryOutput, error) {
		return clusterSummaryTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_list_distributed_tables", Description: "list distributed tables (paginated)"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListDistributedTablesV2Input) (*mcp.CallToolResult, ListDistributedTablesV2Output, error) {
		if input.TableType == "" {
			input.TableType = "distributed"
		}
		return listDistributedTablesV2(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_list_reference_tables", Description: "list reference tables (paginated)"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListDistributedTablesV2Input) (*mcp.CallToolResult, ListDistributedTablesV2Output, error) {
		input.TableType = "reference"
		return listDistributedTablesV2(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_explain_query", Description: "EXPLAIN a query (optionally ANALYZE)"}, func(ctx context.Context, req *mcp.CallToolRequest, input ExplainQueryInput) (*mcp.CallToolResult, ExplainQueryOutput, error) {
		return explainQueryTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_shard_skew_report", Description: "report shard skew per node"}, func(ctx context.Context, req *mcp.CallToolRequest, input ShardSkewInput) (*mcp.CallToolResult, ShardSkewOutput, error) {
		return shardSkewReportTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_rebalance_plan", Description: "rebalance plan"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalancePlanInput) (*mcp.CallToolResult, RebalancePlanOutput, error) {
		return rebalancePlanTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_validate_rebalance_prereqs", Description: "validate prerequisites for rebalance"}, func(ctx context.Context, req *mcp.CallToolRequest, input ValidateRebalancePrereqsInput) (*mcp.CallToolResult, ValidateRebalancePrereqsOutput, error) {
		return validateRebalancePrereqsTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_rebalance_execute", Description: "execute rebalance"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceExecuteInput) (*mcp.CallToolResult, RebalanceExecuteOutput, error) {
		return rebalanceExecuteTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_rebalance_status", Description: "rebalance status"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceStatusInput) (*mcp.CallToolResult, RebalanceStatusOutput, error) {
		return rebalanceStatusTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_snapshot_source_advisor", Description: "advise source worker for snapshot-based node addition"}, func(ctx context.Context, req *mcp.CallToolRequest, input SnapshotSourceAdvisorInput) (*mcp.CallToolResult, SnapshotSourceAdvisorOutput, error) {
		return SnapshotSourceAdvisor(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_move_shard_plan", Description: "plan shard move"}, func(ctx context.Context, req *mcp.CallToolRequest, input MoveShardPlanInput) (*mcp.CallToolResult, MoveShardPlanOutput, error) {
		return moveShardPlanTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_move_shard_execute", Description: "execute shard move (approval required)"}, func(ctx context.Context, req *mcp.CallToolRequest, input MoveShardExecuteInput) (*mcp.CallToolResult, MoveShardExecuteOutput, error) {
		return moveShardExecuteTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_request_approval_token", Description: "request approval token (admin only)"}, func(ctx context.Context, req *mcp.CallToolRequest, input RequestApprovalTokenInput) (*mcp.CallToolResult, RequestApprovalTokenOutput, error) {
		return requestApprovalTokenTool(ctx, deps, input)
	})

	// Advisor
	mcp.AddTool(server, &mcp.Tool{Name: "citus_advisor", Description: "Citus SRE + Query Performance Advisor (read-only)"}, func(ctx context.Context, req *mcp.CallToolRequest, input CitusAdvisorInput) (*mcp.CallToolResult, advisorpkg.Output, error) {
		return citusAdvisorTool(ctx, deps, input)
	})

	// Shard heatmap tool (uses citus_shards view when available)
	mcp.AddTool(server, &mcp.Tool{Name: "citus_shard_heatmap", Description: "Shard heatmap & hot shard advisor (read-only)"}, func(ctx context.Context, req *mcp.CallToolRequest, input ShardHeatmapInput) (*mcp.CallToolResult, ShardHeatmapOutput, error) {
		return shardHeatmapTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_table_inspector", Description: "Inspect distributed/reference table metadata"}, func(ctx context.Context, req *mcp.CallToolRequest, input TableInspectorInput) (*mcp.CallToolResult, TableInspectorOutput, error) {
		return tableInspectorTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_lock_inspector", Description: "Inspect cluster lock waits and locks (read-only)"}, func(ctx context.Context, req *mcp.CallToolRequest, input LockInspectorInput) (*mcp.CallToolResult, LockInspectorOutput, error) {
		return citusLockInspectorTool(ctx, deps, input)
	})

	// New tools: Activity, Job Inspector, Colocation Inspector
	mcp.AddTool(server, &mcp.Tool{Name: "citus_activity", Description: "Cluster-wide activity monitor (read-only)"}, func(ctx context.Context, req *mcp.CallToolRequest, input ActivityInput) (*mcp.CallToolResult, ActivityOutput, error) {
		return activityTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_job_inspector", Description: "Inspect background jobs (rebalance, copy) progress"}, func(ctx context.Context, req *mcp.CallToolRequest, input JobInspectorInput) (*mcp.CallToolResult, JobInspectorOutput, error) {
		return jobInspectorTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "citus_colocation_inspector", Description: "Inspect colocation groups and colocated tables"}, func(ctx context.Context, req *mcp.CallToolRequest, input ColocationInspectorInput) (*mcp.CallToolResult, ColocationInspectorOutput, error) {
		return colocationInspectorTool(ctx, deps, input)
	})

	// Advanced Advisors: Metadata Health, Node Preparation
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_metadata_health",
		Description: "Detect metadata corruption and inconsistencies. Checks orphaned shards, invalid placements, colocation mismatches, and cross-node drift.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input MetadataHealthInput) (*mcp.CallToolResult, MetadataHealthOutput, error) {
		return metadataHealthTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_node_prepare_advisor",
		Description: "Pre-flight checks before adding a worker node. Validates extensions, versions, schemas, types, functions, and roles.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input NodePrepareInput) (*mcp.CallToolResult, NodePrepareOutput, error) {
		return nodePrepareAdvisorTool(ctx, deps, input)
	})

	// Configuration Advisor: Comprehensive Citus and PostgreSQL GUC analysis
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_config_advisor",
		Description: "Comprehensive analysis of Citus and PostgreSQL configuration. Detects compatibility issues, performance problems, and provides best-practice recommendations with fix SQL.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ConfigAdvisorInput) (*mcp.CallToolResult, ConfigAdvisorOutput, error) {
		return configAdvisorTool(ctx, deps, input)
	})

	// Alarms framework (B11): unified early-warning surface. Diagnostic tools
	// emit findings into deps.Alarms; these tools expose them to the client.
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_alarms_list",
		Description: "List active (and optionally acknowledged) diagnostic alarms emitted by citus-mcp tools, sorted by severity then recency. Supports filtering by severity, kind, source, node, object, and age.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input AlarmsListInput) (*mcp.CallToolResult, AlarmsListOutput, error) {
		return AlarmsList(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_alarms_ack",
		Description: "Acknowledge one alarm (by id) or all alarms matching a filter. Acknowledged alarms are hidden until the underlying condition re-emits.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input AlarmsAckInput) (*mcp.CallToolResult, AlarmsAckOutput, error) {
		return AlarmsAck(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_alarms_clear",
		Description: "Drop ALL alarms from the in-memory sink (no cluster effect). Requires confirm=true. Useful to start a fresh observation window after remediation.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input AlarmsClearInput) (*mcp.CallToolResult, AlarmsClearOutput, error) {
		return AlarmsClear(ctx, deps, input)
	})

	// Diagnostics: memory footprint (B1, B1b).
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_metadata_cache_footprint",
		Description: "Predict per-backend Citus MetadataCacheMemoryContext bytes from catalog shape; optionally measure live via pg_backend_memory_contexts; project cluster-wide memory against max_connections. Explains the top contributing tables. Emits alarms when the projected footprint crosses thresholds.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input MetadataCacheFootprintInput) (*mcp.CallToolResult, MetadataCacheFootprintOutput, error) {
		return MetadataCacheFootprintTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_pg_cache_footprint",
		Description: "Predict per-backend PG CacheMemoryContext (relcache + catcache + partcache + plancache) bytes from distributed-table shape; optionally measure live. Use include_shard_rels=true for a worker-in-MX view. Emits alarms on high footprints.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input PgCacheFootprintInput) (*mcp.CallToolResult, PgCacheFootprintOutput, error) {
		return PgCacheFootprintTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_worker_memcontexts",
		Description: "Fan out to every node (coordinator + workers), prime Citus metadata caches, and probe pg_backend_memory_contexts locally on each node. Returns per-node MetadataCacheMemoryContext and CacheMemoryContext sizes, the cluster median, and emits alarms for outliers (nodes > 2x median). Observes THIS session's backend on each node; does not peek into other backends.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input WorkerMemcontextsInput) (*mcp.CallToolResult, WorkerMemcontextsOutput, error) {
		return WorkerMemcontextsTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_partition_growth_simulator",
		Description: "What-if simulator: given proposed partition count changes (per table or a global multiplier), project resulting MetadataCacheMemoryContext + PG CacheMemoryContext per-backend and cluster-wide (× max_connections). Returns baseline, projected, delta, and alarms if delta exceeds 25% (warning) or 100% (critical) of current per-backend footprint. Use BEFORE adding partitions to predict OOM risk.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input PartitionGrowthInput) (*mcp.CallToolResult, PartitionGrowthOutput, error) {
		return PartitionGrowthTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_memory_risk_report",
		Description: "Cluster-wide memory risk report. For each node, sums shared_buffers + wal_buffers + per-backend caches x backends + work_mem budget + autovacuum budget, compares to node RAM (from override or pg_read_file), and emits alarms at 80% (warning) and 95% (critical). Set worst_case=true to project against max_connections instead of current active backends.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input MemoryRiskInput) (*mcp.CallToolResult, MemoryRiskOutput, error) {
		return MemoryRiskReportTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_connection_capacity",
		Description: "Compute maximum safe client connection count for coordinator-only, MX, and PgBouncer (session + transaction) modes. Inputs: node_ram_bytes (or per-node map), safety_fraction (default 0.60). Considers max_connections, citus.max_client_connections, citus.max_shared_pool_size, citus.max_adaptive_executor_pool_size worst-case fan-out, and per-backend memory budget. Emits alarm when max_connections exceeds memory-safe cap.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ConnectionCapacityInput) (*mcp.CallToolResult, ConnectionCapacityOutput, error) {
		return ConnectionCapacityTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_pooler_advisor",
		Description: "PgBouncer-aware advisor. Detects pooler presence in pg_stat_activity; if pgbouncer_admin_dsn is supplied, pulls SHOW CONFIG/POOLS/DATABASES and flags Citus-incompatible settings (pool_mode=transaction breaking cached worker conns, short server_lifetime, default_pool_size > citus.max_client_connections, prepared_statements misconfig). Always returns a tuned pgbouncer.ini recommendation grounded in current cluster GUCs.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input PoolerAdvisorInput) (*mcp.CallToolResult, PoolerAdvisorOutput, error) {
		return PoolerAdvisorTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_config_deep_inspect",
		Description: "Cross-node GUC-drift detector. For a curated list of safety-critical settings (wal_level, max_prepared_transactions, max_connections, shared_buffers, citus.max_shared_pool_size, etc.), reads each node's value and flags any divergence. Use extra_gucs to add site-specific names and ignore to suppress intentional drift. Emits config.drift alarms (critical for wal_level/2PC/connections, warning for capacity/memory, info for cosmetic).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ConfigDeepInspectInput) (*mcp.CallToolResult, ConfigDeepInspectOutput, error) {
		return ConfigDeepInspectTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_proactive_health",
		Description: "Aggregated early-warning dashboard. Runs 8 health checks across coordinator + workers: node availability (pg_dist_node), long-running transactions, idle-in-transaction, stuck 2PC (pg_prepared_xacts), unhealthy shard placements, connection saturation, bloat candidates, stale autovacuum. Each threshold is tunable via input. Emits one alarm per violating category. Overall health rolled up to ok | warning | critical.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ProactiveHealthInput) (*mcp.CallToolResult, ProactiveHealthOutput, error) {
		return ProactiveHealthTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_shard_advisor",
		Description: "Recommends ideal shard count per colocation group based on current+projected table size, worker count, optional cores_per_worker, and target shard size (default 10 GiB). Returns per-table and per-colocation-group recommendations with rationale and action (no_change / increase / decrease). Cross-checks recommendation against Citus metadata-cache cost to prevent sharding yourself into an OOM. Emits shard_advisor.significant_change and shard_advisor.metadata_cost_high alarms.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ShardAdvisorInput) (*mcp.CallToolResult, ShardAdvisorOutput, error) {
		return ShardAdvisorTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_hardware_sizer",
		Description: "Reverse-sizing: given target_data_size_bytes, target_concurrent_clients, workload_mode (oltp|olap|mixed), and deployment_mode (coord-only|mx|pgbouncer-session|pgbouncer-txn), recommends node_count, cores_per_node, ram_per_node_gib, and matching GUCs. Two modes: fit_to_ram (pick minimum node count that fits max_ram_per_node_gib, default) or fit_to_nodes (user fixes node_count, compute per-node RAM). Returns a transparent per-node RAM bucket breakdown (shared_buffers, client_backends, autovacuum, WAL, OS reserve) so the recommendation can be argued line-by-line.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input HardwareSizerInput) (*mcp.CallToolResult, HardwareSizerOutput, error) {
		return HardwareSizerTool(ctx, deps, input)
	})

	// ---- M2: sizing & workload intelligence ----
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_connection_fanout_simulator",
		Description: "Pure what-if over Citus fan-out: given one or more workload descriptors (qps_coordinator, avg_shards_touched, avg_query_ms), predicts concurrent coordinator backends (Little's law) and per-worker connections. Surfaces caps imposed by max_adaptive_executor_pool_size and max_shared_pool_size. Emits fanout_sim.shared_pool_exceeded when capacity is exceeded.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input FanoutSimInput) (*mcp.CallToolResult, FanoutSimOutput, error) {
		return ConnectionFanoutSimulatorTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_shardcount_tradeoff_chart",
		Description: "For a distributed table (by table_name) or a synthetic shape (total_data_bytes), enumerates candidate shard counts and returns a trade-off matrix: avg_shard_bytes, estimated_planner_ms, metadata_cache_mib, parallelism_factor, and a fit label (too_few / sweet_spot / too_many_metadata_heavy ...). Helps operators reason about shard-count choices before committing.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input TradeoffChartInput) (*mcp.CallToolResult, TradeoffChartOutput, error) {
		return ShardCountTradeoffChartTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_tenant_risk",
		Description: "Surfaces hot tenants via citus_stat_tenants (Citus 13.1+). Computes per-tenant share of query volume; flags tenants exceeding hot_share_threshold_pct (default 10%) and emits tenant.very_hot alarms at 2× the threshold. Suggests isolate_tenant_to_new_shard as a remediation.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input TenantRiskInput) (*mcp.CallToolResult, TenantRiskOutput, error) {
		return TenantRiskTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_query_pathology",
		Description: "Classifies top queries from pg_stat_statements into Citus routing patterns (router, adaptive, fanout_multi_shard, repartition, cte_recursive, ddl, modify, insert) via cheap keyword heuristics. Flags heavy patterns (mean_exec_ms ≥ high_mean_time_ms) with query.heavy_<kind> alarms so operators can quickly see which pattern is dominating cost.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input QueryPathologyInput) (*mcp.CallToolResult, QueryPathologyOutput, error) {
		return QueryPathologyTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_pgbouncer_inspector",
		Description: "Connects to a pgbouncer admin_dsn (simple protocol) and parses SHOW POOLS / SHOW STATS to surface per-pool saturation and wait-time issues. Correlates with citus_remote_connection_stats on the coordinator when available. Emits pgbouncer.pool_saturated and pgbouncer.high_wait alarms.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input PgBouncerInspectorInput) (*mcp.CallToolResult, PgBouncerInspectorOutput, error) {
		return PgBouncerInspectorTool(ctx, deps, input)
	})

	// ---- M2/M3: snapshot store + trend tools (opt-in via --snapshot_db) ----
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_snapshot_record",
		Description: "Records a diagnostics snapshot into the opt-in local SQLite history (kind: memory | connections | shards | gucs | alarms | tenants | queries | health). All data is automatically redacted for DSNs / passwords / query text. Requires the server to be started with --snapshot_db.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input SnapshotRecordInput) (*mcp.CallToolResult, SnapshotRecordOutput, error) {
		return SnapshotRecordTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_snapshot_list",
		Description: "Lists snapshots by cluster + kind within a since_hours window, newest first. Requires the snapshot store to be enabled.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input SnapshotListInput) (*mcp.CallToolResult, SnapshotListOutput, error) {
		return SnapshotListTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_trend",
		Description: "Returns a time series of a numeric field extracted by JSON path from snapshots of a given kind, plus first/last/min/max and slope_per_hour. Useful to chart resource consumption over time (requires snapshot store).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input TrendInput) (*mcp.CallToolResult, TrendOutput, error) {
		return TrendTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_regression_detect",
		Description: "Splits the snapshot series into baseline (baseline_hours ago → comparison_hours ago) and comparison (last comparison_hours); flags regression_<kind> alarms when the comparison mean differs from baseline by ≥ threshold_pct (default 25%).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RegressionInput) (*mcp.CallToolResult, RegressionOutput, error) {
		return RegressionDetectTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_growth_projection",
		Description: "Linearly projects when a numeric signal (extracted by JSON path from recent snapshots) will hit a user-specified ceiling. Reports hours_to_ceiling + estimated_when. Emits growth.wall_approaching when the wall is < 90 days away. Requires snapshot store.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GrowthProjectionInput) (*mcp.CallToolResult, GrowthProjectionOutput, error) {
		return GrowthProjectionTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_what_changed",
		Description: "Diffs the newest two snapshots of a kind (within since_hours, default 24) and returns added_keys / removed_keys / changed_keys. Great for 'what changed between yesterday and today' investigations. Requires snapshot store.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input WhatChangedInput) (*mcp.CallToolResult, WhatChangedOutput, error) {
		return WhatChangedTool(ctx, deps, input)
	})
	// M3 tools.
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_rebalance_cost_estimator",
		Description: "Estimates wall-clock, bytes moved, and risk of a Citus rebalance using get_rebalance_table_shards_plan(). Flags timeout_risk when the largest shard ETA exceeds citus.logical_replication_timeout.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceCostInput) (*mcp.CallToolResult, RebalanceCostOutput, error) {
		return RebalanceCostEstimatorTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_extension_drift_scanner",
		Description: "Scans pg_extension across coordinator + all workers and flags version/availability drift for citus and adjacent extensions (citus_columnar, pg_partman, pg_cron, postgres_fdw, pg_stat_statements, …).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ExtensionDriftInput) (*mcp.CallToolResult, ExtensionDriftOutput, error) {
		return ExtensionDriftScannerTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_planner_overhead_probe",
		Description: "Runs EXPLAIN (no ANALYZE) on user-supplied SELECT/WITH queries and measures planner wall-clock. Emits planner.high_overhead when median ≥ high_planner_ms (default 200). Read-only; refuses DML/DDL.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input PlannerProbeInput) (*mcp.CallToolResult, PlannerProbeOutput, error) {
		return PlannerOverheadProbeTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_routing_drift_detector",
		Description: "Scans pg_stat_statements for router-shaped queries returning many rows/call — a heuristic for queries whose distribution column no longer prunes to one shard.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RoutingDriftInput) (*mcp.CallToolResult, RoutingDriftOutput, error) {
		return RoutingDriftDetectorTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_session_guardrails",
		Description: "Flags cluster-level risky GUC defaults (work_mem too high, statement_timeout=0, citus.multi_shard_modify_mode=parallel, idle_in_transaction_session_timeout=0) and lists active client backends.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input SessionGuardrailsInput) (*mcp.CallToolResult, SessionGuardrailsOutput, error) {
		return SessionGuardrailsTool(ctx, deps, input)
	})
	// M4 tools.
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_columnar_advisor",
		Description: "Scans partitions of distributed tables ≥ min_partition_bytes (default 1 GiB) and ranks candidates for conversion to the citus_columnar access method. Good candidates are large, append-only, and not the current partition.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ColumnarAdvisorInput) (*mcp.CallToolResult, ColumnarAdvisorOutput, error) {
		return ColumnarAdvisorTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_synthetic_probe",
		Description: "Bounded read-only microbenchmark: round-trip, fanout-count, router-const. Reports p50/p95/min/max across iterations (default 10). Emits probe.high_latency when p95 exceeds high_p95_ms.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input SyntheticProbeInput) (*mcp.CallToolResult, SyntheticProbeOutput, error) {
		return SyntheticProbeTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_isolate_tenant",
		Description: "Guarded wrapper around isolate_tenant_to_new_shard(table, tenant_value, cascade). Requires allow_execute=true and a valid approval token. Use citus_tenant_risk to identify candidates first.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input IsolateTenantInput) (*mcp.CallToolResult, IsolateTenantOutput, error) {
		return IsolateTenantTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_cleanup_orphaned",
		Description: "Guarded wrapper around citus_cleanup_orphaned_resources() (falls back to citus_cleanup_orphaned_shards() on older versions). Requires allow_execute=true and a valid approval token.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input CleanupOrphanedInput) (*mcp.CallToolResult, CleanupOrphanedOutput, error) {
		return CleanupOrphanedTool(ctx, deps, input)
	})
	// M5 tools: add-node advisor suite.
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_add_node_preflight",
		Description: "CHECKLIST-ONLY advisory. Does NOT connect to the target host. Reads coordinator state (PG version, citus version, distributed extensions, DB locale/encoding) and emits a preparation checklist the operator must satisfy on the new node BEFORE citus_add_node / citus_activate_node. node_role='mx-worker' enables MX-specific checks (reverse auth, hasmetadata).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input AddNodePreflightInput) (*mcp.CallToolResult, AddNodePreflightOutput, error) {
		return AddNodePreflightTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_metadata_sync_risk",
		Description: "Estimate-only. Counts DDL commands, locks, memory, and duration citus_activate_node will require, correlates with max_locks_per_transaction, max_connections, statement_timeout, idle_in_transaction_session_timeout, citus.metadata_sync_mode. Emits metadata_sync.locks_exhausted / worker_oom_risk / timeout_risk; recommends SET citus.metadata_sync_mode='nontransactional' when warranted.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input MetadataSyncRiskInput) (*mcp.CallToolResult, MetadataSyncRiskOutput, error) {
		return MetadataSyncRiskTool(ctx, deps, input)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_mx_readiness",
		Description: "Mesh-connection math + metadata-cache footprint for MX topology after adding one more worker. Reports per-node connection headroom (coordinator, existing workers, synthetic new node); warns when citus.max_client_connections=0 (flood risk) or max_shared_pool_size=0.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input MxReadinessInput) (*mcp.CallToolResult, MxReadinessOutput, error) {
		return MxReadinessTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_2pc_recovery_inspector",
		Description: "Read-only 2PC recovery forensics. Fans out pg_prepared_xacts to the coordinator and every active worker, parses Citus prepared-transaction GIDs (citus_<group>_<pid>_<txn>_<conn>), correlates with pg_dist_transaction and get_all_active_transactions(), and classifies each prepared xact as commit_needed / rollback_needed / in_flight / non_citus (mirrors RecoverWorkerTransactions in src/backend/distributed/transaction/transaction_recovery.c). Emits a ready-to-run per-node COMMIT PREPARED / ROLLBACK PREPARED recovery script plus alarms for commit backlog, orphan xacts (critical if > stuck_orphan_seconds), and slow-in-flight 2PCs. Never executes COMMIT/ROLLBACK itself.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input TwoPCRecoveryInput) (*mcp.CallToolResult, TwoPCRecoveryOutput, error) {
		return TwoPCRecoveryInspectorTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_rebalance_forensics",
		Description: "Read-only diagnosis of WHY a Citus rebalance (or any background job) is stuck. Inspects pg_dist_background_job/_task/_depend, correlates running tasks with pg_stat_activity wait events and pg_blocking_pids, counts pg_dist_cleanup backlog, and reads citus.max_background_task_executors. Classifies stalls as blocked_by_ddl | blocked_by_lock | bg_worker_starvation | retry_backoff | error_with_retries_exhausted | cleanup_backlog | finished_with_errors | no_stall, and emits a concrete playbook (citus_rebalance_stop / citus_cleanup_orphaned_resources / pg_cancel_backend) plus alarms. Addresses operational gaps from citusdata/citus issues #6681, #7103, #8236, #1210.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceForensicsInput) (*mcp.CallToolResult, RebalanceForensicsOutput, error) {
		return RebalanceForensicsTool(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_placement_integrity_check",
		Description: "Three-way cross-check between Citus metadata (pg_dist_placement + pg_dist_shard + pg_dist_node), on-disk reality on every worker (pg_class via run_command_on_workers), and pg_dist_cleanup. Detects ghost placements (metadata says shard exists but no relation on disk — reads will error), orphan tables (shard-suffix tables on worker with no placement row — leaked by failed rebalances/moves), inactive_with_data (shardstate != 1 but data still materialised — reclaimable), size drift (pg_relation_size vs placement.shardlength), and pg_dist_cleanup backlog. Emits a per-class reconciliation playbook (citus_copy_shard_placement / citus_cleanup_orphaned_resources / citus_update_shard_statistics). Read-only; never drops or copies anything itself. Addresses citusdata/citus issues #8236, #5284, #5286, #4977, #5133.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input PlacementIntegrityInput) (*mcp.CallToolResult, PlacementIntegrityOutput, error) {
		return PlacementIntegrityCheckTool(ctx, deps, input)
	})

	// ---- M6: covering report ----
	mcp.AddTool(server, &mcp.Tool{
		Name:        "citus_full_report",
		Description: "One-shot comprehensive diagnostic report. Fans out to every read-only diagnostic and advisor tool (cluster summary, proactive + metadata health, config advisor + deep-inspect, extension drift, session guardrails, all four memory tools, connection capacity + fan-out sim + MX readiness + metadata-sync risk + pooler advisor, activity + locks + jobs, tenant + query pathology + routing drift, shard skew + heatmap + colocation + rebalance cost, citus_advisor + shard + columnar advisors, alarms) and rolls every output into a single structured response with overall_health, top_findings, and recommendations. Execute-class tools (rebalance/move/isolate/cleanup execute, approval token, snapshot record) are intentionally excluded. Optional sections (synthetic probe microbench, pgbouncer inspector, hardware sizer) are only run when the caller supplies the corresponding inputs. Use include_verbose_outputs=true to keep every per-tool output verbatim.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input FullReportInput) (*mcp.CallToolResult, FullReportOutput, error) {
		return FullReportTool(ctx, deps, input)
	})
}

// Ping tool

type PingInput struct {
	Message string `json:"message,omitempty" jsonschema:"optional message to echo"`
}

type PingOutput struct {
	Pong string `json:"pong"`
}

func Ping(ctx context.Context, deps Dependencies, input PingInput) (*mcp.CallToolResult, PingOutput, error) {
	msg := input.Message
	if msg == "" {
		msg = "pong"
	}
	return nil, PingOutput{Pong: msg}, nil
}

// ServerInfo tool

type ServerInfoInput struct{}

type ServerInfoOutput struct {
	Version      string          `json:"version"`
	Commit       string          `json:"commit"`
	BuildDate    string          `json:"build_date"`
	ReadOnly     bool            `json:"read_only"`
	AllowExecute bool            `json:"allow_execute"`
	Metadata     *citus.Metadata `json:"metadata,omitempty"`
}

func ServerInfo(ctx context.Context, deps Dependencies) (*mcp.CallToolResult, ServerInfoOutput, error) {
	info := version.Info()
	out := ServerInfoOutput{
		Version:      info.Version,
		Commit:       info.Commit,
		BuildDate:    info.Date,
		ReadOnly:     !deps.Config.AllowExecute,
		AllowExecute: deps.Config.AllowExecute,
	}
	meta, err := citus.GetMetadata(ctx, deps.Pool)
	if err != nil {
		deps.Logger.Warn("server_info metadata failed", zap.Error(err))
		return nil, out, nil
	}
	out.Metadata = meta
	return nil, out, nil
}

// ListNodes tool
type ListNodesInput struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}
type ListNodesOutput struct {
	Nodes []db.Node `json:"nodes"`
	Meta  Meta      `json:"meta"`
}

// Meta contains pagination metadata.
type Meta struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

func ListNodes(ctx context.Context, deps Dependencies, input ListNodesInput) (*mcp.CallToolResult, ListNodesOutput, error) {
	limit, offset := normalizeLimitOffset(deps.Config, input.Limit, input.Offset)
	nodes, err := db.ListNodes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ListNodesOutput{Nodes: []db.Node{}, Meta: Meta{}}, nil
	}
	if nodes == nil {
		nodes = []db.Node{}
	}
	end := offset + limit
	if end > len(nodes) {
		end = len(nodes)
	}
	if offset > len(nodes) {
		offset = len(nodes)
	}
	return nil, ListNodesOutput{Nodes: nodes[offset:end], Meta: Meta{Limit: limit, Offset: offset, Total: len(nodes)}}, nil
}

// ListDistributedTables tool
type ListDistributedTablesInput struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}
type ListDistributedTablesOutput struct {
	Tables []citus.DistributedTable `json:"tables"`
	Meta   Meta                     `json:"meta"`
}

// ListShards tool
type ListShardsInput struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}
type ListShardsOutput struct {
	Shards []citus.Shard `json:"shards"`
	Meta   Meta          `json:"meta"`
}

func ListShards(ctx context.Context, deps Dependencies, input ListShardsInput) (*mcp.CallToolResult, ListShardsOutput, error) {
	limit, offset := normalizeLimitOffset(deps.Config, input.Limit, input.Offset)
	shards, err := citus.ListShards(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ListShardsOutput{Shards: []citus.Shard{}, Meta: Meta{}}, nil
	}
	if shards == nil {
		shards = []citus.Shard{}
	}
	end := offset + limit
	if end > len(shards) {
		end = len(shards)
	}
	if offset > len(shards) {
		offset = len(shards)
	}
	result := shards[offset:end]
	if result == nil {
		result = []citus.Shard{}
	}
	return nil, ListShardsOutput{Shards: result, Meta: Meta{Limit: limit, Offset: offset, Total: len(shards)}}, nil
}

func ListDistributedTables(ctx context.Context, deps Dependencies, input ListDistributedTablesInput) (*mcp.CallToolResult, ListDistributedTablesOutput, error) {
	limit, offset := normalizeLimitOffset(deps.Config, input.Limit, input.Offset)
	tables, err := citus.ListDistributedTables(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ListDistributedTablesOutput{Tables: []citus.DistributedTable{}, Meta: Meta{}}, nil
	}
	if tables == nil {
		tables = []citus.DistributedTable{}
	}
	end := offset + limit
	if end > len(tables) {
		end = len(tables)
	}
	if offset > len(tables) {
		offset = len(tables)
	}
	return nil, ListDistributedTablesOutput{Tables: tables[offset:end], Meta: Meta{Limit: limit, Offset: offset, Total: len(tables)}}, nil
}

// Rebalance tools
type RebalanceTableInput struct {
	Table string `json:"table" jsonschema:"required"`
}
type RebalanceTablePlanOutput struct {
	Plan *citus.RebalancePlan `json:"plan"`
}

func RebalanceTablePlan(ctx context.Context, deps Dependencies, input RebalanceTableInput) (*mcp.CallToolResult, RebalanceTablePlanOutput, error) {
	if input.Table == "" {
		return callError(serr.CodeInvalidInput, "table required", "provide table name"), RebalanceTablePlanOutput{}, nil
	}
	plan, err := citus.PlanRebalanceTable(ctx, deps.Pool, input.Table)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "citus error"), RebalanceTablePlanOutput{}, nil
	}
	return nil, RebalanceTablePlanOutput{Plan: plan}, nil
}

type RebalanceTableExecuteInput struct {
	Table         string `json:"table" jsonschema:"required"`
	ApprovalToken string `json:"approval_token" jsonschema:"required"`
}
type RebalanceTableExecuteOutput struct {
	Status string `json:"status"`
}

func RebalanceTableExecute(ctx context.Context, deps Dependencies, input RebalanceTableExecuteInput) (*mcp.CallToolResult, RebalanceTableExecuteOutput, error) {
	if input.Table == "" {
		return callError(serr.CodeInvalidInput, "table required", "provide table name"), RebalanceTableExecuteOutput{}, nil
	}
	if err := deps.Guardrails.RequireExecuteAllowed(input.ApprovalToken, "rebalance_table:"+input.Table); err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), RebalanceTableExecuteOutput{}, nil
		}
		return callError(serr.CodeApprovalRequired, err.Error(), "approval required"), RebalanceTableExecuteOutput{}, nil
	}
	if err := citus.ExecuteRebalanceTable(ctx, deps.Pool, input.Table); err != nil {
		return callError(serr.CodeInternalError, err.Error(), "citus error"), RebalanceTableExecuteOutput{}, nil
	}
	return nil, RebalanceTableExecuteOutput{Status: "ok"}, nil
}

// Helper error creation
func callError(code serr.ErrorCode, msg, hint string) *mcp.CallToolResult {
	errObj := map[string]any{"code": code, "message": msg}
	if hint != "" {
		errObj["hint"] = hint
	}
	return &mcp.CallToolResult{
		IsError:           true,
		StructuredContent: errObj,
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("%s: %s", code, msg)},
		},
	}
}

func normalizeLimitOffset(cfg config.Config, limit, offset int) (int, int) {
	if limit <= 0 {
		limit = cfg.MaxRows
	}
	if limit > cfg.MaxRows {
		limit = cfg.MaxRows
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
