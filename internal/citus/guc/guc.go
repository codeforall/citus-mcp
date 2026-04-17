// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Package guc provides GUC (Grand Unified Configuration) definitions and analysis
// for Citus and PostgreSQL configuration parameters.
package guc

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Category represents the functional category of a GUC.
type Category string

const (
	CategoryMemory       Category = "memory"
	CategoryConnections  Category = "connections"
	CategoryParallelism  Category = "parallelism"
	CategoryReplication  Category = "replication"
	CategoryPerformance  Category = "performance"
	CategoryDistribution Category = "distribution"
	CategoryExecution    Category = "execution"
	CategoryHA           Category = "high_availability"
	CategoryOperations   Category = "operations"
	CategorySecurity     Category = "security"
)

// GUCDef defines metadata for a configuration parameter.
type GUCDef struct {
	Name           string   `json:"name"`
	Category       Category `json:"category"`
	Description    string   `json:"description"`
	DefaultValue   string   `json:"default_value"`
	RecommendedMin string   `json:"recommended_min,omitempty"`
	RecommendedMax string   `json:"recommended_max,omitempty"`
	Unit           string   `json:"unit,omitempty"`
	Impact         string   `json:"impact"`
	IsCitus        bool     `json:"is_citus"`
	IsCritical     bool     `json:"is_critical"`
}

// GUCValue represents a fetched GUC value from the database.
type GUCValue struct {
	Name       string  `json:"name"`
	Setting    string  `json:"setting"`
	Unit       *string `json:"unit,omitempty"`
	Category   string  `json:"category"`
	ShortDesc  string  `json:"short_desc"`
	Context    string  `json:"context"`
	VarType    string  `json:"vartype"`
	Source     string  `json:"source"`
	MinVal     *string `json:"min_val,omitempty"`
	MaxVal     *string `json:"max_val,omitempty"`
	BootVal    *string `json:"boot_val,omitempty"`
	ResetVal   *string `json:"reset_val,omitempty"`
	PendingRst bool    `json:"pending_restart"`
}

// CitusGUCs defines all important Citus configuration parameters.
var CitusGUCs = map[string]GUCDef{
	// Distribution
	"citus.shard_count": {
		Name:           "citus.shard_count",
		Category:       CategoryDistribution,
		Description:    "Default number of shards for newly distributed tables",
		DefaultValue:   "32",
		RecommendedMin: "32",
		Impact:         "Affects data distribution granularity and parallelism. Higher values = better load balancing but more overhead.",
		IsCitus:        true,
		IsCritical:     true,
	},
	"citus.shard_replication_factor": {
		Name:           "citus.shard_replication_factor",
		Category:       CategoryHA,
		Description:    "Number of replicas for each shard",
		DefaultValue:   "1",
		RecommendedMin: "1",
		RecommendedMax: "2",
		Impact:         "Higher values increase durability but use more storage and slow writes.",
		IsCitus:        true,
		IsCritical:     true,
	},

	// Execution
	"citus.max_adaptive_executor_pool_size": {
		Name:           "citus.max_adaptive_executor_pool_size",
		Category:       CategoryExecution,
		Description:    "Maximum connections per worker for parallel query execution",
		DefaultValue:   "16",
		RecommendedMin: "8",
		RecommendedMax: "32",
		Impact:         "Controls parallelism of distributed queries. Too high may exhaust worker connections.",
		IsCitus:        true,
		IsCritical:     false,
	},
	"citus.executor_slow_start_interval": {
		Name:         "citus.executor_slow_start_interval",
		Category:     CategoryExecution,
		Description:  "Interval between opening new connections during query execution",
		DefaultValue: "10ms",
		Unit:         "ms",
		Impact:       "Lower values = faster ramp-up but may spike connection usage.",
		IsCitus:      true,
	},
	"citus.multi_shard_modify_mode": {
		Name:         "citus.multi_shard_modify_mode",
		Category:     CategoryExecution,
		Description:  "How multi-shard modifications are executed",
		DefaultValue: "parallel",
		Impact:       "parallel=faster but uses more connections; sequential=slower but predictable.",
		IsCitus:      true,
	},
	"citus.enable_repartition_joins": {
		Name:         "citus.enable_repartition_joins",
		Category:     CategoryPerformance,
		Description:  "Allow repartitioning for non-colocated joins",
		DefaultValue: "on",
		Impact:       "Enables cross-shard joins via data movement. Disable if joins should fail-fast.",
		IsCitus:      true,
	},
	"citus.task_assignment_policy": {
		Name:         "citus.task_assignment_policy",
		Category:     CategoryExecution,
		Description:  "How tasks are assigned to worker nodes",
		DefaultValue: "greedy",
		Impact:       "greedy=prefer local; round-robin=distribute evenly; first-replica=deterministic.",
		IsCitus:      true,
	},

	// Operations
	"citus.max_background_task_executors_per_node": {
		Name:           "citus.max_background_task_executors_per_node",
		Category:       CategoryOperations,
		Description:    "Parallel background task executors per node (shard moves, etc.)",
		DefaultValue:   "1",
		RecommendedMin: "1",
		RecommendedMax: "4",
		Impact:         "Higher = faster rebalancing but more resource usage during operations.",
		IsCitus:        true,
	},
	"citus.node_connection_timeout": {
		Name:         "citus.node_connection_timeout",
		Category:     CategoryOperations,
		Description:  "Timeout for establishing worker connections",
		DefaultValue: "5s",
		Unit:         "s",
		Impact:       "Too short may cause spurious failures; too long delays error detection.",
		IsCitus:      true,
	},
	"citus.log_distributed_deadlock_detection": {
		Name:         "citus.log_distributed_deadlock_detection",
		Category:     CategoryOperations,
		Description:  "Log distributed deadlock detection activity",
		DefaultValue: "off",
		Impact:       "Enable for debugging distributed deadlocks.",
		IsCitus:      true,
	},
	"citus.distributed_deadlock_detection_factor": {
		Name:         "citus.distributed_deadlock_detection_factor",
		Category:     CategoryOperations,
		Description:  "Multiplier for deadlock detection timeout",
		DefaultValue: "2",
		Impact:       "Lower = faster deadlock detection but more overhead.",
		IsCitus:      true,
	},

	// High Availability
	"citus.use_secondary_nodes": {
		Name:         "citus.use_secondary_nodes",
		Category:     CategoryHA,
		Description:  "Route SELECT queries to secondary (read replica) nodes",
		DefaultValue: "never",
		Impact:       "Set to 'always' to offload reads to replicas in HA setups.",
		IsCitus:      true,
	},
	"citus.cluster_name": {
		Name:         "citus.cluster_name",
		Category:     CategoryHA,
		Description:  "Cluster identifier for multi-cluster setups",
		DefaultValue: "default",
		Impact:       "Used in multi-datacenter or multi-cluster deployments.",
		IsCitus:      true,
	},

	// Connections
	"citus.max_client_connections": {
		Name:           "citus.max_client_connections",
		Category:       CategoryConnections,
		Description:    "Maximum client connections to coordinator",
		DefaultValue:   "0",
		RecommendedMin: "100",
		Impact:         "0=no limit. Set to prevent connection exhaustion.",
		IsCitus:        true,
	},
	"citus.node_conninfo": {
		Name:        "citus.node_conninfo",
		Category:    CategoryConnections,
		Description: "Connection string options for worker connections",
		Impact:      "Configure SSL, timeouts, and other libpq options for worker connections.",
		IsCitus:     true,
	},

	// Performance
	"citus.enable_local_execution": {
		Name:         "citus.enable_local_execution",
		Category:     CategoryPerformance,
		Description:  "Execute queries locally when possible",
		DefaultValue: "on",
		Impact:       "Reduces network overhead for single-shard queries on coordinator.",
		IsCitus:      true,
	},
	"citus.enable_fast_path_router_planner": {
		Name:         "citus.enable_fast_path_router_planner",
		Category:     CategoryPerformance,
		Description:  "Use fast-path planner for simple routed queries",
		DefaultValue: "on",
		Impact:       "Speeds up simple single-shard queries significantly.",
		IsCitus:      true,
	},
	"citus.explain_all_tasks": {
		Name:         "citus.explain_all_tasks",
		Category:     CategoryPerformance,
		Description:  "Show EXPLAIN output for all tasks",
		DefaultValue: "off",
		Impact:       "Enable for debugging distributed query plans.",
		IsCitus:      true,
	},
	"citus.enable_statistics_collection": {
		Name:         "citus.enable_statistics_collection",
		Category:     CategoryPerformance,
		Description:  "Collect tenant-level statistics",
		DefaultValue: "off",
		Impact:       "Enable for citus_stat_tenants monitoring.",
		IsCitus:      true,
	},

	// Rebalancing
	"citus.rebalance_by_disk_size": {
		Name:         "citus.rebalance_by_disk_size",
		Category:     CategoryOperations,
		Description:  "Consider disk size in rebalancing decisions",
		DefaultValue: "off",
		Impact:       "Enable for more accurate rebalancing based on actual data size.",
		IsCitus:      true,
	},
	"citus.desired_percent_disk_available_after_move": {
		Name:         "citus.desired_percent_disk_available_after_move",
		Category:     CategoryOperations,
		Description:  "Target free disk percentage after shard moves",
		DefaultValue: "10",
		Unit:         "%",
		Impact:       "Higher = safer but may limit rebalancing options.",
		IsCitus:      true,
	},
}

// PostgresGUCs defines PostgreSQL parameters critical for Citus operation.
var PostgresGUCs = map[string]GUCDef{
	// Memory
	"shared_buffers": {
		Name:           "shared_buffers",
		Category:       CategoryMemory,
		Description:    "Memory for shared buffer cache",
		DefaultValue:   "128MB",
		RecommendedMin: "256MB",
		Unit:           "8kB",
		Impact:         "Set to 15-25% of RAM. Too high may starve OS cache.",
		IsCritical:     true,
	},
	"work_mem": {
		Name:           "work_mem",
		Category:       CategoryMemory,
		Description:    "Memory for query operations (sorts, hashes)",
		DefaultValue:   "4MB",
		RecommendedMin: "16MB",
		RecommendedMax: "256MB",
		Unit:           "kB",
		Impact:         "Higher = faster sorts/joins but risk OOM with many connections.",
		IsCritical:     true,
	},
	"maintenance_work_mem": {
		Name:           "maintenance_work_mem",
		Category:       CategoryMemory,
		Description:    "Memory for maintenance operations (VACUUM, CREATE INDEX)",
		DefaultValue:   "64MB",
		RecommendedMin: "256MB",
		Unit:           "kB",
		Impact:         "Higher = faster maintenance but uses memory during operations.",
		IsCritical:     false,
	},
	"effective_cache_size": {
		Name:           "effective_cache_size",
		Category:       CategoryMemory,
		Description:    "Planner estimate of available cache",
		DefaultValue:   "4GB",
		RecommendedMin: "1GB",
		Unit:           "8kB",
		Impact:         "Set to ~50-75% of RAM. Affects query planner decisions.",
		IsCritical:     false,
	},

	// Connections
	"max_connections": {
		Name:           "max_connections",
		Category:       CategoryConnections,
		Description:    "Maximum concurrent connections",
		DefaultValue:   "100",
		RecommendedMin: "100",
		RecommendedMax: "500",
		Impact:         "Must accommodate coordinator + worker internal connections.",
		IsCritical:     true,
	},
	"superuser_reserved_connections": {
		Name:         "superuser_reserved_connections",
		Category:     CategoryConnections,
		Description:  "Connections reserved for superusers",
		DefaultValue: "3",
		Impact:       "Ensure admins can connect during connection exhaustion.",
	},
	"max_locks_per_transaction": {
		Name:           "max_locks_per_transaction",
		Category:       CategoryConnections,
		Description:    "Shared lock-table slots per backend (pre-allocated at startup)",
		DefaultValue:   "64",
		RecommendedMin: "256",
		Impact:         "Citus locks every touched shard in one transaction. Metadata sync, rebalance, DDL fan-out and multi-shard updates can exhaust the lock table (error: 'out of shared memory'). Recommended value scales with shard count: 256 for <= 1k shards, 1024 for 1k-10k, 2048+ for > 10k. The lock table sizes as max_locks_per_transaction × MaxBackends × ~270 B of shared memory.",
		IsCritical:     true,
	},
	"max_pred_locks_per_transaction": {
		Name:         "max_pred_locks_per_transaction",
		Category:     CategoryConnections,
		Description:  "Predicate lock slots per backend (SERIALIZABLE only)",
		DefaultValue: "64",
		Impact:       "Only relevant under SERIALIZABLE isolation; safe to leave default otherwise.",
	},

	// Parallelism
	"max_worker_processes": {
		Name:           "max_worker_processes",
		Category:       CategoryParallelism,
		Description:    "Maximum background worker processes",
		DefaultValue:   "8",
		RecommendedMin: "16",
		Impact:         "Citus uses background workers for maintenance and rebalancing.",
		IsCritical:     true,
	},
	"max_parallel_workers_per_gather": {
		Name:         "max_parallel_workers_per_gather",
		Category:     CategoryParallelism,
		Description:  "Maximum parallel workers per query",
		DefaultValue: "2",
		Impact:       "Affects local parallelism on each shard.",
	},
	"max_parallel_workers": {
		Name:         "max_parallel_workers",
		Category:     CategoryParallelism,
		Description:  "Maximum parallel workers system-wide",
		DefaultValue: "8",
		Impact:       "Limits total parallel workers across all queries.",
	},
	"max_parallel_maintenance_workers": {
		Name:         "max_parallel_maintenance_workers",
		Category:     CategoryParallelism,
		Description:  "Maximum parallel workers for maintenance",
		DefaultValue: "2",
		Impact:       "Affects parallel index builds and vacuum.",
	},

	// Replication (Critical for Citus)
	"wal_level": {
		Name:         "wal_level",
		Category:     CategoryReplication,
		Description:  "WAL logging level",
		DefaultValue: "replica",
		Impact:       "MUST be 'logical' for shard moves, rebalancing, and CDC.",
		IsCritical:   true,
	},
	"max_wal_senders": {
		Name:           "max_wal_senders",
		Category:       CategoryReplication,
		Description:    "Maximum WAL sender processes",
		DefaultValue:   "10",
		RecommendedMin: "10",
		Impact:         "Needed for logical replication during shard moves.",
		IsCritical:     true,
	},
	"max_replication_slots": {
		Name:           "max_replication_slots",
		Category:       CategoryReplication,
		Description:    "Maximum replication slots",
		DefaultValue:   "10",
		RecommendedMin: "10",
		Impact:         "Each concurrent shard move uses replication slots.",
		IsCritical:     true,
	},
	"max_logical_replication_workers": {
		Name:           "max_logical_replication_workers",
		Category:       CategoryReplication,
		Description:    "Maximum logical replication workers",
		DefaultValue:   "4",
		RecommendedMin: "4",
		Impact:         "Limits parallel logical replication operations.",
	},

	// Performance
	"random_page_cost": {
		Name:         "random_page_cost",
		Category:     CategoryPerformance,
		Description:  "Planner cost for random page fetch",
		DefaultValue: "4.0",
		Impact:       "Set to 1.1-1.5 for SSDs to prefer index scans.",
	},
	"effective_io_concurrency": {
		Name:         "effective_io_concurrency",
		Category:     CategoryPerformance,
		Description:  "Concurrent I/O operations",
		DefaultValue: "1",
		Impact:       "Set to 200 for SSDs, 2 for spinning disks.",
	},
	"default_statistics_target": {
		Name:         "default_statistics_target",
		Category:     CategoryPerformance,
		Description:  "Statistics collection granularity",
		DefaultValue: "100",
		Impact:       "Higher = better estimates but slower ANALYZE.",
	},

	// Startup
	"shared_preload_libraries": {
		Name:         "shared_preload_libraries",
		Category:     CategoryOperations,
		Description:  "Libraries to preload at server start",
		DefaultValue: "",
		Impact:       "MUST include 'citus' for Citus to function.",
		IsCritical:   true,
	},

	// Timeouts
	"statement_timeout": {
		Name:         "statement_timeout",
		Category:     CategoryOperations,
		Description:  "Maximum statement execution time",
		DefaultValue: "0",
		Unit:         "ms",
		Impact:       "Set appropriately to prevent runaway queries. 0=disabled.",
	},
	"lock_timeout": {
		Name:         "lock_timeout",
		Category:     CategoryOperations,
		Description:  "Maximum time to wait for locks",
		DefaultValue: "0",
		Unit:         "ms",
		Impact:       "Set to prevent indefinite lock waits. 0=disabled.",
	},
	"idle_in_transaction_session_timeout": {
		Name:         "idle_in_transaction_session_timeout",
		Category:     CategoryOperations,
		Description:  "Maximum idle time in transaction",
		DefaultValue: "0",
		Unit:         "ms",
		Impact:       "Prevents abandoned transactions from holding locks.",
	},

	// Logging
	"log_min_duration_statement": {
		Name:         "log_min_duration_statement",
		Category:     CategoryOperations,
		Description:  "Log statements exceeding this duration",
		DefaultValue: "-1",
		Unit:         "ms",
		Impact:       "Set to identify slow queries. -1=disabled.",
	},
}

// FetchAllGUCs retrieves all GUC values from the database.
func FetchAllGUCs(ctx context.Context, pool *pgxpool.Pool) (map[string]GUCValue, error) {
	query := `
		SELECT 
			name,
			setting,
			unit,
			category,
			short_desc,
			context,
			vartype,
			source,
			min_val,
			max_val,
			boot_val,
			reset_val,
			pending_restart
		FROM pg_settings
		ORDER BY name
	`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_settings: %w", err)
	}
	defer rows.Close()

	gucs := make(map[string]GUCValue)
	for rows.Next() {
		var g GUCValue
		if err := rows.Scan(
			&g.Name, &g.Setting, &g.Unit, &g.Category, &g.ShortDesc,
			&g.Context, &g.VarType, &g.Source, &g.MinVal, &g.MaxVal,
			&g.BootVal, &g.ResetVal, &g.PendingRst,
		); err != nil {
			return nil, fmt.Errorf("failed to scan GUC row: %w", err)
		}
		gucs[g.Name] = g
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating GUCs: %w", err)
	}
	return gucs, nil
}

// FetchCitusGUCs retrieves only Citus-related GUCs.
func FetchCitusGUCs(ctx context.Context, pool *pgxpool.Pool) (map[string]GUCValue, error) {
	query := `
		SELECT 
			name,
			setting,
			unit,
			category,
			short_desc,
			context,
			vartype,
			source,
			min_val,
			max_val,
			boot_val,
			reset_val,
			pending_restart
		FROM pg_settings
		WHERE name LIKE 'citus.%'
		ORDER BY name
	`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query citus GUCs: %w", err)
	}
	defer rows.Close()

	gucs := make(map[string]GUCValue)
	for rows.Next() {
		var g GUCValue
		if err := rows.Scan(
			&g.Name, &g.Setting, &g.Unit, &g.Category, &g.ShortDesc,
			&g.Context, &g.VarType, &g.Source, &g.MinVal, &g.MaxVal,
			&g.BootVal, &g.ResetVal, &g.PendingRst,
		); err != nil {
			return nil, fmt.Errorf("failed to scan GUC row: %w", err)
		}
		gucs[g.Name] = g
	}
	return gucs, rows.Err()
}

// FetchRelevantPostgresGUCs retrieves PostgreSQL GUCs that affect Citus.
func FetchRelevantPostgresGUCs(ctx context.Context, pool *pgxpool.Pool) (map[string]GUCValue, error) {
	names := make([]string, 0, len(PostgresGUCs))
	for name := range PostgresGUCs {
		names = append(names, name)
	}

	query := `
		SELECT 
			name,
			setting,
			unit,
			category,
			short_desc,
			context,
			vartype,
			source,
			min_val,
			max_val,
			boot_val,
			reset_val,
			pending_restart
		FROM pg_settings
		WHERE name = ANY($1)
		ORDER BY name
	`
	rows, err := pool.Query(ctx, query, names)
	if err != nil {
		return nil, fmt.Errorf("failed to query postgres GUCs: %w", err)
	}
	defer rows.Close()

	gucs := make(map[string]GUCValue)
	for rows.Next() {
		var g GUCValue
		if err := rows.Scan(
			&g.Name, &g.Setting, &g.Unit, &g.Category, &g.ShortDesc,
			&g.Context, &g.VarType, &g.Source, &g.MinVal, &g.MaxVal,
			&g.BootVal, &g.ResetVal, &g.PendingRst,
		); err != nil {
			return nil, fmt.Errorf("failed to scan GUC row: %w", err)
		}
		gucs[g.Name] = g
	}
	return gucs, rows.Err()
}

// ParseBytes parses a PostgreSQL memory setting to bytes.
func ParseBytes(setting, unit string) (int64, error) {
	val, err := strconv.ParseInt(setting, 10, 64)
	if err != nil {
		return 0, err
	}
	switch strings.ToLower(unit) {
	case "8kb":
		return val * 8 * 1024, nil
	case "kb":
		return val * 1024, nil
	case "mb":
		return val * 1024 * 1024, nil
	case "gb":
		return val * 1024 * 1024 * 1024, nil
	case "":
		return val, nil
	default:
		return val, nil
	}
}

// FormatBytes formats bytes to human-readable string.
func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1fGB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1fMB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1fKB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// ParseInt safely parses an integer setting.
func ParseInt(setting string) (int64, error) {
	return strconv.ParseInt(setting, 10, 64)
}

// ParseBool parses a PostgreSQL boolean setting.
func ParseBool(setting string) bool {
	switch strings.ToLower(setting) {
	case "on", "true", "yes", "1":
		return true
	default:
		return false
	}
}
