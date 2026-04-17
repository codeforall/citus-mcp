// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B1: citus_metadata_cache_footprint --
// Predict the per-backend Citus MetadataCacheMemoryContext size from catalog
// shape, optionally measure it live, and roll up the projected
// cluster-wide memory impact against max_connections. Emits alarms when the
// projected footprint crosses configurable thresholds.

package tools

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/diagnostics/memory"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MetadataCacheFootprintInput controls the probe.
type MetadataCacheFootprintInput struct {
	// Measure, when true, acquires a dedicated connection, forces Citus to
	// populate its MetadataCache for every distributed table, and queries
	// pg_backend_memory_contexts for the actual observed size. When false,
	// only the estimator is used.
	Measure bool `json:"measure,omitempty"`

	// ActiveBackendsOverride, if >0, is used instead of probing
	// pg_stat_activity for the projected cluster-wide total. Useful for
	// what-if analyses ("what if we had 500 connections").
	ActiveBackendsOverride int `json:"active_backends_override,omitempty"`

	// WarnBytes / CritBytes override the alarm thresholds for per-backend
	// estimate. Defaults: warn at 128 MiB, critical at 512 MiB per backend.
	WarnBytes     int64 `json:"warn_bytes,omitempty"`
	CriticalBytes int64 `json:"critical_bytes,omitempty"`

	// TypicalTablesTouched overrides the auto-detected typical scenario.
	// When <=0, the tool defaults to min(5, N) or derives from pg_stat_statements.
	TypicalTablesTouched int `json:"typical_tables_touched,omitempty"`

	// HotPathTablesTouched overrides the hot-path scenario. Default: ceil(N/2).
	HotPathTablesTouched int `json:"hot_path_tables_touched,omitempty"`

	// MemoryBudgetBytes is the memory ceiling available for per-backend metadata
	// caches on this node. If 0, the tool tries to read /proc/meminfo (via
	// pg_read_file when available) and uses 25% of MemTotal as a conservative
	// budget. Used to compute concurrent_worst_case_cap.
	MemoryBudgetBytes int64 `json:"memory_budget_bytes,omitempty"`

	// PartitionGrowthFactor simulates increasing every partitioned table's
	// child count by this factor (e.g. 2.0 = double monthly partitions).
	// When <=1 the partition-explosion section is omitted.
	PartitionGrowthFactor float64 `json:"partition_growth_factor,omitempty"`

	// SkipWorstCaseBackendScan disables the pg_stat_activity probe that looks
	// for backends currently holding a fully-warm metadata cache.
	SkipWorstCaseBackendScan bool `json:"skip_worst_case_backend_scan,omitempty"`

	// SkipQueryCorrelation disables the pg_stat_statements-based estimation of
	// tables-touched-per-query used to auto-derive typical_tables_touched.
	SkipQueryCorrelation bool `json:"skip_query_correlation,omitempty"`
}

// MetadataCacheFootprintOutput is the tool response.
type MetadataCacheFootprintOutput struct {
	Estimate         memory.CitusEstimate          `json:"estimate"`
	Measured         *memory.BackendCtxMeasurement `json:"measured,omitempty"`
	ActiveBackends   int                           `json:"active_backends"`
	MaxConnections   int                           `json:"max_connections"`
	ProjectedCluster ProjectedClusterFootprint     `json:"projected_cluster"`
	TopTables        []TableFootprintEntry         `json:"top_tables"`

	Scenarios          *CacheScenarios        `json:"scenarios,omitempty"`
	WorstCaseBackends  []WorstCaseBackend     `json:"worst_case_backends,omitempty"`
	ConcurrentCap      *ConcurrentWorstCap    `json:"concurrent_worst_case_cap,omitempty"`
	PartitionExplosion *PartitionExplosion    `json:"partition_explosion,omitempty"`
	QueryCorrelation   *QueryCorrelation      `json:"query_correlation,omitempty"`

	Alarms []diagnostics.Alarm `json:"alarms"`
	Notes  []string            `json:"notes,omitempty"`
}

// ProjectedClusterFootprint extrapolates per-backend to cluster-wide.
type ProjectedClusterFootprint struct {
	PerBackendBytes    int64  `json:"per_backend_bytes"`
	ActiveBackendBytes int64  `json:"active_backend_bytes"`
	MaxConnectionBytes int64  `json:"max_connection_bytes"`
	Note               string `json:"note"`
}

// CacheScenarios captures the three memory regimes a backend can be in.
type CacheScenarios struct {
	Typical   ScenarioEstimate `json:"typical"`
	HotPath   ScenarioEstimate `json:"hot_path"`
	WorstCase ScenarioEstimate `json:"worst_case"`
	Note      string           `json:"note"`
}

// ScenarioEstimate describes a single regime.
type ScenarioEstimate struct {
	Label                string `json:"label"`
	TablesTouched        int    `json:"tables_touched"`
	TablesTotal          int    `json:"tables_total"`
	PerBackendBytes      int64  `json:"per_backend_bytes"`
	LowBytes             int64  `json:"low_bytes"`
	HighBytes            int64  `json:"high_bytes"`
	ProjectedAtMaxConnBytes int64 `json:"projected_at_max_conn_bytes"`
	Note                 string `json:"note,omitempty"`
}

// WorstCaseBackend identifies a currently-active backend that is (or is
// likely to be) holding a fully-warm Citus metadata cache.
type WorstCaseBackend struct {
	PID             int    `json:"pid"`
	Application     string `json:"application_name,omitempty"`
	BackendType     string `json:"backend_type,omitempty"`
	State           string `json:"state,omitempty"`
	Username        string `json:"usename,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty"`
	QueryPreview    string `json:"query_preview,omitempty"`
	MatchedMarker   string `json:"matched_marker"`
	EstimatedBytes  int64  `json:"estimated_bytes"`
}

// ConcurrentWorstCap reports how many simultaneous full-cache backends the
// node can tolerate before the metadata-cache memory budget is breached.
type ConcurrentWorstCap struct {
	MemoryBudgetBytes   int64  `json:"memory_budget_bytes"`
	BudgetSource        string `json:"budget_source"`
	PerBackendWorstBytes int64 `json:"per_backend_worst_bytes"`
	MaxConcurrent       int    `json:"max_concurrent"`
	CurrentWorstCount   int    `json:"current_worst_count"`
	HeadroomConnections int    `json:"headroom_connections"`
	Note                string `json:"note,omitempty"`
}

// PartitionExplosion simulates what happens if partition counts grow.
type PartitionExplosion struct {
	GrowthFactor       float64 `json:"growth_factor"`
	CurrentChildren    int     `json:"current_children"`
	ProjectedChildren  int     `json:"projected_children"`
	CurrentWorstBytes  int64   `json:"current_worst_bytes"`
	ProjectedWorstBytes int64  `json:"projected_worst_bytes"`
	DeltaPerBackend    int64   `json:"delta_per_backend_bytes"`
	DeltaAtMaxConn     int64   `json:"delta_at_max_connections_bytes"`
	Note               string  `json:"note"`
}

// QueryCorrelation reports pg_stat_statements-derived insights about the
// typical number of distributed tables a query touches.
type QueryCorrelation struct {
	Source                   string  `json:"source"`
	StatementsAnalyzed       int     `json:"statements_analyzed"`
	AvgTablesPerStatement    float64 `json:"avg_tables_per_statement"`
	MedianTablesPerStatement int     `json:"median_tables_per_statement"`
	P95TablesPerStatement    int     `json:"p95_tables_per_statement"`
	MaxTablesPerStatement    int     `json:"max_tables_per_statement"`
	UsedAsTypicalDefault     bool    `json:"used_as_typical_default"`
	Note                     string  `json:"note,omitempty"`
}

// TableFootprintEntry surfaces the biggest per-table contributors.
type TableFootprintEntry struct {
	Name              string `json:"name"`
	Schema            string `json:"schema"`
	NShards           int    `json:"n_shards"`
	ReplicationFactor int    `json:"replication_factor"`
	IsPartitionParent bool   `json:"is_partition_parent"`
	IsReference       bool   `json:"is_reference"`
	EstimatedBytes    int64  `json:"estimated_bytes"`
}

// MetadataCacheFootprintTool implements the tool.
func MetadataCacheFootprintTool(ctx context.Context, deps Dependencies, in MetadataCacheFootprintInput) (*mcp.CallToolResult, MetadataCacheFootprintOutput, error) {
	out := MetadataCacheFootprintOutput{Alarms: []diagnostics.Alarm{}, TopTables: []TableFootprintEntry{}, Notes: []string{}}

	if in.WarnBytes <= 0 {
		in.WarnBytes = 128 * 1024 * 1024
	}
	if in.CriticalBytes <= 0 {
		in.CriticalBytes = 512 * 1024 * 1024
	}

	shapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}

	out.Estimate = memory.EstimateCitusMetadata(shapes)

	// per-table breakdown (top 10 by estimated bytes)
	tableEst := make([]TableFootprintEntry, 0, len(shapes))
	for _, s := range shapes {
		// reuse estimator for a single-table set to get per-table bytes excl. base
		one := memory.EstimateCitusMetadata([]memory.TableShape{s})
		one.Bytes -= memory.CitusMetadataBaseBytes
		tableEst = append(tableEst, TableFootprintEntry{
			Name: s.Name, Schema: s.Schema, NShards: s.NShards,
			ReplicationFactor: s.ReplicationFactor,
			IsPartitionParent: s.IsPartitionParent, IsReference: s.IsReference,
			EstimatedBytes: one.Bytes,
		})
	}
	// sort desc by EstimatedBytes, keep top 10
	for i := 1; i < len(tableEst); i++ {
		for j := i; j > 0 && tableEst[j].EstimatedBytes > tableEst[j-1].EstimatedBytes; j-- {
			tableEst[j], tableEst[j-1] = tableEst[j-1], tableEst[j]
		}
	}
	if len(tableEst) > 10 {
		tableEst = tableEst[:10]
	}
	out.TopTables = tableEst

	// connection counts
	conn, err := deps.Pool.Acquire(ctx)
	if err == nil {
		defer conn.Release()
		_ = conn.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&out.MaxConnections)
		_ = conn.QueryRow(ctx, `SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend'`).Scan(&out.ActiveBackends)

		if in.Measure {
			// Force metadata cache population, then probe contexts. Run inside
			// a dedicated connection so the measurement reflects this session.
			n, terr := memory.TouchAllDistributedMetadata(ctx, conn.Conn())
			if terr != nil {
				out.Notes = append(out.Notes, fmt.Sprintf("touch metadata skipped: %v", terr))
			} else {
				out.Notes = append(out.Notes, fmt.Sprintf("touched %d distributed tables to populate cache", n))
			}
			meas, perr := memory.Probe(ctx, conn.Conn(), []string{"MetadataCacheMemoryContext"})
			if perr != nil {
				out.Notes = append(out.Notes, fmt.Sprintf("probe failed: %v", perr))
			} else {
				for i := range meas {
					if meas[i].Scope == "MetadataCacheMemoryContext" {
						m := meas[i]
						out.Measured = &m
					}
				}
			}
		}
	}

	effBackends := in.ActiveBackendsOverride
	if effBackends <= 0 {
		effBackends = out.ActiveBackends
	}
	out.ProjectedCluster = ProjectedClusterFootprint{
		PerBackendBytes:    out.Estimate.Bytes,
		ActiveBackendBytes: out.Estimate.Bytes * int64(effBackends),
		MaxConnectionBytes: out.Estimate.Bytes * int64(out.MaxConnections),
		Note:               "per_backend × {active_backends, max_connections}; cache is per-process, not shared.",
	}

	// Alarms.
	severity := ""
	switch {
	case out.Estimate.Bytes >= in.CriticalBytes:
		severity = string(diagnostics.SeverityCritical)
	case out.Estimate.Bytes >= in.WarnBytes:
		severity = string(diagnostics.SeverityWarning)
	}
	if severity != "" && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "metadata_cache.footprint",
			Severity: diagnostics.Severity(severity),
			Source:   "citus_metadata_cache_footprint",
			Message:  fmt.Sprintf("Citus MetadataCacheMemoryContext estimated at %d bytes per backend (%d shards across %d entries)", out.Estimate.Bytes, out.Estimate.NShards, out.Estimate.NTableEntries),
			Evidence: map[string]any{
				"per_backend_bytes":    out.Estimate.Bytes,
				"n_shards":             out.Estimate.NShards,
				"n_table_entries":      out.Estimate.NTableEntries,
				"max_connections":      out.MaxConnections,
				"projected_max_total":  out.ProjectedCluster.MaxConnectionBytes,
			},
			FixHint: "Reduce shard or partition count for dominant tables; lower max_connections; or pool at the app layer.",
			DocsURL: "https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	// Additional alarm: measured vs estimated drift > 2×, likely calibration
	// issue or unexpected growth driver.
	if out.Measured != nil && out.Measured.UsedBytes > 0 && out.Estimate.Bytes > 0 {
		ratio := float64(out.Measured.UsedBytes) / float64(out.Estimate.Bytes)
		if ratio > 2.0 && deps.Alarms != nil {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind:     "metadata_cache.estimator_drift",
				Severity: diagnostics.SeverityWarning,
				Source:   "citus_metadata_cache_footprint",
				Message:  fmt.Sprintf("Measured MetadataCacheMemoryContext is %.1fx the estimate; estimator may need retuning or workload has unexpected driver", ratio),
				Evidence: map[string]any{"measured_used_bytes": out.Measured.UsedBytes, "estimate_bytes": out.Estimate.Bytes, "ratio": ratio},
				FixHint:  "Inspect top contexts via citus_worker_memcontexts; open an issue on citus-mcp with the catalog shape.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}

	// --- Enhanced signals ------------------------------------------------
	out.QueryCorrelation = computeQueryCorrelation(ctx, deps, shapes, in)
	out.Scenarios = buildScenarios(shapes, out.Estimate, out.MaxConnections, in, out.QueryCorrelation)
	out.WorstCaseBackends = detectWorstCaseBackends(ctx, deps, out.Estimate.Bytes, in)
	out.ConcurrentCap = computeConcurrentCap(ctx, deps, out.Estimate.Bytes, len(out.WorstCaseBackends), in)
	out.PartitionExplosion = simulatePartitionExplosion(shapes, out.Estimate, out.MaxConnections, in)

	// Invalidation storm note: triggered whenever any partition parent exists.
	anyPartitionParent := false
	totalChildren := 0
	for _, s := range shapes {
		if s.IsPartitionParent && s.NPartitionChildren > 0 {
			anyPartitionParent = true
			totalChildren += s.NPartitionChildren
		}
	}
	if anyPartitionParent {
		out.Notes = append(out.Notes,
			fmt.Sprintf("invalidation_storm_note: %d partition children across the cluster. DDL events (ATTACH/DETACH PARTITION, ALTER, shard moves) fire SysCache invalidations that force backends to rebuild metadata entries; during the rebuild a backend can transiently hold ~2× its warm footprint. High DDL cadence during partition rollover can therefore cause short memory spikes independent of the steady-state numbers above.", totalChildren))
	}

	// Alarm: worst-case footprint × max_connections > memory budget.
	if out.ConcurrentCap != nil && out.ConcurrentCap.HeadroomConnections < 0 && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "metadata_cache.budget_exceeded",
			Severity: diagnostics.SeverityCritical,
			Source:   "citus_metadata_cache_footprint",
			Message:  fmt.Sprintf("Worst-case metadata cache × max_connections (%d) exceeds %d-byte budget by %d connections", out.MaxConnections, out.ConcurrentCap.MemoryBudgetBytes, -out.ConcurrentCap.HeadroomConnections),
			Evidence: map[string]any{
				"per_backend_worst_bytes": out.ConcurrentCap.PerBackendWorstBytes,
				"max_connections":         out.MaxConnections,
				"memory_budget_bytes":     out.ConcurrentCap.MemoryBudgetBytes,
				"max_concurrent":          out.ConcurrentCap.MaxConcurrent,
			},
			FixHint: "Lower max_connections, add a connection pooler, reduce shard count, or allocate more RAM to the metadata-cache budget.",
			DocsURL: "https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	return nil, out, nil
}

// ---------------------------------------------------------------------------
// Helpers for the enhanced signals.
// ---------------------------------------------------------------------------

// worstCaseMarkers is the set of pg_stat_activity query fragments that imply
// a backend has (or will shortly) warmed the full Citus metadata cache. Each
// entry must be a lowercase substring match.
var worstCaseMarkers = []string{
	"citus_rebalance",
	"rebalance_table_shards",
	"replicate_reference_tables",
	"citus_add_node", "citus_remove_node", "citus_update_node",
	"citus_activate_node", "citus_disable_node",
	"start_metadata_sync_to_node", "stop_metadata_sync_to_node",
	"citus_tables", "citus_shards",
	"run_command_on_shards", "run_command_on_placements",
	"run_command_on_colocated_placements",
	"pg_dump",
	"citus_finalize_upgrade_to_citus11",
	"citus_drain_node",
	"citus_move_shard_placement", "master_move_shard_placement",
	"citus_copy_shard_placement",
}

func detectWorstCaseBackends(ctx context.Context, deps Dependencies, perBackendBytes int64, in MetadataCacheFootprintInput) []WorstCaseBackend {
	if in.SkipWorstCaseBackendScan {
		return nil
	}
	rows, err := deps.Pool.Query(ctx, `
SELECT pid,
       COALESCE(application_name,''),
       COALESCE(backend_type,''),
       COALESCE(state,''),
       COALESCE(usename,''),
       COALESCE(EXTRACT(EPOCH FROM (now() - query_start))::bigint, 0) AS duration_seconds,
       LEFT(COALESCE(query,''), 400) AS q
FROM pg_stat_activity
WHERE backend_type IN ('client backend','parallel worker','background worker')
  AND pid <> pg_backend_pid()
  AND query IS NOT NULL
  AND query <> ''`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var found []WorstCaseBackend
	for rows.Next() {
		var b WorstCaseBackend
		if err := rows.Scan(&b.PID, &b.Application, &b.BackendType, &b.State, &b.Username, &b.DurationSeconds, &b.QueryPreview); err != nil {
			continue
		}
		ql := strings.ToLower(b.QueryPreview)
		for _, m := range worstCaseMarkers {
			if strings.Contains(ql, m) {
				b.MatchedMarker = m
				b.EstimatedBytes = perBackendBytes
				found = append(found, b)
				break
			}
		}
	}
	return found
}

func buildScenarios(shapes []memory.TableShape, worst memory.CitusEstimate, maxConn int, in MetadataCacheFootprintInput, qc *QueryCorrelation) *CacheScenarios {
	n := len(shapes)
	if n == 0 {
		return nil
	}
	// Resolve typical: input override > pg_stat_statements derived > heuristic.
	typical := in.TypicalTablesTouched
	typicalNote := "input override"
	if typical <= 0 && qc != nil && qc.MedianTablesPerStatement > 0 {
		typical = qc.MedianTablesPerStatement
		typicalNote = "pg_stat_statements median of distributed tables referenced per statement"
		qc.UsedAsTypicalDefault = true
	}
	if typical <= 0 {
		typical = 5
		if typical > n {
			typical = n
		}
		typicalNote = "heuristic default min(5, N)"
	}
	if typical > n {
		typical = n
	}
	hot := in.HotPathTablesTouched
	hotNote := "input override"
	if hot <= 0 {
		hot = (n + 1) / 2
		hotNote = "default ceil(N/2) — analytical / reporting workload"
	}
	if hot > n {
		hot = n
	}
	if hot < typical {
		hot = typical
	}
	typicalEst := memory.EstimateCitusMetadataScaled(shapes, typical)
	hotEst := memory.EstimateCitusMetadataScaled(shapes, hot)
	mc := int64(maxConn)
	return &CacheScenarios{
		Typical: ScenarioEstimate{
			Label: "typical", TablesTouched: typical, TablesTotal: n,
			PerBackendBytes: typicalEst.Bytes, LowBytes: typicalEst.LowBytes, HighBytes: typicalEst.HighBytes,
			ProjectedAtMaxConnBytes: typicalEst.Bytes * mc,
			Note: typicalNote,
		},
		HotPath: ScenarioEstimate{
			Label: "hot_path", TablesTouched: hot, TablesTotal: n,
			PerBackendBytes: hotEst.Bytes, LowBytes: hotEst.LowBytes, HighBytes: hotEst.HighBytes,
			ProjectedAtMaxConnBytes: hotEst.Bytes * mc,
			Note: hotNote,
		},
		WorstCase: ScenarioEstimate{
			Label: "worst_case", TablesTouched: n, TablesTotal: n,
			PerBackendBytes: worst.Bytes, LowBytes: worst.LowBytes, HighBytes: worst.HighBytes,
			ProjectedAtMaxConnBytes: worst.Bytes * mc,
			Note: "reached by any backend running rebalancer, metadata sync, citus_add_node, pg_dump, or querying citus_tables / citus_shards views",
		},
		Note: "CitusTableCacheEntry is populated lazily per relation. OLTP backends sit near 'typical'; reporting queries drift toward 'hot_path'; maintenance operations reach 'worst_case'.",
	}
}

func computeConcurrentCap(ctx context.Context, deps Dependencies, perBackendWorstBytes int64, currentWorst int, in MetadataCacheFootprintInput) *ConcurrentWorstCap {
	if perBackendWorstBytes <= 0 {
		return nil
	}
	budget := in.MemoryBudgetBytes
	source := "input override"
	if budget <= 0 {
		// Try /proc/meminfo via pg_read_file; assume 25% of MemTotal as budget.
		var memTotal int64
		_ = deps.Pool.QueryRow(ctx, `
SELECT CASE
  WHEN has_function_privilege('pg_read_file(text,bigint,bigint,boolean)', 'EXECUTE') THEN
    (SELECT CASE
       WHEN pg_read_file('/proc/meminfo', 0, 4096, true) ~ 'MemTotal:' THEN
         (regexp_match(pg_read_file('/proc/meminfo',0,4096,true),'MemTotal:\s+(\d+)'))[1]::bigint * 1024
       ELSE 0
     END)
  ELSE 0
END`).Scan(&memTotal)
		if memTotal > 0 {
			budget = memTotal / 4
			source = "25% of /proc/meminfo MemTotal"
		}
	}
	if budget <= 0 {
		return &ConcurrentWorstCap{
			PerBackendWorstBytes: perBackendWorstBytes,
			BudgetSource:         "unavailable",
			Note:                 "Supply memory_budget_bytes to compute the concurrent worst-case cap; /proc/meminfo was not readable (pg_read_file not granted).",
		}
	}
	maxConc := int(budget / perBackendWorstBytes)
	var maxConn int
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&maxConn)
	headroom := maxConc - maxConn
	return &ConcurrentWorstCap{
		MemoryBudgetBytes:    budget,
		BudgetSource:         source,
		PerBackendWorstBytes: perBackendWorstBytes,
		MaxConcurrent:        maxConc,
		CurrentWorstCount:    currentWorst,
		HeadroomConnections:  headroom,
		Note:                 "max_concurrent = budget / per_backend_worst; headroom = max_concurrent − max_connections. Negative headroom means the cluster can OOM on metadata cache alone if every client warms the full cache.",
	}
}

func simulatePartitionExplosion(shapes []memory.TableShape, worst memory.CitusEstimate, maxConn int, in MetadataCacheFootprintInput) *PartitionExplosion {
	g := in.PartitionGrowthFactor
	if g <= 1.0 {
		return nil
	}
	// Build a copy of shapes with partition children multiplied by g. Each
	// partition child currently lives as a separate TableShape (parent oid set).
	// We approximate "growth" by duplicating the per-parent child footprint:
	//   1) compute average bytes per existing partition child
	//   2) add (g-1) * n_children * avg_child_bytes to the worst-case.
	currentChildren := 0
	var childTotalBytes int64
	for _, s := range shapes {
		if s.ParentOid != 0 { // this shape is a partition child
			currentChildren++
			one := memory.EstimateCitusMetadata([]memory.TableShape{s})
			childTotalBytes += one.Bytes - memory.CitusMetadataBaseBytes
		}
	}
	if currentChildren == 0 {
		return nil
	}
	avgChild := childTotalBytes / int64(currentChildren)
	projectedChildren := int(float64(currentChildren) * g)
	deltaChildren := projectedChildren - currentChildren
	deltaBytes := int64(deltaChildren) * avgChild
	projectedBytes := worst.Bytes + deltaBytes
	deltaAtMax := deltaBytes * int64(maxConn)
	return &PartitionExplosion{
		GrowthFactor:        g,
		CurrentChildren:     currentChildren,
		ProjectedChildren:   projectedChildren,
		CurrentWorstBytes:   worst.Bytes,
		ProjectedWorstBytes: projectedBytes,
		DeltaPerBackend:     deltaBytes,
		DeltaAtMaxConn:      deltaAtMax,
		Note:                fmt.Sprintf("Simulated %.2fx partition growth adds ~%d bytes to worst-case per backend; with max_connections=%d that is ~%d extra bytes cluster-wide if every backend warms fully. Partition creation also generates SysCache invalidations that transiently double cache size during rebuild.", g, deltaBytes, maxConn, deltaAtMax),
	}
}

func computeQueryCorrelation(ctx context.Context, deps Dependencies, shapes []memory.TableShape, in MetadataCacheFootprintInput) *QueryCorrelation {
	if in.SkipQueryCorrelation {
		return nil
	}
	var has bool
	if err := deps.Pool.QueryRow(ctx,
		"SELECT to_regclass('public.pg_stat_statements') IS NOT NULL OR to_regclass('pg_catalog.pg_stat_statements') IS NOT NULL").Scan(&has); err != nil || !has {
		return &QueryCorrelation{Source: "pg_stat_statements", Note: "extension not installed; typical scenario falls back to heuristic default"}
	}
	rows, err := deps.Pool.Query(ctx, `
SELECT LEFT(COALESCE(query,''), 2000)
FROM pg_stat_statements
WHERE calls >= 1
ORDER BY total_exec_time DESC NULLS LAST
LIMIT 200`)
	if err != nil {
		return &QueryCorrelation{Source: "pg_stat_statements", Note: fmt.Sprintf("query failed: %v", err)}
	}
	defer rows.Close()
	// Build lowercase table-name list once.
	type tn struct{ lowerName, lowerQualified string }
	names := make([]tn, 0, len(shapes))
	for _, s := range shapes {
		names = append(names, tn{
			lowerName:      strings.ToLower(s.Name),
			lowerQualified: strings.ToLower(s.Schema + "." + s.Name),
		})
	}
	var counts []int
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			continue
		}
		ql := strings.ToLower(q)
		hit := 0
		for _, n := range names {
			if strings.Contains(ql, n.lowerQualified) || wordContains(ql, n.lowerName) {
				hit++
			}
		}
		if hit > 0 {
			counts = append(counts, hit)
		}
	}
	if len(counts) == 0 {
		return &QueryCorrelation{Source: "pg_stat_statements", Note: "no statements referenced known distributed tables"}
	}
	// sort ascending for percentiles
	for i := 1; i < len(counts); i++ {
		for j := i; j > 0 && counts[j] < counts[j-1]; j-- {
			counts[j], counts[j-1] = counts[j-1], counts[j]
		}
	}
	sum := 0
	maxv := 0
	for _, c := range counts {
		sum += c
		if c > maxv {
			maxv = c
		}
	}
	median := counts[len(counts)/2]
	p95 := counts[(len(counts)*95)/100]
	if p95 < median {
		p95 = median
	}
	return &QueryCorrelation{
		Source:                   "pg_stat_statements",
		StatementsAnalyzed:       len(counts),
		AvgTablesPerStatement:    float64(sum) / float64(len(counts)),
		MedianTablesPerStatement: median,
		P95TablesPerStatement:    p95,
		MaxTablesPerStatement:    maxv,
		Note:                     "counts distinct distributed-table name occurrences in the statement text (substring match); approximate.",
	}
}

// wordContains checks if name appears as a whole identifier in q (lowercased).
// Substring match against a name like "events" would false-match "eventstore",
// so we require a non-identifier boundary on both sides.
func wordContains(q, name string) bool {
	if name == "" {
		return false
	}
	idx := 0
	for {
		k := strings.Index(q[idx:], name)
		if k < 0 {
			return false
		}
		start := idx + k
		end := start + len(name)
		leftOK := start == 0 || !isIdentChar(q[start-1])
		rightOK := end == len(q) || !isIdentChar(q[end])
		if leftOK && rightOK {
			return true
		}
		idx = end
	}
}

func isIdentChar(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}
