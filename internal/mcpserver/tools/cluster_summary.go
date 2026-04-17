// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_cluster_summary tool for cluster overview and health.

package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"citus-mcp/internal/citus/guc"
	"citus-mcp/internal/db"
	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

// ClusterSummaryInput defines input for citus_cluster_summary.
type ClusterSummaryInput struct {
	IncludeWorkers     bool `json:"include_workers,omitempty"`
	IncludeGUCs        bool `json:"include_gucs,omitempty"`
	IncludeConfig      bool `json:"include_config,omitempty"`
	// IncludeHealth adds a compact roll-up of memory pressure, effective
	// connection capacity, extension drift and metadata sync status by
	// delegating to the respective tools. Costs ~3 coordinator queries +
	// a handful of fanouts.
	IncludeHealth bool `json:"include_health,omitempty"`
	// IncludeOperational additionally runs the proactive-health probe
	// (long transactions, stuck 2PC, bloat candidates). Requires
	// IncludeHealth=true. More expensive; off by default.
	IncludeOperational bool `json:"include_operational,omitempty"`
	// All turns on every optional section (workers, GUCs, config, health,
	// operational) — the "give me everything" switch. Overrides the
	// per-section flags.
	All bool `json:"all,omitempty"`
}

// ClusterSummaryOutput defines output structure.
type ClusterSummaryOutput struct {
	Coordinator   CoordinatorSummary     `json:"coordinator"`
	Workers       []WorkerSummary        `json:"workers,omitempty"`
	Counts        CountsSummary          `json:"counts"`
	Warnings      []string               `json:"warnings,omitempty"`
	GUCs          map[string]string      `json:"gucs,omitempty"`
	Configuration *ConfigurationReport   `json:"configuration,omitempty"`
	Health        *ClusterHealthSummary  `json:"health,omitempty"`
}

// ClusterHealthSummary rolls up the outputs of the dedicated diagnostics
// tools into a glanceable status. Each sub-section is populated from the
// existing tool; fields are omitted when the corresponding probe fails so
// the summary stays useful even on partial failures.
type ClusterHealthSummary struct {
	OverallHealth string                      `json:"overall_health"` // ok | warning | critical | unknown
	Memory        *HealthMemorySection        `json:"memory,omitempty"`
	MetadataCache *HealthMetadataCacheSection `json:"metadata_cache,omitempty"`
	Connections   *HealthConnectionsSection   `json:"connections,omitempty"`
	Drift         *HealthDriftSection         `json:"drift,omitempty"`
	MetadataSync  *HealthMetadataSyncSection  `json:"metadata_sync,omitempty"`
	Operational   *HealthOperationalSection   `json:"operational,omitempty"`
	Errors        []string                    `json:"errors,omitempty"`
}

type HealthMemorySection struct {
	HottestNode   string  `json:"hottest_node,omitempty"`
	HighestRiskPct float64 `json:"highest_risk_pct,omitempty"`
	OpenAlarms    int     `json:"open_alarms"`
	Status        string  `json:"status"` // ok | warning | critical | unknown
}

// HealthMetadataCacheSection surfaces the three-regime Citus MetadataCache
// view (typical / hot_path / worst_case) plus the concurrent worst-case cap.
// Populated by MetadataCacheFootprintTool.
type HealthMetadataCacheSection struct {
	TypicalBytes           int64  `json:"typical_per_backend_bytes,omitempty"`
	HotPathBytes           int64  `json:"hot_path_per_backend_bytes,omitempty"`
	WorstCaseBytes         int64  `json:"worst_case_per_backend_bytes"`
	ProjectedAtMaxConnBytes int64 `json:"worst_case_at_max_connections_bytes"`
	MaxConcurrent          int    `json:"max_concurrent_worst_case,omitempty"`
	HeadroomConnections    int    `json:"headroom_connections,omitempty"`
	CurrentWorstBackends   int    `json:"current_worst_case_backends"`
	BudgetSource           string `json:"budget_source,omitempty"`
	OpenAlarms             int    `json:"open_alarms"`
	Status                 string `json:"status"`
}

type HealthConnectionsSection struct {
	CoordinatorOnlyMax int    `json:"coordinator_only_client_max"`
	MXMax              int    `json:"mx_client_max"`
	PgBouncerTxnMax    int    `json:"pgbouncer_txn_client_max"`
	Bottleneck         string `json:"bottleneck,omitempty"`
	OpenAlarms         int    `json:"open_alarms"`
	Status             string `json:"status"`
}

type HealthDriftSection struct {
	ExtensionIssues int    `json:"extension_issues"`
	OpenAlarms      int    `json:"open_alarms"`
	Status          string `json:"status"`
}

type HealthMetadataSyncSection struct {
	Verdict       string `json:"verdict"` // ok | nontransactional_recommended | blocked
	EstimatedDDL  int    `json:"estimated_ddl_commands"`
	EstimatedSec  int    `json:"estimated_duration_sec"`
	OpenAlarms    int    `json:"open_alarms"`
	Status        string `json:"status"`
}

type HealthOperationalSection struct {
	LongTx                int    `json:"long_tx_count"`
	IdleInTx              int    `json:"idle_in_tx_count"`
	StuckPreparedXact     int    `json:"stuck_prepared_xact_count"`
	UnhealthyPlacements   int    `json:"unhealthy_placement_count"`
	SaturatedNodes        int    `json:"saturated_node_count"`
	BloatCandidates       int    `json:"bloat_candidate_count"`
	StaleAutovacuum       int    `json:"stale_autovacuum_count"`
	OverallHealth         string `json:"overall_health"`
}

// ConfigurationReport provides a summary of important configuration settings.
type ConfigurationReport struct {
	Summary          string                 `json:"summary"`
	OverallHealth    string                 `json:"overall_health"`
	CriticalIssues   int                    `json:"critical_issues"`
	Warnings         int                    `json:"warnings"`
	CitusSettings    map[string]ConfigValue `json:"citus_settings"`
	PostgresSettings map[string]ConfigValue `json:"postgres_settings"`
	Recommendations  []string               `json:"recommendations,omitempty"`
}

// ConfigValue represents a configuration parameter value with context.
type ConfigValue struct {
	Value     string `json:"value"`
	Unit      string `json:"unit,omitempty"`
	IsDefault bool   `json:"is_default"`
	Status    string `json:"status,omitempty"` // ok, warning, critical
}

type CoordinatorSummary struct {
	Host            string `json:"host"`
	Port            int32  `json:"port"`
	PostgresVersion string `json:"postgres_version"`
	CitusVersion    string `json:"citus_version"`
}

type WorkerSummary struct {
	Host             string `json:"host"`
	Port             int32  `json:"port"`
	IsActive         bool   `json:"is_active"`
	ShouldHaveShards bool   `json:"should_have_shards"`
}

type CountsSummary struct {
	DistributedTables int `json:"distributed_tables"`
	ReferenceTables   int `json:"reference_tables"`
	ShardsTotal       int `json:"shards_total"`
	PlacementsTotal   int `json:"placements_total"`
}

func clusterSummaryTool(ctx context.Context, deps Dependencies, input ClusterSummaryInput) (*mcp.CallToolResult, ClusterSummaryOutput, error) {
	// "all" expands to every optional section.
	if input.All {
		input.IncludeWorkers = true
		input.IncludeGUCs = true
		input.IncludeConfig = true
		input.IncludeHealth = true
		input.IncludeOperational = true
	}
	// defaults - include workers and config by default
	if !input.IncludeWorkers {
		input.IncludeWorkers = true
	}
	// Include config by default for comprehensive summary
	input.IncludeConfig = true

	// read-only guard for potential SQLs
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ClusterSummaryOutput{}, nil
	}

	// validate citus extension
	ext, err := db.GetExtensionInfo(ctx, deps.Pool)
	if err != nil || ext == nil {
		return callError(serr.CodeNotCitus, "citus extension not found", "enable citus extension"), ClusterSummaryOutput{}, nil
	}

	serverInfo, err := db.GetServerInfo(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "server info"), ClusterSummaryOutput{}, nil
	}

	coordHost := deps.Pool.Config().ConnConfig.Host
	coordPort := int32(deps.Pool.Config().ConnConfig.Port)

	counts, err := fetchCounts(ctx, deps)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "counts"), ClusterSummaryOutput{}, nil
	}

	out := ClusterSummaryOutput{
		Coordinator: CoordinatorSummary{Host: coordHost, Port: coordPort, PostgresVersion: serverInfo.PostgresVersion, CitusVersion: serverInfo.CitusVersion},
		Counts:      counts,
	}

	warnings := []string{}

	if input.IncludeWorkers {
		workers, wWarnings := fetchWorkers(ctx, deps)
		out.Workers = workers
		warnings = append(warnings, wWarnings...)
	}

	if input.IncludeGUCs {
		gucs, err := fetchGUCs(ctx, deps)
		if err == nil {
			out.GUCs = gucs
		}
	}

	// Include configuration report
	if input.IncludeConfig {
		configReport, configWarnings := fetchConfigurationReport(ctx, deps, len(out.Workers))
		out.Configuration = configReport
		warnings = append(warnings, configWarnings...)
	}

	// Include cross-tool health roll-up.
	if input.IncludeHealth {
		out.Health = buildClusterHealth(ctx, deps, input.IncludeOperational)
	}

	if len(warnings) > 0 {
		out.Warnings = warnings
	}
	return nil, out, nil
}

// ClusterSummary is exported for reuse by resources.
func ClusterSummary(ctx context.Context, deps Dependencies, input ClusterSummaryInput) (*mcp.CallToolResult, ClusterSummaryOutput, error) {
	return clusterSummaryTool(ctx, deps, input)
}

func fetchCounts(ctx context.Context, deps Dependencies) (CountsSummary, error) {
	var counts CountsSummary
	// distributed tables
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_partition WHERE partmethod <> 'n'").Scan(&counts.DistributedTables); err != nil {
		return counts, err
	}
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_partition WHERE partmethod = 'n'").Scan(&counts.ReferenceTables); err != nil {
		return counts, err
	}
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_shard").Scan(&counts.ShardsTotal); err != nil {
		return counts, err
	}
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_shard_placement").Scan(&counts.PlacementsTotal); err != nil {
		return counts, err
	}
	return counts, nil
}

func fetchWorkers(ctx context.Context, deps Dependencies) ([]WorkerSummary, []string) {
	infos, err := deps.WorkerManager.Topology(ctx)
	if err != nil {
		deps.Logger.Warn("topology fetch failed", zap.Error(err))
		return nil, []string{"failed to fetch worker topology"}
	}
	workers := make([]WorkerSummary, 0, len(infos))
	warnings := []string{}
	if len(infos) == 0 {
		warnings = append(warnings, "no workers")
	}
	for _, info := range infos {
		if !info.IsActive {
			warnings = append(warnings, "inactive worker: "+info.NodeName)
		}
		workers = append(workers, WorkerSummary{Host: info.NodeName, Port: info.NodePort, IsActive: info.IsActive, ShouldHaveShards: info.ShouldHaveShards})
	}
	sort.Slice(workers, func(i, j int) bool {
		if workers[i].Host == workers[j].Host {
			return workers[i].Port < workers[j].Port
		}
		return workers[i].Host < workers[j].Host
	})

	// mixed worker versions (best-effort) - use Fanout
	if deps.Fanout != nil {
		versionSQL := `SELECT version() AS version, (SELECT extversion FROM pg_extension WHERE extname='citus') AS citus_version`
		if results, err := deps.Fanout.OnWorkers(ctx, versionSQL); err == nil {
			versions := map[string]struct{}{}
			for _, res := range results {
				if !res.Success || len(res.Rows) == 0 {
					continue
				}
				pgVer := ""
				citusVer := ""
				if v, ok := res.Rows[0]["version"].(string); ok {
					pgVer = v
				}
				if v, ok := res.Rows[0]["citus_version"].(string); ok {
					citusVer = v
				}
				versions[pgVer+"|"+citusVer] = struct{}{}
			}
			if len(versions) > 1 {
				warnings = append(warnings, "mixed worker versions")
			}
		}
	}

	return workers, warnings
}

func workerInfoByID(infos []db.WorkerInfo, id int32) (db.WorkerInfo, bool) {
	for _, w := range infos {
		if w.NodeID == id {
			return w, true
		}
	}
	return db.WorkerInfo{}, false
}

func fetchGUCs(ctx context.Context, deps Dependencies) (map[string]string, error) {
	rows, err := deps.Pool.Query(ctx, "SELECT name, setting FROM pg_settings WHERE name IN ('citus.shard_count','max_connections')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	gucs := map[string]string{}
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return nil, err
		}
		gucs[name] = setting
	}
	return gucs, rows.Err()
}

// fetchConfigurationReport builds a comprehensive configuration report.
func fetchConfigurationReport(ctx context.Context, deps Dependencies, workerCount int) (*ConfigurationReport, []string) {
	warnings := []string{}

	// Fetch Citus GUCs
	citusGUCs, err := guc.FetchCitusGUCs(ctx, deps.Pool)
	if err != nil {
		warnings = append(warnings, "failed to fetch Citus GUCs")
		return nil, warnings
	}

	// Fetch PostgreSQL GUCs
	postgresGUCs, err := guc.FetchRelevantPostgresGUCs(ctx, deps.Pool)
	if err != nil {
		warnings = append(warnings, "failed to fetch PostgreSQL GUCs")
		return nil, warnings
	}

	// Key Citus settings to include
	keyCitusGUCs := []string{
		"citus.shard_count",
		"citus.shard_replication_factor",
		"citus.max_adaptive_executor_pool_size",
		"citus.multi_shard_modify_mode",
		"citus.enable_repartition_joins",
		"citus.node_connection_timeout",
		"citus.max_background_task_executors_per_node",
		"citus.use_secondary_nodes",
	}

	// Key PostgreSQL settings to include
	keyPostgresGUCs := []string{
		"max_connections",
		"shared_buffers",
		"work_mem",
		"maintenance_work_mem",
		"max_worker_processes",
		"wal_level",
		"max_wal_senders",
		"max_replication_slots",
		"shared_preload_libraries",
		"statement_timeout",
	}

	// Build analysis context for rules
	analysisCtx := &guc.AnalysisContext{
		CitusGUCs:     citusGUCs,
		PostgresGUCs:  postgresGUCs,
		WorkerCount:   workerCount,
		IsCoordinator: true,
	}

	// Run analysis rules
	findings := guc.EvaluateAllRules(analysisCtx)

	// Count issues
	criticalCount := 0
	warningCount := 0
	recommendations := []string{}

	for _, f := range findings {
		switch f.Severity {
		case guc.SeverityCritical:
			criticalCount++
			recommendations = append(recommendations, fmt.Sprintf("[CRITICAL] %s: %s", f.Title, f.Recommendation))
		case guc.SeverityWarning:
			warningCount++
			if len(recommendations) < 5 {
				recommendations = append(recommendations, fmt.Sprintf("[WARNING] %s: %s", f.Title, f.Recommendation))
			}
		}
	}

	// Build Citus settings map
	citusSettings := make(map[string]ConfigValue)
	for _, name := range keyCitusGUCs {
		if g, ok := citusGUCs[name]; ok {
			unit := ""
			if g.Unit != nil {
				unit = *g.Unit
			}
			status := "ok"
			// Check if there's a finding for this GUC
			for _, f := range findings {
				for _, affected := range f.AffectedGUCs {
					if affected == name {
						status = string(f.Severity)
						break
					}
				}
			}
			citusSettings[name] = ConfigValue{
				Value:     g.Setting,
				Unit:      unit,
				IsDefault: g.Source == "default",
				Status:    status,
			}
		}
	}

	// Build PostgreSQL settings map
	postgresSettings := make(map[string]ConfigValue)
	for _, name := range keyPostgresGUCs {
		if g, ok := postgresGUCs[name]; ok {
			unit := ""
			if g.Unit != nil {
				unit = *g.Unit
			}
			// Format memory values nicely
			value := g.Setting
			if unit == "8kB" || unit == "kB" {
				if bytes, err := guc.ParseBytes(g.Setting, unit); err == nil {
					value = guc.FormatBytes(bytes)
					unit = ""
				}
			}
			status := "ok"
			// Check if there's a finding for this GUC
			for _, f := range findings {
				for _, affected := range f.AffectedGUCs {
					if affected == name {
						if f.Severity == guc.SeverityCritical || f.Severity == guc.SeverityWarning {
							status = string(f.Severity)
						}
						break
					}
				}
			}
			postgresSettings[name] = ConfigValue{
				Value:     value,
				Unit:      unit,
				IsDefault: g.Source == "default",
				Status:    status,
			}
		}
	}

	// Determine overall health
	overallHealth := "healthy"
	if criticalCount > 0 {
		overallHealth = "critical"
	} else if warningCount > 0 {
		overallHealth = "needs_attention"
	}

	// Build summary paragraph
	summary := buildConfigSummary(citusGUCs, postgresGUCs, workerCount, criticalCount, warningCount)

	return &ConfigurationReport{
		Summary:          summary,
		OverallHealth:    overallHealth,
		CriticalIssues:   criticalCount,
		Warnings:         warningCount,
		CitusSettings:    citusSettings,
		PostgresSettings: postgresSettings,
		Recommendations:  recommendations,
	}, warnings
}

// buildConfigSummary creates a human-readable summary paragraph about the cluster configuration.
func buildConfigSummary(citusGUCs, postgresGUCs map[string]guc.GUCValue, workerCount, criticalCount, warningCount int) string {
	var parts []string

	// Shard configuration
	if g, ok := citusGUCs["citus.shard_count"]; ok {
		shardCount := g.Setting
		shardsPerWorker := "N/A"
		if workerCount > 0 {
			if sc, err := guc.ParseInt(shardCount); err == nil {
				shardsPerWorker = fmt.Sprintf("%d", sc/int64(workerCount))
			}
		}
		parts = append(parts, fmt.Sprintf("The cluster is configured with %s shards per distributed table (%s shards per worker)", shardCount, shardsPerWorker))
	}

	// Replication factor
	if g, ok := citusGUCs["citus.shard_replication_factor"]; ok {
		if g.Setting == "1" {
			parts = append(parts, "using single-copy replication (recommended for PostgreSQL HA setups)")
		} else {
			parts = append(parts, fmt.Sprintf("using %s-way shard replication for high availability", g.Setting))
		}
	}

	// Execution mode
	if g, ok := citusGUCs["citus.multi_shard_modify_mode"]; ok {
		parts = append(parts, fmt.Sprintf("Multi-shard modifications run in %s mode", g.Setting))
	}

	// WAL level check
	if g, ok := postgresGUCs["wal_level"]; ok {
		if strings.ToLower(g.Setting) == "logical" {
			parts = append(parts, "WAL level is correctly set to 'logical' enabling shard moves and rebalancing")
		} else {
			parts = append(parts, fmt.Sprintf("WARNING: wal_level='%s' - shard moves and rebalancing will not work", g.Setting))
		}
	}

	// Memory configuration
	if g, ok := postgresGUCs["shared_buffers"]; ok {
		if g.Unit != nil {
			if bytes, err := guc.ParseBytes(g.Setting, *g.Unit); err == nil {
				parts = append(parts, fmt.Sprintf("Memory: shared_buffers=%s", guc.FormatBytes(bytes)))
			}
		}
	}

	// Connection capacity
	if g, ok := postgresGUCs["max_connections"]; ok {
		parts = append(parts, fmt.Sprintf("max_connections=%s", g.Setting))
	}

	// Health status
	if criticalCount > 0 {
		parts = append(parts, fmt.Sprintf("ATTENTION: %d critical configuration issues detected that should be addressed immediately", criticalCount))
	} else if warningCount > 0 {
		parts = append(parts, fmt.Sprintf("Note: %d configuration warnings detected - consider reviewing recommendations", warningCount))
	} else {
		parts = append(parts, "Configuration appears healthy with no critical issues detected")
	}

	return strings.Join(parts, ". ") + "."
}

// buildClusterHealth delegates to the dedicated diagnostic tools and
// collapses their outputs into the compact ClusterHealthSummary. Errors
// from individual probes are recorded in Health.Errors so the rest of the
// summary still renders.
func buildClusterHealth(ctx context.Context, deps Dependencies, includeOperational bool) *ClusterHealthSummary {
h := &ClusterHealthSummary{OverallHealth: "ok", Errors: []string{}}

// Memory risk roll-up.
if _, memOut, err := MemoryRiskReportTool(ctx, deps, MemoryRiskInput{}); err != nil {
h.Errors = append(h.Errors, "memory_risk: "+err.Error())
} else {
sec := &HealthMemorySection{Status: "ok"}
for _, n := range memOut.Nodes {
if n.RiskPct > sec.HighestRiskPct {
sec.HighestRiskPct = n.RiskPct
sec.HottestNode = fmt.Sprintf("%s:%d", n.NodeName, n.NodePort)
}
}
sec.OpenAlarms = len(memOut.Alarms)
if sec.HighestRiskPct >= 0.95 || hasCritical(memOut.Alarms) {
sec.Status = "critical"
} else if sec.HighestRiskPct >= 0.80 || len(memOut.Alarms) > 0 {
sec.Status = "warning"
}
if sec.HighestRiskPct == 0 && len(memOut.Nodes) == 0 {
sec.Status = "unknown"
}
h.Memory = sec
}

// Metadata cache footprint roll-up: surfaces the three scenarios + concurrent cap.
if _, mcOut, err := MetadataCacheFootprintTool(ctx, deps, MetadataCacheFootprintInput{}); err != nil {
h.Errors = append(h.Errors, "metadata_cache_footprint: "+err.Error())
} else {
sec := &HealthMetadataCacheSection{
WorstCaseBytes:          mcOut.Estimate.Bytes,
ProjectedAtMaxConnBytes: mcOut.ProjectedCluster.MaxConnectionBytes,
CurrentWorstBackends:    len(mcOut.WorstCaseBackends),
OpenAlarms:              len(mcOut.Alarms),
Status:                  "ok",
}
if mcOut.Scenarios != nil {
sec.TypicalBytes = mcOut.Scenarios.Typical.PerBackendBytes
sec.HotPathBytes = mcOut.Scenarios.HotPath.PerBackendBytes
}
if mcOut.ConcurrentCap != nil {
sec.MaxConcurrent = mcOut.ConcurrentCap.MaxConcurrent
sec.HeadroomConnections = mcOut.ConcurrentCap.HeadroomConnections
sec.BudgetSource = mcOut.ConcurrentCap.BudgetSource
}
switch {
case hasCritical(mcOut.Alarms):
sec.Status = "critical"
case mcOut.ConcurrentCap != nil && mcOut.ConcurrentCap.HeadroomConnections < 0 && mcOut.ConcurrentCap.BudgetSource != "unavailable":
sec.Status = "critical"
case len(mcOut.WorstCaseBackends) > 0:
sec.Status = "warning"
case len(mcOut.Alarms) > 0:
sec.Status = "warning"
}
h.MetadataCache = sec
}

// Connection capacity roll-up.
if _, capOut, err := ConnectionCapacityTool(ctx, deps, ConnectionCapacityInput{}); err != nil {
h.Errors = append(h.Errors, "connection_capacity: "+err.Error())
} else {
sec := &HealthConnectionsSection{
CoordinatorOnlyMax: capOut.CoordinatorOnly.RecommendedClientMax,
MXMax:              capOut.MXMode.RecommendedClientMax,
PgBouncerTxnMax:    capOut.PgBouncerTxn.RecommendedClientMax,
Bottleneck:         capOut.CoordinatorOnly.Bottleneck,
OpenAlarms:         len(capOut.Alarms),
Status:             "ok",
}
if hasCritical(capOut.Alarms) {
sec.Status = "critical"
} else if len(capOut.Alarms) > 0 {
sec.Status = "warning"
}
h.Connections = sec
}

// Extension / version drift.
if _, drOut, err := ExtensionDriftScannerTool(ctx, deps, ExtensionDriftInput{}); err != nil {
h.Errors = append(h.Errors, "extension_drift: "+err.Error())
} else {
drifted := 0
for _, rep := range drOut.Reports {
if rep.Drifted {
drifted++
}
}
sec := &HealthDriftSection{
ExtensionIssues: drifted,
OpenAlarms:      len(drOut.Alarms),
Status:          "ok",
}
if hasCritical(drOut.Alarms) {
sec.Status = "critical"
} else if len(drOut.Alarms) > 0 || drifted > 0 {
sec.Status = "warning"
}
h.Drift = sec
}

// Metadata sync readiness (advisory — always safe to run).
if _, msOut, err := MetadataSyncRiskTool(ctx, deps, MetadataSyncRiskInput{}); err != nil {
h.Errors = append(h.Errors, "metadata_sync_risk: "+err.Error())
} else {
sec := &HealthMetadataSyncSection{
Verdict:      msOut.Verdict,
EstimatedDDL: msOut.EstimatedDDLCommands,
EstimatedSec: msOut.EstimatedDurationSec,
OpenAlarms:   len(msOut.Alarms),
Status:       "ok",
}
switch msOut.Verdict {
case "blocked":
sec.Status = "critical"
case "nontransactional_recommended":
sec.Status = "warning"
}
h.MetadataSync = sec
}

// Operational dashboard (opt-in).
if includeOperational {
if _, opOut, err := ProactiveHealthTool(ctx, deps, ProactiveHealthInput{}); err != nil {
h.Errors = append(h.Errors, "proactive_health: "+err.Error())
} else {
h.Operational = &HealthOperationalSection{
LongTx:              opOut.Summary.LongTxCount,
IdleInTx:            opOut.Summary.IdleInTxCount,
StuckPreparedXact:   opOut.Summary.StuckPreparedXactCount,
UnhealthyPlacements: opOut.Summary.UnhealthyPlacementCount,
SaturatedNodes:      opOut.Summary.SaturatedNodeCount,
BloatCandidates:     opOut.Summary.BloatCandidateCount,
StaleAutovacuum:     opOut.Summary.StaleAutovacuumCount,
OverallHealth:       opOut.Summary.OverallHealth,
}
}
}

h.OverallHealth = rollupHealth(h)
return h
}

func hasCritical(alarms []diagnostics.Alarm) bool {
for _, a := range alarms {
if a.Severity == diagnostics.SeverityCritical {
return true
}
}
return false
}

func rollupHealth(h *ClusterHealthSummary) string {
worst := "ok"
check := func(s string) {
switch s {
case "critical":
worst = "critical"
case "warning":
if worst != "critical" {
worst = "warning"
}
case "unknown":
if worst == "ok" {
worst = "unknown"
}
}
}
if h.Memory != nil {
check(h.Memory.Status)
}
if h.MetadataCache != nil {
check(h.MetadataCache.Status)
}
if h.Connections != nil {
check(h.Connections.Status)
}
if h.Drift != nil {
check(h.Drift.Status)
}
if h.MetadataSync != nil {
check(h.MetadataSync.Status)
}
if h.Operational != nil {
check(h.Operational.OverallHealth)
}
return worst
}
