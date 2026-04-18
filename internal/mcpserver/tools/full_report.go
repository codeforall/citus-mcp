// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// citus_full_report: one-shot comprehensive diagnostic report that fans out
// to every read-only diagnostic/advisor tool, collects their output, and
// rolls everything up into a single structured response. Execute-class
// tools (rebalance/move/isolate/cleanup execute, snapshot record, approval
// token) are intentionally excluded; tools that require user-supplied SQL
// or admin DSNs (explain, planner probe, pgbouncer inspector) are only run
// when the caller supplies the corresponding input.
//
// The report is designed so that a single call gives an operator a
// complete, defensible picture of cluster health, capacity, memory risk,
// configuration drift, and workload pathologies — suitable for the
// "I need the full picture before making a change" workflow.

package tools

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	advisorpkg "citus-mcp/internal/citus/advisor"
	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// FullReportInput controls which optional sections the covering tool runs.
// All sections are on by default except the ones that require caller-supplied
// inputs (synthetic probe, pooler admin DSN, partition growth simulator).
type FullReportInput struct {
	// MemoryBudgetBytes forwarded to citus_metadata_cache_footprint to compute
	// the concurrent-worst-case cap. If 0, the tool auto-derives from /proc/meminfo.
	MemoryBudgetBytes int64 `json:"memory_budget_bytes,omitempty"`
	// PartitionGrowthFactor forwarded to citus_metadata_cache_footprint to
	// simulate partition growth. <=1 disables the simulation in the report.
	PartitionGrowthFactor float64 `json:"partition_growth_factor,omitempty"`

	// IncludeSyntheticProbe runs the bounded microbenchmark (p50/p95 latency).
	// Off by default because it sends real queries.
	IncludeSyntheticProbe bool `json:"include_synthetic_probe,omitempty"`

	// PgBouncerAdminDSN, when set, also runs the pgbouncer inspector.
	PgBouncerAdminDSN string `json:"pgbouncer_admin_dsn,omitempty"`

	// TargetDataSizeBytes / TargetConcurrentClients / WorkloadMode / DeploymentMode
	// enable the hardware sizer recommendation inside the report. All four must
	// be supplied; otherwise the sizer section is skipped.
	TargetDataSizeBytes     int64  `json:"target_data_size_bytes,omitempty"`
	TargetConcurrentClients int    `json:"target_concurrent_clients,omitempty"`
	WorkloadMode            string `json:"workload_mode,omitempty"`
	DeploymentMode          string `json:"deployment_mode,omitempty"`
	MaxRAMPerNodeGiB        int    `json:"max_ram_per_node_gib,omitempty"`

	// SkipSections is a list of section short-names to suppress (e.g.
	// "locks", "activity"). See ToolsRun in the response for valid names.
	SkipSections []string `json:"skip_sections,omitempty"`

	// IncludeVerboseOutputs, when false (default), drops large fields from the
	// raw per-tool outputs to keep the response compact. Set true to include
	// every per-tool output verbatim.
	IncludeVerboseOutputs bool `json:"include_verbose_outputs,omitempty"`
}

// SkipReason is the structured "this tool produced no actionable output
// on this cluster" contract. Every reason is one of a small set of
// well-known codes so downstream consumers can distinguish
// "user_requested" (the operator passed skip_sections) from
// "missing_dependency" (e.g. pg_stat_statements not installed) from
// "not_citus" (coordinator has no citus extension loaded). The
// FullReport aggregator treats tools_skipped as the single source of
// truth: a section that returns a skipReason appears here and is NOT
// added to tools_run.
type SkipReason struct {
	Tool   string `json:"tool"`
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

// skipError lets a section signal "I ran but produced no actionable
// output, for this well-known reason". The aggregator unwraps this via
// errors.As and records it in ToolsSkipped. Section functions that
// can legitimately produce no output (e.g. pg_stat_statements-dependent
// tools on clusters without that extension) return skipSection(...).
type skipError struct {
	Reason string
	Detail string
}

func (e *skipError) Error() string { return e.Reason }

// skipSection is the helper a tool returns when it would otherwise
// produce an empty output. See SkipReason.
func skipSection(reason, detail string) error {
	return &skipError{Reason: reason, Detail: detail}
}
type FullReportOutput struct {
	GeneratedAt   string            `json:"generated_at"`
	DurationMs    int64             `json:"duration_ms"`
	ToolsRun      []string          `json:"tools_run"`
	ToolsSkipped  []SkipReason      `json:"tools_skipped,omitempty"`
	SectionErrors map[string]string `json:"section_errors,omitempty"`

	// High-level rollup
	OverallHealth   string   `json:"overall_health"`
	TopFindings     []string `json:"top_findings"`
	Recommendations []string `json:"recommendations,omitempty"`

	// Per-section outputs
	Summary       *ClusterSummaryOutput          `json:"summary,omitempty"`
	Health        *ProactiveHealthOutput         `json:"health,omitempty"`
	MetadataHealth *MetadataHealthOutput         `json:"metadata_health,omitempty"`
	Configuration *FullReportConfigSection       `json:"configuration,omitempty"`
	Memory        *FullReportMemorySection       `json:"memory,omitempty"`
	Capacity      *FullReportCapacitySection     `json:"capacity,omitempty"`
	Workload      *FullReportWorkloadSection     `json:"workload,omitempty"`
	Shards        *FullReportShardsSection       `json:"shards,omitempty"`
	Advisors      *FullReportAdvisorsSection     `json:"advisors,omitempty"`
	Sizer         *HardwareSizerOutput           `json:"hardware_sizer,omitempty"`
	Alarms        *AlarmsListOutput              `json:"alarms,omitempty"`

	// Aggregated per-section runtime for observability / debugging.
	SectionDurationsMs map[string]int64 `json:"section_durations_ms,omitempty"`
}

type FullReportConfigSection struct {
	ConfigAdvisor     *ConfigAdvisorOutput     `json:"config_advisor,omitempty"`
	ConfigDeepInspect *ConfigDeepInspectOutput `json:"config_deep_inspect,omitempty"`
	ExtensionDrift    *ExtensionDriftOutput    `json:"extension_drift,omitempty"`
	SessionGuardrails *SessionGuardrailsOutput `json:"session_guardrails,omitempty"`
}

type FullReportMemorySection struct {
	MetadataCacheFootprint *MetadataCacheFootprintOutput `json:"metadata_cache_footprint,omitempty"`
	PgCacheFootprint       *PgCacheFootprintOutput       `json:"pg_cache_footprint,omitempty"`
	WorkerMemcontexts      *WorkerMemcontextsOutput      `json:"worker_memcontexts,omitempty"`
	MemoryRisk             *MemoryRiskOutput             `json:"memory_risk,omitempty"`
	MemoryRiskWorstCase    *MemoryRiskOutput             `json:"memory_risk_worst_case,omitempty"`
}

type FullReportCapacitySection struct {
	ConnectionCapacity     *ConnectionCapacityOutput `json:"connection_capacity,omitempty"`
	ConnectionFanoutSim    *FanoutSimOutput          `json:"connection_fanout_simulator,omitempty"`
	MXReadiness            *MxReadinessOutput        `json:"mx_readiness,omitempty"`
	MetadataSyncRisk       *MetadataSyncRiskOutput   `json:"metadata_sync_risk,omitempty"`
	PoolerAdvisor          *PoolerAdvisorOutput      `json:"pooler_advisor,omitempty"`
	PgBouncerInspector     *PgBouncerInspectorOutput `json:"pgbouncer_inspector,omitempty"`
}

type FullReportWorkloadSection struct {
	Activity       *ActivityOutput       `json:"activity,omitempty"`
	Locks          *LockInspectorOutput  `json:"locks,omitempty"`
	Jobs           *JobInspectorOutput   `json:"jobs,omitempty"`
	TenantRisk     *TenantRiskOutput     `json:"tenant_risk,omitempty"`
	QueryPathology *QueryPathologyOutput `json:"query_pathology,omitempty"`
	RoutingDrift   *RoutingDriftOutput   `json:"routing_drift,omitempty"`
	SyntheticProbe *SyntheticProbeOutput `json:"synthetic_probe,omitempty"`
	TwoPCRecovery  *TwoPCRecoveryOutput  `json:"two_pc_recovery,omitempty"`
}

type FullReportShardsSection struct {
	ShardSkew     *ShardSkewOutput           `json:"shard_skew,omitempty"`
	ShardHeatmap  *ShardHeatmapOutput        `json:"shard_heatmap,omitempty"`
	Colocation    *ColocationInspectorOutput `json:"colocation,omitempty"`
	RebalanceCost       *RebalanceCostOutput         `json:"rebalance_cost,omitempty"`
	RebalanceForensics  *RebalanceForensicsOutput    `json:"rebalance_forensics,omitempty"`
	PlacementIntegrity  *PlacementIntegrityOutput    `json:"placement_integrity,omitempty"`
}

type FullReportAdvisorsSection struct {
	CitusAdvisor     *advisorpkg.Output     `json:"citus_advisor,omitempty"`
	ShardAdvisor     *ShardAdvisorOutput    `json:"shard_advisor,omitempty"`
	ColumnarAdvisor  *ColumnarAdvisorOutput `json:"columnar_advisor,omitempty"`
}

// FullReportTool drives the covering report.
func FullReportTool(ctx context.Context, deps Dependencies, in FullReportInput) (*mcp.CallToolResult, FullReportOutput, error) {
	start := time.Now()
	out := FullReportOutput{
		GeneratedAt:        start.UTC().Format(time.RFC3339),
		ToolsRun:           []string{},
		ToolsSkipped:       []SkipReason{},
		SectionErrors:      map[string]string{},
		SectionDurationsMs: map[string]int64{},
		TopFindings:        []string{},
		Recommendations:    []string{},
	}
	skip := map[string]bool{}
	for _, s := range in.SkipSections {
		skip[s] = true
	}

	// run wraps a section call with timing, error capture, and skip-list honor.
	// Skip contract:
	//   * user pre-selected via SkipSections  -> ToolsSkipped[user_requested]
	//   * section returned skipError          -> ToolsSkipped[<reason>]
	//   * section returned real error         -> SectionErrors + ToolsRun
	//   * section returned nil                -> ToolsRun
	// tools_skipped is the SINGLE source of truth for "no output".
	run := func(name string, fn func() error) {
		if skip[name] {
			out.ToolsSkipped = append(out.ToolsSkipped, SkipReason{Tool: name, Reason: "user_requested"})
			return
		}
		t0 := time.Now()
		err := fn()
		out.SectionDurationsMs[name] = time.Since(t0).Milliseconds()
		var se *skipError
		if errors.As(err, &se) {
			out.ToolsSkipped = append(out.ToolsSkipped, SkipReason{Tool: name, Reason: se.Reason, Detail: se.Detail})
			return
		}
		if err != nil {
			out.SectionErrors[name] = err.Error()
		}
		out.ToolsRun = append(out.ToolsRun, name)
	}

	// --- Summary ---------------------------------------------------------
	run("cluster_summary", func() error {
		_, o, err := clusterSummaryTool(ctx, deps, ClusterSummaryInput{All: true})
		if err != nil {
			return err
		}
		// The tool can return an empty output when the coordinator has
		// no citus extension loaded (callError path). Surface that as a
		// structured skip so tools_skipped stays authoritative.
		if o.Coordinator.Host == "" && o.Coordinator.CitusVersion == "" {
			return skipSection("citus_extension_not_installed", "CREATE EXTENSION citus on coordinator")
		}
		out.Summary = &o
		return nil
	})

	// --- Operational health ---------------------------------------------
	run("proactive_health", func() error {
		_, o, err := ProactiveHealthTool(ctx, deps, ProactiveHealthInput{})
		if err != nil {
			return err
		}
		out.Health = &o
		return nil
	})
	run("metadata_health", func() error {
		_, o, err := metadataHealthTool(ctx, deps, MetadataHealthInput{})
		if err != nil {
			return err
		}
		out.MetadataHealth = &o
		return nil
	})

	// --- Configuration ---------------------------------------------------
	cfg := &FullReportConfigSection{}
	run("config_advisor", func() error {
		_, o, err := configAdvisorTool(ctx, deps, ConfigAdvisorInput{})
		if err != nil {
			return err
		}
		cfg.ConfigAdvisor = &o
		return nil
	})
	run("config_deep_inspect", func() error {
		_, o, err := ConfigDeepInspectTool(ctx, deps, ConfigDeepInspectInput{})
		if err != nil {
			return err
		}
		cfg.ConfigDeepInspect = &o
		return nil
	})
	run("extension_drift", func() error {
		_, o, err := ExtensionDriftScannerTool(ctx, deps, ExtensionDriftInput{})
		if err != nil {
			return err
		}
		cfg.ExtensionDrift = &o
		return nil
	})
	run("session_guardrails", func() error {
		_, o, err := SessionGuardrailsTool(ctx, deps, SessionGuardrailsInput{})
		if err != nil {
			return err
		}
		cfg.SessionGuardrails = &o
		return nil
	})
	if cfg.ConfigAdvisor != nil || cfg.ConfigDeepInspect != nil || cfg.ExtensionDrift != nil || cfg.SessionGuardrails != nil {
		out.Configuration = cfg
	}

	// --- Memory ----------------------------------------------------------
	mem := &FullReportMemorySection{}
	run("metadata_cache_footprint", func() error {
		_, o, err := MetadataCacheFootprintTool(ctx, deps, MetadataCacheFootprintInput{
			MemoryBudgetBytes:     in.MemoryBudgetBytes,
			PartitionGrowthFactor: in.PartitionGrowthFactor,
		})
		if err != nil {
			return err
		}
		mem.MetadataCacheFootprint = &o
		return nil
	})
	run("pg_cache_footprint", func() error {
		_, o, err := PgCacheFootprintTool(ctx, deps, PgCacheFootprintInput{})
		if err != nil {
			return err
		}
		mem.PgCacheFootprint = &o
		return nil
	})
	run("worker_memcontexts", func() error {
		_, o, err := WorkerMemcontextsTool(ctx, deps, WorkerMemcontextsInput{})
		if err != nil {
			return err
		}
		mem.WorkerMemcontexts = &o
		return nil
	})
	run("memory_risk", func() error {
		_, o, err := MemoryRiskReportTool(ctx, deps, MemoryRiskInput{})
		if err != nil {
			return err
		}
		mem.MemoryRisk = &o
		return nil
	})
	run("memory_risk_worst_case", func() error {
		_, o, err := MemoryRiskReportTool(ctx, deps, MemoryRiskInput{WorstCase: true})
		if err != nil {
			return err
		}
		mem.MemoryRiskWorstCase = &o
		return nil
	})
	if mem.MetadataCacheFootprint != nil || mem.PgCacheFootprint != nil || mem.WorkerMemcontexts != nil || mem.MemoryRisk != nil {
		out.Memory = mem
	}

	// --- Capacity --------------------------------------------------------
	cap := &FullReportCapacitySection{}
	run("connection_capacity", func() error {
		_, o, err := ConnectionCapacityTool(ctx, deps, ConnectionCapacityInput{})
		if err != nil {
			return err
		}
		cap.ConnectionCapacity = &o
		return nil
	})
	run("connection_fanout_simulator", func() error {
		// Supply a representative default workload so the sim runs inside the
		// covering report; callers wanting custom mixes can invoke the tool
		// directly.
		_, o, err := ConnectionFanoutSimulatorTool(ctx, deps, FanoutSimInput{
			Workloads: []FanoutWorkload{
				{Name: "router_oltp", QpsCoordinator: 500, AvgShardsTouched: 1, AvgQueryMs: 5},
				{Name: "fanout_analytical", QpsCoordinator: 20, AvgShardsTouched: 32, AvgQueryMs: 200},
			},
		})
		if err != nil {
			return err
		}
		cap.ConnectionFanoutSim = &o
		return nil
	})
	run("mx_readiness", func() error {
		_, o, err := MxReadinessTool(ctx, deps, MxReadinessInput{})
		if err != nil {
			return err
		}
		cap.MXReadiness = &o
		return nil
	})
	run("metadata_sync_risk", func() error {
		_, o, err := MetadataSyncRiskTool(ctx, deps, MetadataSyncRiskInput{})
		if err != nil {
			return err
		}
		cap.MetadataSyncRisk = &o
		return nil
	})
	run("pooler_advisor", func() error {
		_, o, err := PoolerAdvisorTool(ctx, deps, PoolerAdvisorInput{})
		if err != nil {
			return err
		}
		cap.PoolerAdvisor = &o
		return nil
	})
	if in.PgBouncerAdminDSN != "" {
		run("pgbouncer_inspector", func() error {
			_, o, err := PgBouncerInspectorTool(ctx, deps, PgBouncerInspectorInput{AdminDSN: in.PgBouncerAdminDSN})
			if err != nil {
				return err
			}
			cap.PgBouncerInspector = &o
			return nil
		})
	} else {
		out.ToolsSkipped = append(out.ToolsSkipped, SkipReason{Tool: "pgbouncer_inspector", Reason: "no_admin_dsn", Detail: "pass pgbouncer_admin_dsn input to enable"})
	}
	if cap.ConnectionCapacity != nil || cap.ConnectionFanoutSim != nil || cap.MXReadiness != nil ||
		cap.MetadataSyncRisk != nil || cap.PoolerAdvisor != nil || cap.PgBouncerInspector != nil {
		out.Capacity = cap
	}

	// --- Workload --------------------------------------------------------
	wl := &FullReportWorkloadSection{}
	run("activity", func() error {
		_, o, err := activityTool(ctx, deps, ActivityInput{})
		if err != nil {
			return err
		}
		wl.Activity = &o
		return nil
	})
	run("locks", func() error {
		_, o, err := citusLockInspectorTool(ctx, deps, LockInspectorInput{})
		if err != nil {
			return err
		}
		wl.Locks = &o
		return nil
	})
	run("jobs", func() error {
		_, o, err := jobInspectorTool(ctx, deps, JobInspectorInput{})
		if err != nil {
			return err
		}
		wl.Jobs = &o
		return nil
	})
	run("tenant_risk", func() error {
		_, o, err := TenantRiskTool(ctx, deps, TenantRiskInput{})
		if err != nil {
			return err
		}
		wl.TenantRisk = &o
		return nil
	})
	run("query_pathology", func() error {
		_, o, err := QueryPathologyTool(ctx, deps, QueryPathologyInput{})
		if err != nil {
			return err
		}
		wl.QueryPathology = &o
		return nil
	})
	run("routing_drift", func() error {
		_, o, err := RoutingDriftDetectorTool(ctx, deps, RoutingDriftInput{})
		if err != nil {
			return err
		}
		wl.RoutingDrift = &o
		return nil
	})
	run("two_pc_recovery_inspector", func() error {
		_, o, err := TwoPCRecoveryInspectorTool(ctx, deps, TwoPCRecoveryInput{SuppressRecoveryScript: !in.IncludeVerboseOutputs})
		if err != nil {
			return err
		}
		wl.TwoPCRecovery = &o
		return nil
	})
	if in.IncludeSyntheticProbe {
		run("synthetic_probe", func() error {
			_, o, err := SyntheticProbeTool(ctx, deps, SyntheticProbeInput{})
			if err != nil {
				return err
			}
			wl.SyntheticProbe = &o
			return nil
		})
	} else {
		out.ToolsSkipped = append(out.ToolsSkipped, SkipReason{Tool: "synthetic_probe", Reason: "opt_in", Detail: "set run_synthetic_probe=true to enable"})
	}
	if wl.Activity != nil || wl.Locks != nil || wl.Jobs != nil || wl.TenantRisk != nil ||
		wl.QueryPathology != nil || wl.RoutingDrift != nil || wl.SyntheticProbe != nil ||
		wl.TwoPCRecovery != nil {
		out.Workload = wl
	}

	// --- Shards ----------------------------------------------------------
	sh := &FullReportShardsSection{}
	run("shard_skew", func() error {
		_, o, err := shardSkewReportTool(ctx, deps, ShardSkewInput{})
		if err != nil {
			return err
		}
		sh.ShardSkew = &o
		return nil
	})
	run("shard_heatmap", func() error {
		_, o, err := shardHeatmapTool(ctx, deps, ShardHeatmapInput{})
		if err != nil {
			return err
		}
		sh.ShardHeatmap = &o
		return nil
	})
	run("colocation", func() error {
		_, o, err := colocationInspectorTool(ctx, deps, ColocationInspectorInput{})
		if err != nil {
			return err
		}
		sh.Colocation = &o
		return nil
	})
	run("rebalance_cost", func() error {
		_, o, err := RebalanceCostEstimatorTool(ctx, deps, RebalanceCostInput{})
		if err != nil {
			return err
		}
		sh.RebalanceCost = &o
		return nil
	})
	run("rebalance_forensics", func() error {
		_, o, err := RebalanceForensicsTool(ctx, deps, RebalanceForensicsInput{})
		if err != nil {
			return err
		}
		sh.RebalanceForensics = &o
		return nil
	})
	run("placement_integrity_check", func() error {
		_, o, err := PlacementIntegrityCheckTool(ctx, deps, PlacementIntegrityInput{})
		if err != nil {
			return err
		}
		sh.PlacementIntegrity = &o
		return nil
	})
	if sh.ShardSkew != nil || sh.ShardHeatmap != nil || sh.Colocation != nil || sh.RebalanceCost != nil || sh.RebalanceForensics != nil || sh.PlacementIntegrity != nil {
		out.Shards = sh
	}

	// --- Advisors --------------------------------------------------------
	ad := &FullReportAdvisorsSection{}
	run("citus_advisor", func() error {
		_, o, err := citusAdvisorTool(ctx, deps, CitusAdvisorInput{})
		if err != nil {
			return err
		}
		ad.CitusAdvisor = &o
		return nil
	})
	run("shard_advisor", func() error {
		_, o, err := ShardAdvisorTool(ctx, deps, ShardAdvisorInput{})
		if err != nil {
			return err
		}
		ad.ShardAdvisor = &o
		return nil
	})
	run("columnar_advisor", func() error {
		_, o, err := ColumnarAdvisorTool(ctx, deps, ColumnarAdvisorInput{})
		if err != nil {
			return err
		}
		ad.ColumnarAdvisor = &o
		return nil
	})
	if ad.CitusAdvisor != nil || ad.ShardAdvisor != nil || ad.ColumnarAdvisor != nil {
		out.Advisors = ad
	}

	// --- Hardware sizer (only when caller supplies a target) -------------
	if in.TargetDataSizeBytes > 0 && in.TargetConcurrentClients > 0 &&
		in.WorkloadMode != "" && in.DeploymentMode != "" {
		run("hardware_sizer", func() error {
			_, o, err := HardwareSizerTool(ctx, deps, HardwareSizerInput{
				TargetDataSizeBytes:     in.TargetDataSizeBytes,
				TargetConcurrentClients: in.TargetConcurrentClients,
				WorkloadMode:            in.WorkloadMode,
				DeploymentMode:          in.DeploymentMode,
			MaxRamPerNodeGiB:        in.MaxRAMPerNodeGiB,
			})
			if err != nil {
				return err
			}
			out.Sizer = &o
			return nil
		})
	} else {
		out.ToolsSkipped = append(out.ToolsSkipped, SkipReason{Tool: "hardware_sizer", Reason: "needs_target_inputs", Detail: "target_data_size_bytes, target_concurrent_clients, workload_mode, and deployment_mode required"})
	}

	// --- Aggregate alarms (last; gives every tool a chance to emit) -----
	run("alarms_list", func() error {
		_, o, err := AlarmsList(ctx, deps, AlarmsListInput{})
		if err != nil {
			return err
		}
		out.Alarms = &o
		return nil
	})

	// --- Rollup ----------------------------------------------------------
	out.OverallHealth = rollupFullReport(&out)
	out.TopFindings = buildTopFindings(&out)
	out.Recommendations = buildTopRecommendations(&out)

	if !in.IncludeVerboseOutputs {
		compactFullReport(&out)
	}

	out.DurationMs = time.Since(start).Milliseconds()
	return nil, out, nil
}

// rollupFullReport produces a single overall_health by combining
// cluster_summary.health (if present), the proactive_health summary, the
// number of critical alarms, and key capacity signals.
func rollupFullReport(r *FullReportOutput) string {
	worst := "ok"
	bump := func(s string) {
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
	if r.Summary != nil && r.Summary.Health != nil {
		bump(r.Summary.Health.OverallHealth)
	}
	if r.Health != nil {
		bump(r.Health.Summary.OverallHealth)
	}
	if r.Alarms != nil {
		for _, a := range r.Alarms.Alarms {
			if a.Severity == diagnostics.SeverityCritical {
				bump("critical")
				break
			}
		}
	}
	return worst
}

// buildTopFindings extracts the 10 most important signals from the full
// report, ordered by severity.
func buildTopFindings(r *FullReportOutput) []string {
	type f struct {
		sev  int
		text string
	}
	var findings []f
	push := func(sev int, s string) { findings = append(findings, f{sev, s}) }

	if r.Alarms != nil {
		crit := 0
		warn := 0
		for _, a := range r.Alarms.Alarms {
			switch a.Severity {
			case diagnostics.SeverityCritical:
				crit++
			case diagnostics.SeverityWarning:
				warn++
			}
		}
		if crit > 0 {
			push(3, fmt.Sprintf("%d critical alarm(s) active — see alarms section", crit))
		}
		if warn > 0 {
			push(2, fmt.Sprintf("%d warning alarm(s) active", warn))
		}
	}
	if s := r.Summary; s != nil && s.Health != nil {
		if s.Health.MetadataCache != nil && s.Health.MetadataCache.HeadroomConnections < 0 && s.Health.MetadataCache.BudgetSource != "unavailable" {
			push(3, fmt.Sprintf("metadata cache budget exceeded by %d connections at worst-case warm",
				-s.Health.MetadataCache.HeadroomConnections))
		}
		if s.Health.MetadataCache != nil && s.Health.MetadataCache.CurrentWorstBackends > 0 {
			push(2, fmt.Sprintf("%d backend(s) currently holding fully-warm metadata cache (rebalancer / metadata sync / pg_dump / citus_tables views)",
				s.Health.MetadataCache.CurrentWorstBackends))
		}
		if s.Health.Memory != nil && s.Health.Memory.Status == "critical" {
			push(3, fmt.Sprintf("memory critical on %s (%.0f%% of node RAM)", s.Health.Memory.HottestNode, s.Health.Memory.HighestRiskPct*100))
		}
		if s.Health.Drift != nil && s.Health.Drift.ExtensionIssues > 0 {
			push(2, fmt.Sprintf("%d extension drift issue(s)", s.Health.Drift.ExtensionIssues))
		}
		if s.Health.Connections != nil && s.Health.Connections.Bottleneck != "" {
			push(1, "connection bottleneck: "+s.Health.Connections.Bottleneck)
		}
	}
	if m := r.Memory; m != nil {
		if m.MetadataCacheFootprint != nil && m.MetadataCacheFootprint.PartitionExplosion != nil {
			pe := m.MetadataCacheFootprint.PartitionExplosion
			push(1, fmt.Sprintf("partition growth %.1fx would add %d bytes / backend (%d bytes at max_connections)",
				pe.GrowthFactor, pe.DeltaPerBackend, pe.DeltaAtMaxConn))
		}
		if m.MemoryRisk != nil {
			for _, n := range m.MemoryRisk.Nodes {
				if n.RiskPct >= 0.95 {
					push(3, fmt.Sprintf("memory >=95%% on %s:%d", n.NodeName, n.NodePort))
				}
			}
		}
	}
	if r.Shards != nil && r.Shards.ShardSkew != nil && r.Shards.ShardSkew.Skew.Warning != "" && r.Shards.ShardSkew.Skew.Warning != "ok" {
		push(1, fmt.Sprintf("shard skew %s (stddev=%.2f, max=%.0f, min=%.0f)",
			r.Shards.ShardSkew.Skew.Warning, r.Shards.ShardSkew.Skew.Stddev,
			r.Shards.ShardSkew.Skew.Max, r.Shards.ShardSkew.Skew.Min))
	}
	if r.Health != nil {
		if r.Health.Summary.StuckPreparedXactCount > 0 {
			push(3, fmt.Sprintf("%d stuck prepared transaction(s)", r.Health.Summary.StuckPreparedXactCount))
		}
		if r.Health.Summary.UnhealthyPlacementCount > 0 {
			push(3, fmt.Sprintf("%d unhealthy shard placement(s)", r.Health.Summary.UnhealthyPlacementCount))
		}
		if r.Health.Summary.LongTxCount > 0 {
			push(2, fmt.Sprintf("%d long-running transaction(s)", r.Health.Summary.LongTxCount))
		}
	}

	sort.SliceStable(findings, func(i, j int) bool { return findings[i].sev > findings[j].sev })
	out := make([]string, 0, len(findings))
	for _, x := range findings {
		out = append(out, x.text)
		if len(out) >= 10 {
			break
		}
	}
	if len(out) == 0 {
		out = append(out, "no critical/warning signals detected")
	}
	return out
}

// buildTopRecommendations synthesises concrete next steps from the
// individual advisors' outputs.
func buildTopRecommendations(r *FullReportOutput) []string {
	var rec []string
	seen := map[string]bool{}
	push := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		rec = append(rec, s)
	}
	// Surface the minimum-RAM-per-node recommendation up top so operators
	// sizing hardware see it immediately. Uses the coord+workers rollup
	// produced by citus_memory_risk_report (normal + worst_case passes).
	if r.Memory != nil {
		normalMax := maxNodeTotalBytes(r.Memory.MemoryRisk)
		worstMax := maxNodeTotalBytes(r.Memory.MemoryRiskWorstCase)
		if normalMax > 0 || worstMax > 0 {
			normalStr := "unknown"
			if normalMax > 0 {
				normalStr = humanBytes(normalMax)
			}
			worstStr := "unknown"
			if worstMax > 0 {
				worstStr = humanBytes(worstMax)
			}
			push(fmt.Sprintf(
				"Minimum RAM per node — normal workload: %s, worst case (rebalancer/metadata-sync/pg_dump/citus_tables): %s. Add OS + filesystem-cache headroom on top (typically 20–30%%).",
				normalStr, worstStr))
		}
	}
	if r.Configuration != nil && r.Configuration.ConfigAdvisor != nil {
		for _, s := range r.Configuration.ConfigAdvisor.Recommendations {
			push(s)
		}
	}
	// Surface concrete max_locks_per_transaction recommendation (metadata
	// sync / rebalance / multi-shard DDL lock exhaustion).
	if r.Capacity != nil && r.Capacity.MetadataSyncRisk != nil {
		if v := r.Capacity.MetadataSyncRisk.RecommendedMaxLocksPerTransaction; v > 0 {
			push(fmt.Sprintf(
				"Raise max_locks_per_transaction to %d on every node (currently %d). Required because metadata sync / rebalance / multi-shard DDL locks every touched shard in one transaction and current setting will exhaust the shared lock table. Restart required.",
				v, r.Capacity.MetadataSyncRisk.Coordinator.MaxLocksPerTx))
		}
		for _, s := range r.Capacity.MetadataSyncRisk.Recommendations {
			push(s)
		}
	}
	// 2PC recovery — surface commit/rollback backlog prominently (operators
	// often miss prepared-xact accumulation until replication lag or WAL
	// retention alerts fire).
	if r.Workload != nil && r.Workload.TwoPCRecovery != nil {
		t := r.Workload.TwoPCRecovery
		if t.Summary.CommitNeeded+t.Summary.RollbackNeeded > 0 {
			push(fmt.Sprintf(
				"2PC recovery needed: %d commit_needed + %d rollback_needed prepared xact(s) (oldest %.0fs). Run SELECT recover_prepared_transactions(); on the coordinator, or consult two_pc_recovery.recovery_script for per-node manual recovery.",
				t.Summary.CommitNeeded, t.Summary.RollbackNeeded, t.Summary.OldestAgeSeconds))
		}
	}
	// Rebalance forensics — surface stall diagnoses and cleanup backlog.
	if r.Shards != nil && r.Shards.RebalanceForensics != nil {
		rf := r.Shards.RebalanceForensics
		if rf.Stall.Classification != "no_stall" && rf.Stall.Classification != "" && rf.Job != nil {
			push(fmt.Sprintf(
				"Rebalance job %d is stalled (%s): %s. See rebalance_forensics.playbook for recovery steps.",
				rf.Job.JobID, rf.Stall.Classification, rf.Stall.PrimaryReason))
		}
		if rf.CleanupPendingCount > 0 && rf.Stall.Classification != "cleanup_backlog" {
			push(fmt.Sprintf(
				"pg_dist_cleanup has %d pending record(s); run SELECT citus_cleanup_orphaned_resources(); on coordinator to reclaim orphaned shards left by prior moves.",
				rf.CleanupPendingCount))
		}
	}
	// Placement integrity — ghost/orphan placements.
	if r.Shards != nil && r.Shards.PlacementIntegrity != nil {
		pi := r.Shards.PlacementIntegrity
		if len(pi.GhostPlacements) > 0 {
			push(fmt.Sprintf(
				"CRITICAL: %d ghost placement(s) — metadata references shards missing on disk (reads will error). Use citus_copy_shard_placement from a healthy replica; see placement_integrity.playbook.",
				len(pi.GhostPlacements)))
		}
		if len(pi.OrphanTables) > 0 {
			unqueued := 0
			for _, o := range pi.OrphanTables {
				if !o.InCleanupList {
					unqueued++
				}
			}
			push(fmt.Sprintf(
				"%d orphan shard table(s) on workers with no placement metadata (%d not queued). Run SELECT citus_cleanup_orphaned_resources(); to drop queued orphans.",
				len(pi.OrphanTables), unqueued))
		}
	}
	if r.Alarms != nil {
		for _, a := range r.Alarms.Alarms {
			if a.Severity == diagnostics.SeverityCritical && a.FixHint != "" {
				push(a.FixHint)
			}
		}
	}
	if len(rec) > 15 {
		rec = rec[:15]
	}
	return rec
}

func maxNodeTotalBytes(m *MemoryRiskOutput) int64 {
	if m == nil {
		return 0
	}
	var mx int64
	for _, n := range m.Nodes {
		if n.TotalBytes > mx {
			mx = n.TotalBytes
		}
	}
	return mx
}


// compactFullReport drops large/verbose sub-fields the caller is unlikely
// to need in the summary mode. Users can re-request a single tool for full
// detail, or set IncludeVerboseOutputs=true.
func compactFullReport(r *FullReportOutput) {
	if r.Memory != nil && r.Memory.MetadataCacheFootprint != nil {
		// top tables tend to be the verbose part; keep max 5.
		if len(r.Memory.MetadataCacheFootprint.TopTables) > 5 {
			r.Memory.MetadataCacheFootprint.TopTables = r.Memory.MetadataCacheFootprint.TopTables[:5]
		}
	}
	if r.Workload != nil && r.Workload.QueryPathology != nil && len(r.Workload.QueryPathology.Rows) > 20 {
		r.Workload.QueryPathology.Rows = r.Workload.QueryPathology.Rows[:20]
	}
	if r.Shards != nil && r.Shards.ShardHeatmap != nil && len(r.Shards.ShardHeatmap.HotShards) > 20 {
		r.Shards.ShardHeatmap.HotShards = r.Shards.ShardHeatmap.HotShards[:20]
	}
}
