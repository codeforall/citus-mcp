// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B5: citus_config_deep_inspect --
// Cross-node GUC-drift detector. The existing citus_config_advisor scores
// a *single* node's configuration against best practices; this tool does
// the complementary job: for each GUC in a curated safety-critical list,
// read its value on every node (coordinator + workers) and flag any
// divergence. Many Citus incidents are caused by silent drift after a
// partial rolling restart or hand-patched postgresql.conf on one node.
//
// Drift categories:
//   * CRITICAL   -- wal_level, max_prepared_transactions, max_connections.
//                   Differences here cause HA/2PC/capacity failures.
//   * WARNING    -- shared_buffers, work_mem, citus.max_shared_pool_size,
//                   citus.max_adaptive_executor_pool_size. Affects
//                   capacity planning and memory estimates.
//   * INFO       -- timezone, jit, default_statistics_target. Cosmetic
//                   or planner-only differences.

package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"citus-mcp/internal/db"
	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// driftRule describes one GUC to check and the severity of any divergence.
type driftRule struct {
	Name     string
	Severity diagnostics.Severity
	Note     string
}

// defaultDriftRules is the curated safety-critical set.
var defaultDriftRules = []driftRule{
	// Critical: correctness / HA / 2PC / connection cap.
	{Name: "wal_level", Severity: diagnostics.SeverityCritical,
		Note: "Logical replication and streaming require identical wal_level across nodes."},
	{Name: "max_prepared_transactions", Severity: diagnostics.SeverityCritical,
		Note: "Citus 2PC requires >0 on every node; drift silently disables 2PC on the lower side."},
	{Name: "max_connections", Severity: diagnostics.SeverityCritical,
		Note: "Drift breaks capacity planning; workers with lower caps become the cluster-wide bottleneck."},
	{Name: "max_worker_processes", Severity: diagnostics.SeverityCritical,
		Note: "Must be high enough on every node to accommodate Citus parallel executor workers."},
	{Name: "max_locks_per_transaction", Severity: diagnostics.SeverityCritical,
		Note: "Citus locks every shard per tx; drift or under-sized values cause metadata sync, rebalance, and multi-shard DDL to fail with 'out of shared memory'. Scale with shard count (>=256 for <=1k shards, >=1024 for 1k-10k)."},

	// Warning: performance + memory model.
	{Name: "shared_buffers", Severity: diagnostics.SeverityWarning,
		Note: "Different shared_buffers across nodes makes memory budgeting inconsistent."},
	{Name: "work_mem", Severity: diagnostics.SeverityWarning,
		Note: "Distributed queries inherit coordinator's work_mem; worker drift affects sort/hash spill."},
	{Name: "maintenance_work_mem", Severity: diagnostics.SeverityWarning,
		Note: "Affects VACUUM / CREATE INDEX / autovacuum memory per node."},
	{Name: "effective_cache_size", Severity: diagnostics.SeverityWarning,
		Note: "Planner uses this; mismatched values produce different plans on coord vs worker."},
	{Name: "citus.max_shared_pool_size", Severity: diagnostics.SeverityWarning,
		Note: "Worker-side fan-in cap; drift silently lowers cluster throughput."},
	{Name: "citus.max_client_connections", Severity: diagnostics.SeverityWarning,
		Note: "Client-side cap; should usually match across nodes."},
	{Name: "citus.max_adaptive_executor_pool_size", Severity: diagnostics.SeverityWarning,
		Note: "Worst-case worker fan-out per client query."},
	{Name: "citus.multi_shard_modify_mode", Severity: diagnostics.SeverityWarning,
		Note: "Parallel vs sequential modify mode drift causes inconsistent lock behaviour."},
	{Name: "citus.enable_repartition_joins", Severity: diagnostics.SeverityWarning,
		Note: "Repartition join capability should be uniform across nodes."},
	{Name: "citus.shard_replication_factor", Severity: diagnostics.SeverityWarning,
		Note: "Default RF for new tables; drift creates inconsistent reference/distributed defaults."},
	{Name: "autovacuum", Severity: diagnostics.SeverityWarning,
		Note: "Autovacuum disabled on any node risks bloat accumulation."},
	{Name: "autovacuum_max_workers", Severity: diagnostics.SeverityWarning,
		Note: "Used in memory budgeting (B4)."},

	// Info: cosmetic / planner tweaks.
	{Name: "timezone", Severity: diagnostics.SeverityInfo,
		Note: "Timezone drift affects log timestamps and some timestamptz conversions."},
	{Name: "default_statistics_target", Severity: diagnostics.SeverityInfo,
		Note: "Planner statistics target; drift affects plan stability."},
	{Name: "jit", Severity: diagnostics.SeverityInfo,
		Note: "JIT drift can cause plan-time surprises on worker-pushed subplans."},
	{Name: "track_activity_query_size", Severity: diagnostics.SeverityInfo,
		Note: "Affects pg_stat_activity query truncation; drift is cosmetic but confusing."},
}

// ConfigDeepInspectInput controls the scan.
type ConfigDeepInspectInput struct {
	// ExtraGucs adds further GUCs to the default set (severity defaults to
	// warning). Useful to audit site-specific settings.
	ExtraGucs []string `json:"extra_gucs,omitempty"`
	// Ignore lists GUCs to skip. Example: ["timezone"] when intentional.
	Ignore []string `json:"ignore,omitempty"`
	// IncludeCoordinator defaults true.
	IncludeCoordinator *bool `json:"include_coordinator,omitempty"`
}

// ConfigDeepInspectOutput is the tool response.
type ConfigDeepInspectOutput struct {
	GucDrifts []GucDriftReport    `json:"guc_drifts"`
	Summary   DriftSummary        `json:"summary"`
	Alarms    []diagnostics.Alarm `json:"alarms"`
	Warnings  []string            `json:"warnings,omitempty"`
}

// GucDriftReport is one GUC's per-node picture. Two severity fields are
// exposed to avoid the class-vs-finding confusion:
//
//   - `Class` describes the inherent criticality of this GUC IF it drifts
//     (e.g. wal_level is critical because a mismatch breaks logical
//     replication; timezone is info because it only affects log
//     timestamps). Class never depends on whether the GUC drifted here.
//
//   - `Severity` is the SEVERITY OF THIS FINDING: empty string when the
//     GUC did not drift on this cluster (there IS no finding), otherwise
//     equal to Class. Consumers that want to know "is there something
//     wrong here" must check Severity, not Class.
type GucDriftReport struct {
	GUC            string            `json:"guc"`
	Class          string            `json:"class"`
	Severity       string            `json:"severity"` // empty when Drifted=false
	Note           string            `json:"note"`
	Values         map[string]string `json:"values"` // "coordinator" / "host:port" -> value
	DistinctValues int               `json:"distinct_values"`
	Drifted        bool              `json:"drifted"`
}

// DriftSummary aggregates top-level counts.
type DriftSummary struct {
	TotalGucs          int `json:"total_gucs"`
	DriftedGucs        int `json:"drifted_gucs"`
	CriticalDrifts     int `json:"critical_drifts"`
	WarningDrifts      int `json:"warning_drifts"`
	InfoDrifts         int `json:"info_drifts"`
	NodesInspected     int `json:"nodes_inspected"`
	NodesUnreachable   int `json:"nodes_unreachable"`
}

// ConfigDeepInspectTool implements B5.
func ConfigDeepInspectTool(ctx context.Context, deps Dependencies, in ConfigDeepInspectInput) (*mcp.CallToolResult, ConfigDeepInspectOutput, error) {
	out := ConfigDeepInspectOutput{GucDrifts: []GucDriftReport{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	includeCoord := true
	if in.IncludeCoordinator != nil {
		includeCoord = *in.IncludeCoordinator
	}

	// Build effective rule set.
	ignore := make(map[string]bool, len(in.Ignore))
	for _, g := range in.Ignore {
		ignore[g] = true
	}
	rules := make([]driftRule, 0, len(defaultDriftRules)+len(in.ExtraGucs))
	for _, r := range defaultDriftRules {
		if !ignore[r.Name] {
			rules = append(rules, r)
		}
	}
	for _, g := range in.ExtraGucs {
		if ignore[g] {
			continue
		}
		rules = append(rules, driftRule{Name: g, Severity: diagnostics.SeverityWarning, Note: "user-supplied"})
	}

	// Build SQL with inline ARRAY for GUC names (can't use params in Fanout)
	names := make([]string, 0, len(rules))
	for _, r := range rules {
		names = append(names, r.Name)
	}
	quotedNames := make([]string, len(names))
	for i, n := range names {
		quotedNames[i] = db.QuoteLiteral(n)
	}
	sql := fmt.Sprintf(`SELECT name, setting FROM pg_settings WHERE name = ANY(ARRAY[%s])`,
		strings.Join(quotedNames, ","))

	// Coordinator query
	nodeValues := map[string]map[string]string{}
	if includeCoord {
		nodeValues["coordinator"] = map[string]string{}
		rows, err := deps.Pool.Query(ctx, sql)
		if err != nil {
			out.Warnings = append(out.Warnings, "coordinator query: "+err.Error())
			out.Summary.NodesUnreachable++
		} else {
			defer rows.Close()
			for rows.Next() {
				var name, setting string
				if err := rows.Scan(&name, &setting); err == nil {
					nodeValues["coordinator"][name] = setting
				}
			}
		}
	}

	// Workers via Fanout
	if deps.Fanout != nil {
		results, err := deps.Fanout.OnWorkers(ctx, sql)
		if err != nil {
			out.Warnings = append(out.Warnings, "worker fanout failed: "+err.Error())
		} else {
			for _, r := range results {
				key := fmt.Sprintf("%s:%d", r.NodeName, r.NodePort)
				nodeValues[key] = map[string]string{}
				if !r.Success {
					out.Summary.NodesUnreachable++
					out.Warnings = append(out.Warnings, fmt.Sprintf("%s: %s", key, r.Error))
					continue
				}
				for _, row := range r.Rows {
					if name, ok := row["name"].(string); ok {
						if setting, ok := row["setting"].(string); ok {
							nodeValues[key][name] = setting
						}
					}
				}
			}
		}
	}

	out.Summary.NodesInspected = len(nodeValues)

	// For each rule, build the per-node map and decide drift.
	out.Summary.TotalGucs = len(rules)
	for _, r := range rules {
		rep := buildGucDriftReport(r, nodeValues)
		if rep.Drifted {
			out.Summary.DriftedGucs++
			switch r.Severity {
			case diagnostics.SeverityCritical:
				out.Summary.CriticalDrifts++
			case diagnostics.SeverityWarning:
				out.Summary.WarningDrifts++
			case diagnostics.SeverityInfo:
				out.Summary.InfoDrifts++
			}
			if deps.Alarms != nil {
				keys := make([]string, 0, len(rep.Values))
				for k := range rep.Values {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				// Build a compact evidence view.
				evValues := make(map[string]string, len(rep.Values))
				for _, k := range keys {
					evValues[k] = rep.Values[k]
				}
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind:     "config.drift",
					Severity: r.Severity,
					Source:   "citus_config_deep_inspect",
					Message:  fmt.Sprintf("GUC %s differs across %d distinct values between nodes", r.Name, rep.DistinctValues),
					Evidence: map[string]any{
						"guc":             r.Name,
						"values":          evValues,
						"distinct_values": rep.DistinctValues,
						"note":            r.Note,
					},
					FixHint: "Align postgresql.conf + citus.* settings across every node and perform a coordinated reload/restart.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
		out.GucDrifts = append(out.GucDrifts, rep)
	}

	// Stable order: drifted first, then by class (criticality of this GUC
	// if it were to drift), then alphabetical. Sorting by Severity (the
	// finding severity) would put all non-drifted rows in one
	// indistinguishable empty-severity bucket.
	classRank := map[string]int{"critical": 0, "warning": 1, "info": 2}
	sort.SliceStable(out.GucDrifts, func(i, j int) bool {
		a, b := out.GucDrifts[i], out.GucDrifts[j]
		if a.Drifted != b.Drifted {
			return a.Drifted
		}
		if classRank[a.Class] != classRank[b.Class] {
			return classRank[a.Class] < classRank[b.Class]
		}
		return a.GUC < b.GUC
	})

	_ = serr.CodeInternalError // silence import if unused
	return nil, out, nil
}

// buildGucDriftReport computes a per-GUC drift report from the per-node
// value map. Exposed as a pure helper so P0-#3 severity semantics can be
// unit-tested without a live cluster: severity must equal class when a
// drift is detected, and must be the empty string otherwise. See
// docs/methodology.md "config_deep_inspect — severity semantics" for the
// rationale.
func buildGucDriftReport(r driftRule, nodeValues map[string]map[string]string) GucDriftReport {
rep := GucDriftReport{
GUC:    r.Name,
Class:  string(r.Severity),
Note:   r.Note,
Values: map[string]string{},
}
distinct := map[string]bool{}
for nodeKey, vals := range nodeValues {
v, ok := vals[r.Name]
if !ok {
v = "(not set / unreachable)"
}
rep.Values[nodeKey] = v
if ok {
distinct[v] = true
}
}
rep.DistinctValues = len(distinct)
rep.Drifted = rep.DistinctValues > 1
if rep.Drifted {
rep.Severity = rep.Class
}
return rep
}
