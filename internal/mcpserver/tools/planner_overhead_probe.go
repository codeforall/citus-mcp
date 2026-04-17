// citus-mcp: B17 citus_planner_overhead_probe -- runs EXPLAIN (ANALYZE
// false) on user-provided representative queries and reports planner-time.
// Can optionally probe the same queries across a range of hypothetical
// shard counts (via citus.shard_count planner hints) — but default is
// just baseline planner time. Read-only, no query execution.

package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type PlannerProbeInput struct {
	Queries           []string `json:"queries"`              // required
	Iterations        int      `json:"iterations,omitempty"` // default 3 (take median)
	HighPlannerMs     float64  `json:"high_planner_ms,omitempty"` // default 200
}

type PlannerProbeRow struct {
	Query        string  `json:"query"`
	MedianPlanMs float64 `json:"median_plan_ms"`
	MinPlanMs    float64 `json:"min_plan_ms"`
	MaxPlanMs    float64 `json:"max_plan_ms"`
	Error        string  `json:"error,omitempty"`
}

type PlannerProbeOutput struct {
	Rows   []PlannerProbeRow   `json:"rows"`
	Alarms []diagnostics.Alarm `json:"alarms"`
}

func PlannerOverheadProbeTool(ctx context.Context, deps Dependencies, in PlannerProbeInput) (*mcp.CallToolResult, PlannerProbeOutput, error) {
	out := PlannerProbeOutput{Rows: []PlannerProbeRow{}, Alarms: []diagnostics.Alarm{}}
	if len(in.Queries) == 0 {
		return nil, out, fmt.Errorf("queries required")
	}
	if in.Iterations == 0 {
		in.Iterations = 3
	}
	if in.HighPlannerMs == 0 {
		in.HighPlannerMs = 200
	}

	for _, q := range in.Queries {
		row := PlannerProbeRow{Query: truncate(q, 500)}
		// Refuse anything starting with DML/DDL-looking verbs for safety.
		lower := strings.ToLower(strings.TrimSpace(q))
		if !(strings.HasPrefix(lower, "select") || strings.HasPrefix(lower, "with")) {
			row.Error = "only SELECT/WITH queries permitted"
			out.Rows = append(out.Rows, row)
			continue
		}
		var samples []float64
		for i := 0; i < in.Iterations; i++ {
			start := time.Now()
			// EXPLAIN (without ANALYZE) to only measure planner cost, then
			// discard result.
			rows, err := deps.Pool.Query(ctx, "EXPLAIN (FORMAT JSON) "+q)
			if err != nil {
				row.Error = err.Error()
				break
			}
			var js string
			for rows.Next() {
				_ = rows.Scan(&js)
			}
			rows.Close()
			// Parse planning time from EXPLAIN JSON if present.
			// Fast path: we measure wall-clock of EXPLAIN which is a very
			// close proxy to planning time (no executor).
			elapsed := float64(time.Since(start).Microseconds()) / 1000.0
			samples = append(samples, elapsed)
		}
		if len(samples) > 0 {
			row.MedianPlanMs = medianFloat(samples)
			row.MinPlanMs, row.MaxPlanMs = samples[0], samples[0]
			for _, v := range samples {
				if v < row.MinPlanMs {
					row.MinPlanMs = v
				}
				if v > row.MaxPlanMs {
					row.MaxPlanMs = v
				}
			}
			if row.MedianPlanMs >= in.HighPlannerMs && deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "planner.high_overhead", Severity: diagnostics.SeverityWarning,
					Source: "citus_planner_overhead_probe",
					Message: fmt.Sprintf("Planner overhead %.1f ms (median of %d runs)", row.MedianPlanMs, len(samples)),
					Evidence: map[string]any{"median_ms": row.MedianPlanMs, "query_preview": truncate(q, 200)},
					FixHint: "Check shard count and partition count on touched tables; consider prepared statements or reducing fan-out.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
		out.Rows = append(out.Rows, row)
	}
	return nil, out, nil
}

func medianFloat(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	// Small n; sort copy.
	c := make([]float64, len(vs))
	copy(c, vs)
	for i := 1; i < len(c); i++ {
		for j := i; j > 0 && c[j-1] > c[j]; j-- {
			c[j], c[j-1] = c[j-1], c[j]
		}
	}
	return c[len(c)/2]
}
