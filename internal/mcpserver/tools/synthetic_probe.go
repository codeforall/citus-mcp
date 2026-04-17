// citus-mcp: B22 citus_synthetic_probe -- bounded read-only microbenchmark
// to establish a latency baseline for the cluster. Runs:
//   1. A trivial SELECT 1 (coordinator round trip)
//   2. A single-shard router query (SELECT count(*) FROM <dist_table> WHERE <col> = <const>)
//   3. A fanout count over a distributed table (SELECT count(*) FROM <dist_table>)
//
// All queries are synthetic and run N iterations; median, p50, p95 reported.
// Emits `probe.high_latency` if p95 exceeds threshold.

package tools

import (
	"context"
	"fmt"
	"sort"
	"time"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SyntheticProbeInput struct {
	Iterations      int     `json:"iterations,omitempty"`       // default 10
	DistributedTable string `json:"distributed_table,omitempty"` // auto-pick smallest if empty
	HighP95Ms       float64 `json:"high_p95_ms,omitempty"`      // default 500
}

type ProbeResult struct {
	Name     string  `json:"name"`
	Query    string  `json:"query"`
	N        int     `json:"n"`
	P50Ms    float64 `json:"p50_ms"`
	P95Ms    float64 `json:"p95_ms"`
	MinMs    float64 `json:"min_ms"`
	MaxMs    float64 `json:"max_ms"`
	Error    string  `json:"error,omitempty"`
}

type SyntheticProbeOutput struct {
	Results []ProbeResult       `json:"results"`
	Alarms  []diagnostics.Alarm `json:"alarms"`
	Warnings []string           `json:"warnings,omitempty"`
}

func SyntheticProbeTool(ctx context.Context, deps Dependencies, in SyntheticProbeInput) (*mcp.CallToolResult, SyntheticProbeOutput, error) {
	out := SyntheticProbeOutput{Results: []ProbeResult{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.Iterations == 0 {
		in.Iterations = 10
	}
	if in.HighP95Ms == 0 {
		in.HighP95Ms = 500
	}

	// Pick a distributed table if not given: smallest by size (to keep the probe cheap).
	tbl := in.DistributedTable
	if tbl == "" {
		err := deps.Pool.QueryRow(ctx, `
SELECT logicalrelid::regclass::text
FROM pg_dist_partition
ORDER BY pg_catalog.pg_total_relation_size(logicalrelid) ASC
LIMIT 1`).Scan(&tbl)
		if err != nil || tbl == "" {
			out.Warnings = append(out.Warnings, "no distributed table found — skipping shard/fanout probes")
		}
	}
	// Find distribution column for the router test.
	var distCol string
	if tbl != "" {
		_ = deps.Pool.QueryRow(ctx, `
SELECT a.attname
FROM pg_dist_partition p
JOIN pg_attribute a ON a.attrelid = p.logicalrelid
  AND a.attnum = (regexp_match(p.partkey,'varattno\s+(\d+)'))[1]::int
WHERE p.logicalrelid::regclass::text = $1
LIMIT 1`, tbl).Scan(&distCol)
	}

	probes := []struct {
		name, query string
	}{{"roundtrip", "SELECT 1"}}
	if tbl != "" {
		probes = append(probes, struct{ name, query string }{
			"fanout_count", fmt.Sprintf("SELECT count(*) FROM %s", tbl),
		})
		if distCol != "" {
			probes = append(probes, struct{ name, query string }{
				"router_const", fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = 0", tbl, distCol),
			})
		}
	}

	for _, p := range probes {
		pr := ProbeResult{Name: p.name, Query: p.query, N: in.Iterations}
		samples := make([]float64, 0, in.Iterations)
		for i := 0; i < in.Iterations; i++ {
			start := time.Now()
			rows, err := deps.Pool.Query(ctx, p.query)
			if err != nil {
				pr.Error = err.Error()
				break
			}
			for rows.Next() {
				_ = rows.Scan(new(any))
			}
			rows.Close()
			samples = append(samples, float64(time.Since(start).Microseconds())/1000.0)
		}
		if len(samples) > 0 {
			pr.MinMs = samples[0]
			pr.MaxMs = samples[0]
			for _, v := range samples {
				if v < pr.MinMs {
					pr.MinMs = v
				}
				if v > pr.MaxMs {
					pr.MaxMs = v
				}
			}
			sort.Float64s(samples)
			pr.P50Ms = samples[len(samples)/2]
			idx := int(float64(len(samples))*0.95) - 1
			if idx < 0 {
				idx = 0
			}
			if idx >= len(samples) {
				idx = len(samples) - 1
			}
			pr.P95Ms = samples[idx]
			if pr.P95Ms > in.HighP95Ms && deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "probe.high_latency", Severity: diagnostics.SeverityWarning,
					Source: "citus_synthetic_probe",
					Message: fmt.Sprintf("Probe %q p95=%.1fms exceeds %v", p.name, pr.P95Ms, in.HighP95Ms),
					Evidence: map[string]any{"probe": p.name, "p95_ms": pr.P95Ms, "p50_ms": pr.P50Ms, "query": p.query},
					FixHint: "Check pg_stat_activity for contention; re-run during quiet period for baseline.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
		out.Results = append(out.Results, pr)
	}
	return nil, out, nil
}
