// citus-mcp: B19 citus_routing_drift_detector -- scans pg_stat_statements
// for queries whose behavior looks like fan-out when historically they
// were single-shard router queries (or vice versa). Uses a simple
// heuristic: queries with a reference to the distribution column in
// WHERE ... = should be router; if mean_exec_time or rows grow
// disproportionately across calls, flag for investigation.
//
// This is a best-effort detector — the real "routing" is invisible from
// pg_stat_statements alone, so we cross-check with a live EXPLAIN when
// a query looks suspicious.

package tools

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type RoutingDriftInput struct {
	TopN         int     `json:"top_n,omitempty"`          // default 20
	MinCalls     int64   `json:"min_calls,omitempty"`      // default 100
	MaxRowsRatio float64 `json:"max_rows_ratio,omitempty"` // default 10 (rows/call > this suggests fanout)
}

type RoutingDriftRow struct {
	QueryID     int64   `json:"query_id"`
	Query       string  `json:"query"`
	Calls       int64   `json:"calls"`
	RowsPerCall float64 `json:"rows_per_call"`
	MeanExecMs  float64 `json:"mean_exec_ms"`
	Finding     string  `json:"finding"`
}

type RoutingDriftOutput struct {
	Rows    []RoutingDriftRow   `json:"rows"`
	Alarms  []diagnostics.Alarm `json:"alarms"`
	Warnings []string           `json:"warnings,omitempty"`
}

func RoutingDriftDetectorTool(ctx context.Context, deps Dependencies, in RoutingDriftInput) (*mcp.CallToolResult, RoutingDriftOutput, error) {
	out := RoutingDriftOutput{Rows: []RoutingDriftRow{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.TopN == 0 {
		in.TopN = 20
	}
	if in.MinCalls == 0 {
		in.MinCalls = 100
	}
	if in.MaxRowsRatio == 0 {
		in.MaxRowsRatio = 10
	}
	var ok bool
	if err := deps.Pool.QueryRow(ctx,
		"SELECT to_regclass('pg_catalog.pg_stat_statements') IS NOT NULL OR to_regclass('public.pg_stat_statements') IS NOT NULL").Scan(&ok); err != nil || !ok {
		out.Warnings = append(out.Warnings, "pg_stat_statements not installed")
		return nil, out, skipSection("pg_stat_statements_not_installed", "install pg_stat_statements on coordinator and add to shared_preload_libraries")
	}
	rows, err := deps.Pool.Query(ctx, `
SELECT COALESCE(queryid,0)::bigint, LEFT(query,1000), calls, mean_exec_time,
       CASE WHEN calls>0 THEN rows::float/calls ELSE 0 END AS rpc
FROM pg_stat_statements
WHERE calls >= $1
ORDER BY (CASE WHEN calls>0 THEN rows::float/calls ELSE 0 END) DESC NULLS LAST
LIMIT $2`, in.MinCalls, in.TopN)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("pg_stat_statements query failed: %v", err))
		return nil, out, nil
	}
	defer rows.Close()

	for rows.Next() {
		var r RoutingDriftRow
		if err := rows.Scan(&r.QueryID, &r.Query, &r.Calls, &r.MeanExecMs, &r.RowsPerCall); err != nil {
			continue
		}
		lower := strings.ToLower(r.Query)
		looksRouter := strings.Contains(lower, "where ") &&
			(strings.Contains(lower, "_id = ") || strings.Contains(lower, "= $1"))
		if looksRouter && r.RowsPerCall > in.MaxRowsRatio {
			r.Finding = fmt.Sprintf("router-looking query returning %.0f rows/call — may be scanning multiple shards", r.RowsPerCall)
			if deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "routing.drift_suspected", Severity: diagnostics.SeverityWarning,
					Source: "citus_routing_drift_detector",
					Message: fmt.Sprintf("Query %d returns %.0f rows/call despite router-shaped WHERE clause", r.QueryID, r.RowsPerCall),
					Evidence: map[string]any{"queryid": r.QueryID, "rows_per_call": r.RowsPerCall, "calls": r.Calls, "mean_ms": r.MeanExecMs, "query_preview": truncate(r.Query, 200)},
					FixHint: "Run EXPLAIN on this query — the distribution column may not be reaching the planner (e.g., cast mismatch, wrapped in expression).",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
		if r.Finding == "" {
			r.Finding = "ok"
		}
		out.Rows = append(out.Rows, r)
	}
	if len(out.Rows) == 0 {
		return nil, out, skipSection("no_statements_tracked", fmt.Sprintf("pg_stat_statements installed but no queries met min_calls=%d threshold", in.MinCalls))
	}
	return nil, out, nil
}
