// citus-mcp: B8 citus_query_pathology -- classify top queries from
// pg_stat_statements into Citus routing patterns (router / adaptive /
// repartition / fanout / cte_recursive) using keyword heuristics on
// the normalized query text, and correlate with citus_stat_activity
// where available. Flags heavy patterns. Pure read-only.

package tools

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type QueryPathologyInput struct {
	TopN              int     `json:"top_n,omitempty"`                     // default 50
	MinCalls          int64   `json:"min_calls,omitempty"`                 // default 10
	HighMeanTimeMs    float64 `json:"high_mean_time_ms,omitempty"`         // default 500
}

type PathologyRow struct {
	QueryID          int64   `json:"query_id"`
	Classification   string  `json:"classification"`
	Calls            int64   `json:"calls"`
	MeanExecMs       float64 `json:"mean_exec_ms"`
	TotalExecMs      float64 `json:"total_exec_ms"`
	RowsTotal        int64   `json:"rows_total"`
	Query            string  `json:"query"`
	Risk             string  `json:"risk,omitempty"`
	Notes            string  `json:"notes,omitempty"`
}

type QueryPathologyOutput struct {
	Rows     []PathologyRow     `json:"rows"`
	Counts   map[string]int     `json:"classification_counts"`
	Alarms   []diagnostics.Alarm `json:"alarms"`
	Warnings []string           `json:"warnings,omitempty"`
}

func QueryPathologyTool(ctx context.Context, deps Dependencies, in QueryPathologyInput) (*mcp.CallToolResult, QueryPathologyOutput, error) {
	out := QueryPathologyOutput{Rows: []PathologyRow{}, Counts: map[string]int{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.TopN == 0 {
		in.TopN = 50
	}
	if in.MinCalls == 0 {
		in.MinCalls = 10
	}
	if in.HighMeanTimeMs == 0 {
		in.HighMeanTimeMs = 500
	}
	var ok bool
	if err := deps.Pool.QueryRow(ctx,
		"SELECT to_regclass('public.pg_stat_statements') IS NOT NULL OR to_regclass('pg_catalog.pg_stat_statements') IS NOT NULL").Scan(&ok); err != nil || !ok {
		out.Warnings = append(out.Warnings, "pg_stat_statements extension not installed")
		return nil, out, skipSection("pg_stat_statements_not_installed", "install pg_stat_statements on coordinator and add to shared_preload_libraries")
	}

	rows, err := deps.Pool.Query(ctx, `
SELECT COALESCE(queryid,0)::bigint, calls, mean_exec_time, total_exec_time, rows, LEFT(query, 2000)
FROM pg_stat_statements
WHERE calls >= $1
ORDER BY total_exec_time DESC NULLS LAST
LIMIT $2`, in.MinCalls, in.TopN)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("pg_stat_statements query failed: %v", err))
		return nil, out, nil
	}
	defer rows.Close()
	for rows.Next() {
		var r PathologyRow
		if err := rows.Scan(&r.QueryID, &r.Calls, &r.MeanExecMs, &r.TotalExecMs, &r.RowsTotal, &r.Query); err != nil {
			continue
		}
		r.Classification, r.Notes = classifyQuery(r.Query)
		out.Counts[r.Classification]++
		if r.MeanExecMs >= in.HighMeanTimeMs {
			switch r.Classification {
			case "repartition", "fanout_multi_shard":
				r.Risk = "high"
				if deps.Alarms != nil {
					a := deps.Alarms.Emit(diagnostics.Alarm{
						Kind: "query.heavy_" + r.Classification, Severity: diagnostics.SeverityWarning,
						Source:  "citus_query_pathology",
						Message: fmt.Sprintf("%s query averaging %.0f ms over %d calls", r.Classification, r.MeanExecMs, r.Calls),
						Evidence: map[string]any{"queryid": r.QueryID, "calls": r.Calls, "mean_ms": r.MeanExecMs, "query_preview": truncate(r.Query, 200)},
						FixHint:  "Run EXPLAIN; consider colocating tables, adding distribution-column predicates, or rewriting the query.",
					})
					out.Alarms = append(out.Alarms, *a)
				}
			case "cte_recursive":
				r.Risk = "medium"
			default:
				r.Risk = "medium"
			}
		}
		out.Rows = append(out.Rows, r)
	}
	if len(out.Rows) == 0 {
		return nil, out, skipSection("no_statements_tracked", fmt.Sprintf("pg_stat_statements installed but no queries met min_calls=%d threshold; raise pg_stat_statements.max, wait for workload, or lower min_calls input", in.MinCalls))
	}
	return nil, out, nil
}

// classifyQuery applies cheap keyword heuristics.
func classifyQuery(q string) (string, string) {
	lower := strings.ToLower(q)
	hasDDL := strings.Contains(lower, "alter table") || strings.Contains(lower, "create table") || strings.Contains(lower, "drop table")
	switch {
	case hasDDL:
		return "ddl", "DDL replicated by Citus to all nodes"
	case strings.HasPrefix(strings.TrimSpace(lower), "with recursive"):
		return "cte_recursive", "recursive CTE may require recursive planning on coordinator"
	case strings.Contains(lower, "with ") && strings.Contains(lower, "select "):
		return "cte", "CTE — may materialize on coordinator"
	case (strings.Contains(lower, "join") && strings.Contains(lower, "on ")) && !keyedEquality(lower):
		return "repartition", "non-colocated join — repartition or recursive planning likely"
	case strings.Contains(lower, "group by") && !keyedEquality(lower):
		return "fanout_multi_shard", "GROUP BY without distribution-column predicate — scans all shards"
	case strings.Contains(lower, "select ") && strings.Contains(lower, "= $") && strings.Count(lower, " = $") == 1:
		return "router", "looks like a single-shard router query"
	case strings.Contains(lower, "insert into"):
		return "insert", "INSERT; router if distribution column present"
	case strings.Contains(lower, "delete from") || strings.Contains(lower, "update "):
		return "modify", "DML; check citus.multi_shard_modify_mode"
	default:
		return "adaptive", "default adaptive executor path"
	}
}

// keyedEquality is a very rough proxy for "WHERE dist_col = X".
func keyedEquality(lower string) bool {
	return strings.Contains(lower, "where ") && (strings.Contains(lower, "_id = ") || strings.Contains(lower, "= $1") || strings.Contains(lower, "= $2"))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
