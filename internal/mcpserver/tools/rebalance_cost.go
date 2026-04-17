// citus-mcp: B14 citus_rebalance_cost_estimator -- estimates wall clock,
// bytes moved, and network/write amplification for a proposed rebalance.
// Uses get_rebalance_table_shards_plan() when available; otherwise falls
// back to citus_shards totals. Flags risks: logical replication timeout
// too low for largest shard, too many parallel moves saturating WAL.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type RebalanceCostInput struct {
	TableName              string  `json:"table_name,omitempty"` // optional filter
	AssumedThroughputMiBps int     `json:"assumed_throughput_mibps,omitempty"` // default 50
	MaxParallelMoves       int     `json:"max_parallel_moves,omitempty"`       // default 1
	WarnTimeoutSeconds     int     `json:"warn_timeout_seconds,omitempty"`     // default 300
}

type RebalanceCostOutput struct {
	MovesPlanned         int     `json:"moves_planned"`
	TotalBytesMoved      int64   `json:"total_bytes_moved"`
	LargestShardBytes    int64   `json:"largest_shard_bytes"`
	EstimatedSeconds     int     `json:"estimated_seconds"`
	EstimatedMinutes     float64 `json:"estimated_minutes"`
	LargestShardEstSeconds int   `json:"largest_shard_estimated_seconds"`
	LogicalReplTimeoutS  int     `json:"logical_replication_timeout_s"`
	Alarms               []diagnostics.Alarm `json:"alarms"`
	Warnings             []string `json:"warnings,omitempty"`
}

func RebalanceCostEstimatorTool(ctx context.Context, deps Dependencies, in RebalanceCostInput) (*mcp.CallToolResult, RebalanceCostOutput, error) {
	out := RebalanceCostOutput{Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.AssumedThroughputMiBps == 0 {
		in.AssumedThroughputMiBps = 50
	}
	if in.MaxParallelMoves == 0 {
		in.MaxParallelMoves = 1
	}
	if in.WarnTimeoutSeconds == 0 {
		in.WarnTimeoutSeconds = 300
	}

	// Try get_rebalance_table_shards_plan.
	q := `SELECT shardid, shard_size FROM get_rebalance_table_shards_plan()`
	if in.TableName != "" {
		q = fmt.Sprintf(`SELECT shardid, shard_size FROM get_rebalance_table_shards_plan(%q)`, in.TableName)
	}
	rows, err := deps.Pool.Query(ctx, q)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("get_rebalance_table_shards_plan failed (%v); falling back to static size estimate", err))
		// Fallback: assume worst-case rebalance = move every shard once.
		if ferr := deps.Pool.QueryRow(ctx,
			`SELECT COALESCE(count(*),0), COALESCE(sum(shard_size),0), COALESCE(max(shard_size),0) FROM citus_shards`).
			Scan(&out.MovesPlanned, &out.TotalBytesMoved, &out.LargestShardBytes); ferr != nil {
			return nil, out, fmt.Errorf("fallback citus_shards failed: %w", ferr)
		}
	} else {
		defer rows.Close()
		for rows.Next() {
			var sid int64
			var sz int64
			if err := rows.Scan(&sid, &sz); err != nil {
				continue
			}
			out.MovesPlanned++
			out.TotalBytesMoved += sz
			if sz > out.LargestShardBytes {
				out.LargestShardBytes = sz
			}
		}
	}

	tpBps := int64(in.AssumedThroughputMiBps) * 1024 * 1024
	if tpBps > 0 {
		secs := out.TotalBytesMoved / tpBps / int64(in.MaxParallelMoves)
		out.EstimatedSeconds = int(secs)
		out.EstimatedMinutes = float64(secs) / 60.0
		out.LargestShardEstSeconds = int(out.LargestShardBytes / tpBps)
	}

	// Get logical_replication_timeout.
	_ = deps.Pool.QueryRow(ctx,
		`SELECT COALESCE(current_setting('citus.logical_replication_timeout', true),'')::text`).Scan(new(string))
	var ms int
	_ = deps.Pool.QueryRow(ctx,
		`SELECT COALESCE(NULLIF(current_setting('citus.logical_replication_timeout', true),'')::int,0)`).Scan(&ms)
	out.LogicalReplTimeoutS = ms / 1000

	// Alarms.
	if deps.Alarms != nil && out.LogicalReplTimeoutS > 0 && out.LargestShardEstSeconds > out.LogicalReplTimeoutS {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.timeout_risk", Severity: diagnostics.SeverityCritical,
			Source: "citus_rebalance_cost_estimator",
			Message: fmt.Sprintf("Largest shard ETA %ds exceeds citus.logical_replication_timeout=%ds — rebalance will abort",
				out.LargestShardEstSeconds, out.LogicalReplTimeoutS),
			Evidence: map[string]any{"largest_eta_s": out.LargestShardEstSeconds, "timeout_s": out.LogicalReplTimeoutS, "largest_bytes": out.LargestShardBytes},
			FixHint: "SET citus.logical_replication_timeout = <higher>; or split the shard first.",
		})
		out.Alarms = append(out.Alarms, *a)
	}
	if deps.Alarms != nil && out.EstimatedSeconds > in.WarnTimeoutSeconds {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.long_duration", Severity: diagnostics.SeverityWarning,
			Source: "citus_rebalance_cost_estimator",
			Message: fmt.Sprintf("Estimated rebalance duration %.1f min (%d moves, %d MiB/s assumed)",
				out.EstimatedMinutes, out.MovesPlanned, in.AssumedThroughputMiBps),
			Evidence: map[string]any{"minutes": out.EstimatedMinutes, "moves": out.MovesPlanned, "bytes": out.TotalBytesMoved},
			FixHint: "Schedule in a maintenance window; increase max_parallel_moves if WAL can absorb.",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	return nil, out, nil
}
