// citus-mcp: B3b citus_shardcount_tradeoff_chart -- for a given table (or
// synthetic shape), enumerate candidate shard counts and return a matrix
// of [shard_count, avg_shard_bytes, metadata_cache_mib, est_planner_ms,
// parallelism_factor]. Pure, read-only. Uses memlib.Estimate* for
// metadata cost and a simple linear model for planner time (citus
// planner time is ~linear in shard count for multi-shard ops).

package tools

import (
	"context"

	"citus-mcp/internal/diagnostics"
	memlib "citus-mcp/internal/diagnostics/memory"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type TradeoffChartInput struct {
	TableName          string  `json:"table_name,omitempty"`          // if set, read size from citus_shards
	TotalDataBytes     int64   `json:"total_data_bytes,omitempty"`    // otherwise user supplies
	ReplicationFactor  int     `json:"replication_factor,omitempty"`  // default 1
	WorkerCount        int     `json:"worker_count,omitempty"`        // default: live
	CandidateShards    []int   `json:"candidate_shard_counts,omitempty"` // default [1,2,4,8,16,32,64,128,256,512,1024]
	PlannerMsPerShard  float64 `json:"planner_ms_per_shard,omitempty"` // default 0.5
	NAttributes        int     `json:"n_attributes,omitempty"`        // default 10
	NIndexes           int     `json:"n_indexes,omitempty"`           // default 2
}

type TradeoffRow struct {
	ShardCount          int     `json:"shard_count"`
	AvgShardBytes       int64   `json:"avg_shard_bytes"`
	MetadataCacheMiB    int     `json:"metadata_cache_mib"`
	EstimatedPlannerMs  float64 `json:"estimated_planner_ms"`
	ParallelismFactor   float64 `json:"parallelism_factor"`
	Fit                 string  `json:"fit"` // too_few | sweet_spot | too_many
}

type TradeoffChartOutput struct {
	Input   TradeoffChartInput   `json:"input"`
	Rows    []TradeoffRow        `json:"rows"`
	SweetSpot int                `json:"sweet_spot_shard_count"`
	Alarms  []diagnostics.Alarm  `json:"alarms"`
}

func ShardCountTradeoffChartTool(ctx context.Context, deps Dependencies, in TradeoffChartInput) (*mcp.CallToolResult, TradeoffChartOutput, error) {
	out := TradeoffChartOutput{Rows: []TradeoffRow{}, Alarms: []diagnostics.Alarm{}}
	if in.ReplicationFactor == 0 {
		in.ReplicationFactor = 1
	}
	if in.PlannerMsPerShard == 0 {
		in.PlannerMsPerShard = 0.5
	}
	if in.NAttributes == 0 {
		in.NAttributes = 10
	}
	if in.NIndexes == 0 {
		in.NIndexes = 2
	}
	if len(in.CandidateShards) == 0 {
		in.CandidateShards = []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
	}
	if in.WorkerCount <= 0 && deps.WorkerManager != nil {
		infos, _ := deps.WorkerManager.Topology(ctx)
		in.WorkerCount = len(infos)
	}
	if in.WorkerCount <= 0 {
		in.WorkerCount = 1
	}
	if in.TotalDataBytes == 0 && in.TableName != "" {
		_ = deps.Pool.QueryRow(ctx,
			"SELECT COALESCE(sum(shard_size),0)::bigint FROM citus_shards WHERE table_name::text = $1",
			in.TableName).Scan(&in.TotalDataBytes)
	}
	out.Input = in

	// Pick sweet spot: target ~10 GiB per shard but not below min per worker.
	const targetShardBytes = int64(10) * 1024 * 1024 * 1024
	sweetSpotShardCount := int(in.TotalDataBytes/targetShardBytes) + 1
	if sweetSpotShardCount < 2*in.WorkerCount {
		sweetSpotShardCount = 2 * in.WorkerCount
	}

	// Choose which candidate is closest to sweet spot.
	closest := 0
	bestDiff := int(^uint(0) >> 1)
	for _, c := range in.CandidateShards {
		diff := c - sweetSpotShardCount
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			closest = c
		}
	}
	out.SweetSpot = closest

	for _, n := range in.CandidateShards {
		avg := int64(0)
		if n > 0 && in.TotalDataBytes > 0 {
			avg = in.TotalDataBytes / int64(n)
		}
		est := memlib.EstimateCitusMetadata([]memlib.TableShape{{
			Name: "__sim__", NShards: n, ReplicationFactor: in.ReplicationFactor,
			NAttributes: in.NAttributes, NIndexes: in.NIndexes,
		}})
		metaMiB := int((est.Bytes + 1024*1024 - 1) / (1024 * 1024))
		// Parallelism factor: capped at worker_count (you cannot parallelize
		// across more nodes than you have); linear below that.
		pf := float64(n)
		if pf > float64(in.WorkerCount) {
			pf = float64(in.WorkerCount)
		}
		plannerMs := float64(n) * in.PlannerMsPerShard
		fit := "sweet_spot"
		switch {
		case n < in.WorkerCount:
			fit = "too_few_no_parallelism"
		case avg > 50*1024*1024*1024:
			fit = "too_few_shards_too_large"
		case avg < 100*1024*1024 && in.TotalDataBytes > 0 && n > 4*in.WorkerCount:
			fit = "too_many_shards_too_small"
		case metaMiB > 256:
			fit = "too_many_metadata_heavy"
		}
		out.Rows = append(out.Rows, TradeoffRow{
			ShardCount: n, AvgShardBytes: avg, MetadataCacheMiB: metaMiB,
			EstimatedPlannerMs: plannerMs, ParallelismFactor: pf, Fit: fit,
		})
	}

	return nil, out, nil
}
