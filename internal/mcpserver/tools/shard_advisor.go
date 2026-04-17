// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B3: citus_shard_advisor --
// Recommends ideal shard count per distributed table based on current size,
// projected growth, cluster hardware, and metadata-cache cost.
//
// Heuristics (widely cited Citus guidance):
//   - Target shard size: 1-50 GB (default 10 GiB) for row-store tables.
//     Smaller gives more parallelism; larger reduces metadata overhead.
//   - Total shards per table should be a multiple of the worker count
//     for even distribution.
//   - Cluster-wide, total shard count should be roughly 2-4x the total
//     CPU cores across workers — more hurts planning, less underuses
//     parallelism.
//   - Tables in the same colocation group MUST share shard count; we
//     report recommendations per colocation group AND flag if any member
//     table would individually recommend differently.
//
// The advisor reconciles these competing pressures and returns a
// single recommended_shard_count per colocation group plus per-table
// rationale. Optional projected_growth_multiplier runs a "what would
// future size look like" pass through the same logic.

package tools

import (
	"context"
	"fmt"
	"sort"

	"citus-mcp/internal/diagnostics"
	memlib "citus-mcp/internal/diagnostics/memory"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ShardAdvisorInput controls recommendation knobs.
type ShardAdvisorInput struct {
	TargetShardSizeBytes       int64   `json:"target_shard_size_bytes,omitempty"`       // default 10 GiB
	MinShardSizeBytes          int64   `json:"min_shard_size_bytes,omitempty"`          // default 1 GiB
	MaxShardSizeBytes          int64   `json:"max_shard_size_bytes,omitempty"`          // default 50 GiB
	ProjectedGrowthMultiplier  float64 `json:"projected_growth_multiplier,omitempty"`   // default 1.0
	CoresPerWorker             int     `json:"cores_per_worker,omitempty"`              // user must supply for hw-bound check; 0 = skip
	ShardsPerCoreTarget        float64 `json:"shards_per_core_target,omitempty"`        // default 2.0
	MinShardsPerWorker         int     `json:"min_shards_per_worker,omitempty"`         // default 2
	MaxShardsPerTable          int     `json:"max_shards_per_table,omitempty"`          // safety cap, default 1024
	IncludeTables              []string `json:"include_tables,omitempty"`                // filter by regclass names
}

// ShardAdvisorOutput carries group + table recommendations.
type ShardAdvisorOutput struct {
	WorkerCount              int                       `json:"worker_count"`
	TotalCores               int                       `json:"total_cores,omitempty"`
	ColocationRecommendations []ColocationRec          `json:"colocation_recommendations"`
	TableRecommendations      []TableRec               `json:"table_recommendations"`
	ClusterTotalCurrentShards int                      `json:"cluster_total_current_shards"`
	ClusterTotalRecommendedShards int                  `json:"cluster_total_recommended_shards"`
	ClusterWideAdvice         string                   `json:"cluster_wide_advice,omitempty"`
	Alarms                    []diagnostics.Alarm      `json:"alarms"`
	Warnings                  []string                 `json:"warnings,omitempty"`
}

type ColocationRec struct {
	ColocationID          int32   `json:"colocation_id"`
	MemberTables          []string `json:"member_tables"`
	CurrentShardCount     int     `json:"current_shard_count"`
	TotalSizeBytes        int64   `json:"total_size_bytes"`
	ProjectedSizeBytes    int64   `json:"projected_size_bytes"`
	LargestTableBytes     int64   `json:"largest_table_bytes"`
	RecommendedShardCount int     `json:"recommended_shard_count"`
	Action                string  `json:"action"` // no_change | increase | decrease | investigate
	Rationale             string  `json:"rationale"`
}

type TableRec struct {
	TableName         string  `json:"table_name"`
	ColocationID      int32   `json:"colocation_id"`
	TableType         string  `json:"table_type"` // distributed | reference | local
	CurrentShards     int     `json:"current_shards"`
	CurrentSizeBytes  int64   `json:"current_size_bytes"`
	ProjectedSizeBytes int64  `json:"projected_size_bytes"`
	CurrentAvgShardBytes int64 `json:"current_avg_shard_bytes"`
	ProjectedAvgShardBytes int64 `json:"projected_avg_shard_bytes"`
	IsolatedRecommendation int  `json:"isolated_recommendation"`
	Note              string  `json:"note,omitempty"`
}

// ShardAdvisorTool implements B3.
func ShardAdvisorTool(ctx context.Context, deps Dependencies, in ShardAdvisorInput) (*mcp.CallToolResult, ShardAdvisorOutput, error) {
	out := ShardAdvisorOutput{
		Alarms:   []diagnostics.Alarm{},
		Warnings: []string{},
		ColocationRecommendations: []ColocationRec{},
		TableRecommendations:      []TableRec{},
	}
	// Defaults.
	if in.TargetShardSizeBytes == 0 {
		in.TargetShardSizeBytes = 10 * 1024 * 1024 * 1024
	}
	if in.MinShardSizeBytes == 0 {
		in.MinShardSizeBytes = 1 * 1024 * 1024 * 1024
	}
	if in.MaxShardSizeBytes == 0 {
		in.MaxShardSizeBytes = 50 * 1024 * 1024 * 1024
	}
	if in.ProjectedGrowthMultiplier <= 0 {
		in.ProjectedGrowthMultiplier = 1.0
	}
	if in.ShardsPerCoreTarget <= 0 {
		in.ShardsPerCoreTarget = 2.0
	}
	if in.MinShardsPerWorker == 0 {
		in.MinShardsPerWorker = 2
	}
	if in.MaxShardsPerTable == 0 {
		in.MaxShardsPerTable = 1024
	}

	// Worker count & cores.
	workerCount := 1
	if deps.WorkerManager != nil {
		infos, _ := deps.WorkerManager.Topology(ctx)
		workerCount = len(infos)
		if workerCount == 0 {
			workerCount = 1
		}
	}
	out.WorkerCount = workerCount
	if in.CoresPerWorker > 0 {
		out.TotalCores = in.CoresPerWorker * workerCount
	}

	// Fetch per-table shard counts + sizes.
	// citus_shards view joins nicely: aggregate by table + colocation_id.
	rows, err := deps.Pool.Query(ctx, `
SELECT table_name::text, citus_table_type, colocation_id,
       count(shardid)::int AS current_shards,
       COALESCE(sum(shard_size),0)::bigint AS total_size
FROM citus_shards
WHERE citus_table_type IN ('distributed','hash_distributed','range_distributed')
GROUP BY table_name, citus_table_type, colocation_id
ORDER BY colocation_id, table_name`)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("citus_shards query failed: %v", err))
		return nil, out, nil
	}
	defer rows.Close()

	type rawTable struct {
		name        string
		ttype       string
		colocID     int32
		shards      int
		totalBytes  int64
	}
	var raws []rawTable
	filter := map[string]bool{}
	for _, t := range in.IncludeTables {
		filter[t] = true
	}
	for rows.Next() {
		var r rawTable
		if err := rows.Scan(&r.name, &r.ttype, &r.colocID, &r.shards, &r.totalBytes); err != nil {
			continue
		}
		if len(filter) > 0 && !filter[r.name] {
			continue
		}
		raws = append(raws, r)
	}

	if len(raws) == 0 {
		out.Warnings = append(out.Warnings, "no distributed tables found")
		return nil, out, nil
	}

	// Build per-table recommendations + group by colocation_id.
	type colocAgg struct {
		tables        []string
		shardCount    int
		totalBytes    int64
		projectedBytes int64
		largestBytes  int64
	}
	groups := map[int32]*colocAgg{}

	for _, r := range raws {
		projBytes := int64(float64(r.totalBytes) * in.ProjectedGrowthMultiplier)
		var curAvg, projAvg int64
		if r.shards > 0 {
			curAvg = r.totalBytes / int64(r.shards)
			projAvg = projBytes / int64(r.shards)
		}
		// Isolated recommendation: projBytes / target, rounded up to multiple of worker count.
		iso := 0
		if projBytes > 0 {
			iso = int((projBytes + in.TargetShardSizeBytes - 1) / in.TargetShardSizeBytes)
		}
		iso = reconcileShardCount(iso, workerCount, in.MinShardsPerWorker, in.MaxShardsPerTable)

		note := ""
		if r.totalBytes < in.MinShardSizeBytes*int64(r.shards) && projBytes < in.MinShardSizeBytes*int64(r.shards) {
			note = "all shards well below min_shard_size_bytes — sharding is likely over-eager for current+projected volume"
		}
		if projAvg > in.MaxShardSizeBytes {
			note = "projected avg shard size exceeds max_shard_size_bytes — hot shards likely, consider more shards"
		}

		out.TableRecommendations = append(out.TableRecommendations, TableRec{
			TableName:              r.name,
			ColocationID:           r.colocID,
			TableType:              r.ttype,
			CurrentShards:          r.shards,
			CurrentSizeBytes:       r.totalBytes,
			ProjectedSizeBytes:     projBytes,
			CurrentAvgShardBytes:   curAvg,
			ProjectedAvgShardBytes: projAvg,
			IsolatedRecommendation: iso,
			Note:                   note,
		})
		out.ClusterTotalCurrentShards += r.shards

		g, ok := groups[r.colocID]
		if !ok {
			g = &colocAgg{}
			groups[r.colocID] = g
		}
		g.tables = append(g.tables, r.name)
		// shard count is a property of the group; take max (should be equal)
		if r.shards > g.shardCount {
			g.shardCount = r.shards
		}
		g.totalBytes += r.totalBytes
		g.projectedBytes += projBytes
		if r.totalBytes > g.largestBytes {
			g.largestBytes = r.totalBytes
		}
	}

	// Build colocation recommendations.
	groupIDs := make([]int32, 0, len(groups))
	for id := range groups {
		groupIDs = append(groupIDs, id)
	}
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })

	hwCapPerTable := 0
	if in.CoresPerWorker > 0 {
		// Cap each table's shard count at (total_cores * shards_per_core_target / tables_in_group) roughly;
		// but for simplicity, cap at total_cores * shards_per_core_target (the whole budget).
		hwCapPerTable = int(float64(out.TotalCores) * in.ShardsPerCoreTarget)
	}

	for _, id := range groupIDs {
		g := groups[id]
		rec := ColocationRec{
			ColocationID:       id,
			MemberTables:       g.tables,
			CurrentShardCount:  g.shardCount,
			TotalSizeBytes:     g.totalBytes,
			ProjectedSizeBytes: g.projectedBytes,
			LargestTableBytes:  g.largestBytes,
		}
		// Base recommendation on largest member table (since all members share shards,
		// the largest table is the binding constraint for shard size).
		projMax := int64(float64(g.largestBytes) * in.ProjectedGrowthMultiplier)
		recShards := 0
		if projMax > 0 {
			recShards = int((projMax + in.TargetShardSizeBytes - 1) / in.TargetShardSizeBytes)
		}
		recShards = reconcileShardCount(recShards, workerCount, in.MinShardsPerWorker, in.MaxShardsPerTable)

		// HW bound.
		if hwCapPerTable > 0 && recShards > hwCapPerTable {
			recShards = reconcileShardCount(hwCapPerTable, workerCount, in.MinShardsPerWorker, in.MaxShardsPerTable)
		}

		// Rationale.
		reasons := []string{}
		reasons = append(reasons, fmt.Sprintf("largest member %s, target shard size %s",
			humanBytes(g.largestBytes), humanBytes(in.TargetShardSizeBytes)))
		if in.ProjectedGrowthMultiplier != 1.0 {
			reasons = append(reasons, fmt.Sprintf("projected growth %.2fx", in.ProjectedGrowthMultiplier))
		}
		if hwCapPerTable > 0 {
			reasons = append(reasons, fmt.Sprintf("HW cap %d (%d workers × %d cores × %.1f)",
				hwCapPerTable, workerCount, in.CoresPerWorker, in.ShardsPerCoreTarget))
		}
		reasons = append(reasons, fmt.Sprintf("rounded to multiple of %d workers", workerCount))

		rec.RecommendedShardCount = recShards
		rec.Rationale = joinComma(reasons)

		switch {
		case recShards == g.shardCount:
			rec.Action = "no_change"
		case recShards > g.shardCount*2:
			rec.Action = "increase_significant"
		case recShards > g.shardCount:
			rec.Action = "increase"
		case recShards*2 < g.shardCount:
			rec.Action = "decrease_significant"
		case recShards < g.shardCount:
			rec.Action = "decrease"
		default:
			rec.Action = "no_change"
		}

		out.ColocationRecommendations = append(out.ColocationRecommendations, rec)
		out.ClusterTotalRecommendedShards += recShards * len(g.tables)
	}

	// Cluster-wide advice: check metadata-cache pressure implied by recommendation.
	// If recommendation explodes total shards, cross-check with the memory estimator.
	if out.ClusterTotalRecommendedShards > 0 {
		shapes, ferr := memlib.FetchShapes(ctx, deps.Pool)
		if ferr == nil {
			// Scale each shape's shard count proportionally to the recommendation.
			scaled := make([]memlib.TableShape, 0, len(shapes))
			// Compute ratio per table.
			recByTable := map[string]int{}
			for _, rec := range out.ColocationRecommendations {
				for _, tn := range rec.MemberTables {
					recByTable[tn] = rec.RecommendedShardCount
				}
			}
			for _, s := range shapes {
				if nShards, ok := recByTable[s.Name]; ok && s.NShards > 0 {
					s.NShards = nShards
				}
				scaled = append(scaled, s)
			}
			est := memlib.EstimateCitusMetadata(scaled)
			out.ClusterWideAdvice = fmt.Sprintf(
				"Applying these recommendations yields %d total shards → estimated Citus metadata cache ≈ %s per backend (was sized for %d shards).",
				out.ClusterTotalRecommendedShards, humanBytes(est.Bytes), out.ClusterTotalCurrentShards)

			if est.Bytes > 512*1024*1024 && deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind:     "shard_advisor.metadata_cost_high",
					Severity: diagnostics.SeverityWarning,
					Source:   "citus_shard_advisor",
					Message:  fmt.Sprintf("Recommended shard count would push Citus metadata cache to ~%s/backend", humanBytes(est.Bytes)),
					Evidence: map[string]any{"total_shards": out.ClusterTotalRecommendedShards, "estimated_bytes": est.Bytes},
					FixHint:  "Consider a larger target_shard_size_bytes or fewer partitions; run citus_memory_risk_report after applying.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	}

	// Per-table alarms for extreme drift.
	for _, rec := range out.ColocationRecommendations {
		switch rec.Action {
		case "increase_significant", "decrease_significant":
			if deps.Alarms != nil {
				sev := diagnostics.SeverityWarning
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind:     "shard_advisor.significant_change",
					Severity: sev,
					Source:   "citus_shard_advisor",
					Message:  fmt.Sprintf("Colocation group %d: recommend %s shard count from %d → %d (%s)",
						rec.ColocationID, rec.Action, rec.CurrentShardCount, rec.RecommendedShardCount, joinComma(rec.MemberTables)),
					Evidence: map[string]any{"colocation_id": rec.ColocationID, "current": rec.CurrentShardCount, "recommended": rec.RecommendedShardCount, "tables": rec.MemberTables},
					FixHint:  "Shard count changes require data migration. Plan a maintenance window. See citus_alter_distributed_table(..., shard_count => N).",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	}

	return nil, out, nil
}

// reconcileShardCount rounds n up to a multiple of workerCount, applies
// min-shards-per-worker floor and max-shards-per-table ceiling.
func reconcileShardCount(n, workerCount, minPerWorker, maxTotal int) int {
	if workerCount <= 0 {
		workerCount = 1
	}
	floor := minPerWorker * workerCount
	if n < floor {
		n = floor
	}
	// Round up to nearest multiple of workerCount.
	if n%workerCount != 0 {
		n = ((n / workerCount) + 1) * workerCount
	}
	if n > maxTotal {
		// Round maxTotal down to multiple of workerCount.
		n = (maxTotal / workerCount) * workerCount
		if n == 0 {
			n = workerCount
		}
	}
	return n
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func joinComma(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}
