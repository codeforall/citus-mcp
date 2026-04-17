// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B9: citus_hardware_sizer --
// Reverse-sizing tool. Given a target workload (data volume, concurrent
// clients, growth) the sizer recommends a cluster shape: node count, RAM
// per node, CPU per node, and the corresponding Citus/Postgres GUCs.
//
// Two modes:
//   - fit_to_ram  (default): caller provides max_ram_per_node_gib and we
//     pick the minimum node count that fits. Good for "what's the cheapest
//     cluster that survives this load?"
//   - fit_to_nodes: caller fixes node_count and we compute the per-node
//     RAM/CPU that would be required. Good for "I already have 5 nodes,
//     do they have enough headroom?"
//
// The sizing model is deliberately transparent: every input contributes
// a named bucket in the per-node RAM breakdown so operators can argue
// with a specific number rather than a black-box recommendation.

package tools

import (
	"context"
	"fmt"
	"math"

	"citus-mcp/internal/diagnostics"
	memlib "citus-mcp/internal/diagnostics/memory"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	gib = int64(1024) * 1024 * 1024
	mib = int64(1024) * 1024
)

// HardwareSizerInput collects the workload description.
type HardwareSizerInput struct {
	// Workload
	TargetDataSizeBytes      int64   `json:"target_data_size_bytes"`            // required
	TargetConcurrentClients  int     `json:"target_concurrent_clients"`         // required
	WorkloadMode             string  `json:"workload_mode,omitempty"`           // oltp | olap | mixed (default oltp)
	DeploymentMode           string  `json:"deployment_mode,omitempty"`         // coord-only | mx | pgbouncer-session | pgbouncer-txn (default pgbouncer-txn)
	ReplicationFactor        int     `json:"replication_factor,omitempty"`      // default 1
	TargetShardSizeBytes     int64   `json:"target_shard_size_bytes,omitempty"` // default 10 GiB

	// Sizing mode
	Mode                   string `json:"mode,omitempty"`                      // fit_to_ram | fit_to_nodes (default fit_to_ram)
	NodeCount              int    `json:"node_count,omitempty"`                // required if mode=fit_to_nodes
	MaxRamPerNodeGiB       int    `json:"max_ram_per_node_gib,omitempty"`      // default 128 when mode=fit_to_ram
	MaxNodeCount           int    `json:"max_node_count,omitempty"`            // safety cap, default 64

	// PG tuning assumptions (overrides)
	SharedBuffersPct       int    `json:"shared_buffers_pct,omitempty"`        // default 25
	WorkMemMiB             int    `json:"work_mem_mib,omitempty"`              // default 4 oltp / 32 olap
	MaintenanceWorkMemMiB  int    `json:"maintenance_work_mem_mib,omitempty"`  // default 256
	PerBackendOverheadMiB  int    `json:"per_backend_overhead_mib,omitempty"`  // default 10
	RamHeadroomPct         int    `json:"ram_headroom_pct,omitempty"`          // default 20 (for OS / page cache)

	// Citus-specific
	ShardsPerCoreTarget    float64 `json:"shards_per_core_target,omitempty"`   // default 2.0
	MaxAdaptiveExecutorPool int    `json:"max_adaptive_executor_pool_size,omitempty"` // default 16
	CachedConnsPerWorker   int     `json:"cached_conns_per_worker,omitempty"`  // default 1
}

// HardwareSizerOutput returns the recommended cluster shape.
type HardwareSizerOutput struct {
	Workload       SizerWorkload          `json:"workload"`
	Recommendation SizerRecommendation    `json:"recommendation"`
	PerNodeBudget  []RamBucket            `json:"per_node_ram_budget"`
	GUCRecommendations map[string]string  `json:"guc_recommendations"`
	Alarms         []diagnostics.Alarm    `json:"alarms"`
	Warnings       []string               `json:"warnings,omitempty"`
}

type SizerWorkload struct {
	DataSizeBytes        int64   `json:"data_size_bytes"`
	ConcurrentClients    int     `json:"concurrent_clients"`
	WorkloadMode         string  `json:"workload_mode"`
	DeploymentMode       string  `json:"deployment_mode"`
	ReplicationFactor    int     `json:"replication_factor"`
	RecommendedShards    int     `json:"recommended_total_shards"`
}

type SizerRecommendation struct {
	Mode               string  `json:"sizing_mode"`
	NodeCount          int     `json:"node_count"`
	CoresPerNode       int     `json:"cores_per_node"`
	RamPerNodeGiB      int     `json:"ram_per_node_gib"`
	TotalRamGiB        int     `json:"total_ram_gib"`
	TotalCores         int     `json:"total_cores"`
	BackendsPerNode    int     `json:"estimated_backends_per_node"`
	ShardsPerNode      int     `json:"estimated_shards_per_node"`
	MetadataCacheMiB   int     `json:"metadata_cache_per_backend_mib"`
	Rationale          string  `json:"rationale"`
}

type RamBucket struct {
	Kind      string `json:"kind"`
	Bytes     int64  `json:"bytes"`
	Note      string `json:"note,omitempty"`
}

// HardwareSizerTool implements B9.
func HardwareSizerTool(ctx context.Context, deps Dependencies, in HardwareSizerInput) (*mcp.CallToolResult, HardwareSizerOutput, error) {
	out := HardwareSizerOutput{
		Alarms:   []diagnostics.Alarm{},
		Warnings: []string{},
		GUCRecommendations: map[string]string{},
		PerNodeBudget: []RamBucket{},
	}
	if in.TargetDataSizeBytes <= 0 {
		return nil, out, fmt.Errorf("target_data_size_bytes required and must be > 0")
	}
	if in.TargetConcurrentClients <= 0 {
		return nil, out, fmt.Errorf("target_concurrent_clients required and must be > 0")
	}
	// Defaults.
	if in.WorkloadMode == "" {
		in.WorkloadMode = "oltp"
	}
	if in.DeploymentMode == "" {
		in.DeploymentMode = "pgbouncer-txn"
	}
	if in.ReplicationFactor == 0 {
		in.ReplicationFactor = 1
	}
	if in.TargetShardSizeBytes == 0 {
		in.TargetShardSizeBytes = 10 * gib
	}
	if in.Mode == "" {
		in.Mode = "fit_to_ram"
	}
	if in.MaxRamPerNodeGiB == 0 {
		in.MaxRamPerNodeGiB = 128
	}
	if in.MaxNodeCount == 0 {
		in.MaxNodeCount = 64
	}
	if in.SharedBuffersPct == 0 {
		in.SharedBuffersPct = 25
	}
	if in.WorkMemMiB == 0 {
		if in.WorkloadMode == "olap" {
			in.WorkMemMiB = 32
		} else if in.WorkloadMode == "mixed" {
			in.WorkMemMiB = 16
		} else {
			in.WorkMemMiB = 4
		}
	}
	if in.MaintenanceWorkMemMiB == 0 {
		in.MaintenanceWorkMemMiB = 256
	}
	if in.PerBackendOverheadMiB == 0 {
		in.PerBackendOverheadMiB = 10
	}
	if in.RamHeadroomPct == 0 {
		in.RamHeadroomPct = 20
	}
	if in.ShardsPerCoreTarget == 0 {
		in.ShardsPerCoreTarget = 2.0
	}
	if in.MaxAdaptiveExecutorPool == 0 {
		in.MaxAdaptiveExecutorPool = 16
	}
	if in.CachedConnsPerWorker == 0 {
		in.CachedConnsPerWorker = 1
	}

	// Compute total recommended shards.
	totalShards := int(math.Ceil(float64(in.TargetDataSizeBytes) / float64(in.TargetShardSizeBytes)))
	if totalShards < 32 {
		totalShards = 32
	}

	// Metadata cache budget: estimate one table with totalShards (rough but honest).
	metaEst := memlib.EstimateCitusMetadata([]memlib.TableShape{{
		Name: "__synthetic__", NShards: totalShards,
		ReplicationFactor: in.ReplicationFactor, NAttributes: 10, NIndexes: 2,
	}})
	metadataMiB := int((metaEst.Bytes + mib - 1) / mib)
	if metadataMiB < 8 {
		metadataMiB = 8
	}

	out.Workload = SizerWorkload{
		DataSizeBytes:     in.TargetDataSizeBytes,
		ConcurrentClients: in.TargetConcurrentClients,
		WorkloadMode:      in.WorkloadMode,
		DeploymentMode:    in.DeploymentMode,
		ReplicationFactor: in.ReplicationFactor,
		RecommendedShards: totalShards,
	}

	sizeForNodes := func(nodeCount int) (ramGiB, cores, backendsPerNode int, breakdown []RamBucket) {
		shardsPerNode := (totalShards*in.ReplicationFactor + nodeCount - 1) / nodeCount
		// Backends per worker derived from deployment mode.
		backendsPerNode = estimateBackendsPerNode(in.DeploymentMode, in.TargetConcurrentClients,
			nodeCount, in.MaxAdaptiveExecutorPool, in.CachedConnsPerWorker)

		// RAM model.
		perBackendMiB := in.WorkMemMiB + in.PerBackendOverheadMiB + metadataMiB
		// pg catalog cache: rough static estimate via memlib.
		pgEst := memlib.EstimatePgCache([]memlib.TableShape{{
			Name: "__synthetic__", NShards: shardsPerNode,
			NAttributes: 10, NIndexes: 2,
		}}, true)
		pgCacheMiB := int((pgEst.Bytes + mib - 1) / mib)
		perBackendMiB += pgCacheMiB

		backendMemMiB := int64(backendsPerNode) * int64(perBackendMiB)
		// Autovacuum workers (assume 3).
		autovacMiB := int64(3) * int64(in.MaintenanceWorkMemMiB)
		// We pick shared_buffers as a fraction of total RAM → iterative fixed point.
		// Start with backend+autovac, then solve: total = (backend+autovac+WAL+maint) / (1 - sb_pct - headroom_pct)
		walMiB := int64(64) // WAL buffers + misc
		fixedMiB := backendMemMiB + autovacMiB + walMiB
		nonFixedPct := float64(100-in.SharedBuffersPct-in.RamHeadroomPct) / 100.0
		if nonFixedPct <= 0 {
			nonFixedPct = 0.3
		}
		totalMiB := int64(math.Ceil(float64(fixedMiB) / nonFixedPct))
		// Round to nearest whole GiB, minimum 4 GiB.
		ramGiB = int((totalMiB + 1023) / 1024)
		if ramGiB < 4 {
			ramGiB = 4
		}
		sharedBufMiB := int64(ramGiB*1024) * int64(in.SharedBuffersPct) / 100
		headroomMiB := int64(ramGiB*1024) * int64(in.RamHeadroomPct) / 100

		// CPU: prefer 1 core per shardsPerCoreTarget shards on the node, min 4.
		cores = int(math.Ceil(float64(shardsPerNode) / in.ShardsPerCoreTarget))
		if cores < 4 {
			cores = 4
		}

		breakdown = []RamBucket{
			{Kind: "shared_buffers", Bytes: sharedBufMiB * mib, Note: fmt.Sprintf("%d%% of RAM", in.SharedBuffersPct)},
			{Kind: "client_backends", Bytes: backendMemMiB * mib, Note: fmt.Sprintf("%d backends × (work_mem %d MiB + overhead %d MiB + metadata %d MiB + pg_cache %d MiB)",
				backendsPerNode, in.WorkMemMiB, in.PerBackendOverheadMiB, metadataMiB, pgCacheMiB)},
			{Kind: "autovacuum", Bytes: autovacMiB * mib, Note: fmt.Sprintf("3 workers × maintenance_work_mem %d MiB", in.MaintenanceWorkMemMiB)},
			{Kind: "wal_misc", Bytes: walMiB * mib, Note: "WAL buffers + misc"},
			{Kind: "os_reserve", Bytes: headroomMiB * mib, Note: fmt.Sprintf("%d%% reserved for OS / page cache", in.RamHeadroomPct)},
		}
		return
	}

	var nodeCount int
	var ramGiB, cores, backendsPerNode int
	var breakdown []RamBucket

	switch in.Mode {
	case "fit_to_nodes":
		if in.NodeCount <= 0 {
			return nil, out, fmt.Errorf("mode=fit_to_nodes requires node_count > 0")
		}
		nodeCount = in.NodeCount
		ramGiB, cores, backendsPerNode, breakdown = sizeForNodes(nodeCount)
	case "fit_to_ram":
		// Find minimum node count that fits in MaxRamPerNodeGiB.
		found := false
		for n := 1; n <= in.MaxNodeCount; n++ {
			r, c, b, bk := sizeForNodes(n)
			if r <= in.MaxRamPerNodeGiB {
				nodeCount, ramGiB, cores, backendsPerNode, breakdown = n, r, c, b, bk
				found = true
				break
			}
			// Remember last attempt in case nothing fits.
			nodeCount, ramGiB, cores, backendsPerNode, breakdown = n, r, c, b, bk
		}
		if !found {
			out.Warnings = append(out.Warnings,
				fmt.Sprintf("no node count in [1,%d] fits within max_ram_per_node_gib=%d; reporting highest-node-count attempt (RAM would still be %d GiB/node)",
					in.MaxNodeCount, in.MaxRamPerNodeGiB, ramGiB))
			if deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind:     "hardware_sizer.infeasible",
					Severity: diagnostics.SeverityCritical,
					Source:   "citus_hardware_sizer",
					Message:  fmt.Sprintf("Target workload cannot fit in max_ram_per_node_gib=%d at any node count up to %d", in.MaxRamPerNodeGiB, in.MaxNodeCount),
					Evidence: map[string]any{"max_ram_per_node_gib": in.MaxRamPerNodeGiB, "max_node_count": in.MaxNodeCount, "last_ram_gib": ramGiB},
					FixHint:  "Increase max_ram_per_node_gib, reduce target_concurrent_clients, or rework workload_mode/deployment_mode.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	default:
		return nil, out, fmt.Errorf("unknown mode %q; use fit_to_ram or fit_to_nodes", in.Mode)
	}

	shardsPerNode := (totalShards*in.ReplicationFactor + nodeCount - 1) / nodeCount

	rationale := fmt.Sprintf(
		"data %s ÷ target shard %s ≈ %d shards (rf=%d, %d per node). Backends per node estimated via %s deployment. Shared buffers %d%%, OS reserve %d%%. Metadata cache ≈ %d MiB/backend.",
		humanBytes(in.TargetDataSizeBytes), humanBytes(in.TargetShardSizeBytes), totalShards, in.ReplicationFactor,
		shardsPerNode, in.DeploymentMode, in.SharedBuffersPct, in.RamHeadroomPct, metadataMiB)

	out.Recommendation = SizerRecommendation{
		Mode:             in.Mode,
		NodeCount:        nodeCount,
		CoresPerNode:     cores,
		RamPerNodeGiB:    ramGiB,
		TotalRamGiB:      ramGiB * nodeCount,
		TotalCores:       cores * nodeCount,
		BackendsPerNode:  backendsPerNode,
		ShardsPerNode:    shardsPerNode,
		MetadataCacheMiB: metadataMiB,
		Rationale:        rationale,
	}
	out.PerNodeBudget = breakdown

	// GUC recommendations.
	out.GUCRecommendations["shared_buffers"] = fmt.Sprintf("%dMB", ramGiB*1024*in.SharedBuffersPct/100)
	out.GUCRecommendations["work_mem"] = fmt.Sprintf("%dMB", in.WorkMemMiB)
	out.GUCRecommendations["maintenance_work_mem"] = fmt.Sprintf("%dMB", in.MaintenanceWorkMemMiB)
	out.GUCRecommendations["effective_cache_size"] = fmt.Sprintf("%dMB", ramGiB*1024*(100-in.SharedBuffersPct-in.RamHeadroomPct)/100)
	out.GUCRecommendations["max_connections"] = fmt.Sprintf("%d", backendsPerNode+20)
	out.GUCRecommendations["citus.max_adaptive_executor_pool_size"] = fmt.Sprintf("%d", in.MaxAdaptiveExecutorPool)
	out.GUCRecommendations["citus.max_cached_conns_per_worker"] = fmt.Sprintf("%d", in.CachedConnsPerWorker)
	out.GUCRecommendations["citus.max_shared_pool_size"] = fmt.Sprintf("%d", (backendsPerNode+20)*nodeCount)

	// Sanity alarms.
	if ramGiB > 1024 && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "hardware_sizer.unreasonable_ram",
			Severity: diagnostics.SeverityWarning,
			Source:   "citus_hardware_sizer",
			Message:  fmt.Sprintf("Recommended RAM per node %d GiB exceeds 1 TiB — usually indicates workload should scale out instead of up", ramGiB),
			Evidence: map[string]any{"ram_gib": ramGiB, "node_count": nodeCount},
			FixHint:  "Raise max_node_count or lower target_concurrent_clients.",
		})
		out.Alarms = append(out.Alarms, *a)
	}
	if metadataMiB > 512 && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "hardware_sizer.metadata_heavy",
			Severity: diagnostics.SeverityWarning,
			Source:   "citus_hardware_sizer",
			Message:  fmt.Sprintf("Projected Citus metadata cache %d MiB/backend — each client connection will cost this much RAM", metadataMiB),
			Evidence: map[string]any{"metadata_mib": metadataMiB, "total_shards": totalShards},
			FixHint:  "Consider larger target_shard_size_bytes to reduce total shard count.",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	return nil, out, nil
}

// estimateBackendsPerNode models how many concurrent backends each node
// will have to sustain given the deployment mode.
//   - coord-only: all clients land on coordinator; workers only get the
//     adaptive executor fan-out, so per-worker ≈ clients × pool/nodes.
//   - mx: clients spread across all nodes uniformly.
//   - pgbouncer-txn: pooling dramatically reduces physical backends.
//   - pgbouncer-session: reduces by a modest factor only.
func estimateBackendsPerNode(mode string, clients, nodes, adaptivePool, cachedConns int) int {
	if nodes <= 0 {
		nodes = 1
	}
	switch mode {
	case "coord-only":
		// Workers absorb adaptive executor fanout from coordinator backends.
		return intMax(clients/nodes, (clients*intMin(adaptivePool, 4))/intMax(nodes-1, 1))
	case "mx":
		return (clients + nodes - 1) / nodes
	case "pgbouncer-session":
		return intMax(clients/2, 10) / nodes
	case "pgbouncer-txn":
		// Pool reduces backends to roughly 1 per active txn; assume 25% active.
		return intMax(clients/4, 20) / intMax(nodes, 1)
	default:
		return clients / intMax(nodes, 1)
	}
}

func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
