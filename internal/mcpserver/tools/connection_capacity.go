// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B2: citus_connection_capacity --
// Compute the maximum safe number of client connections a cluster can
// sustain, accounting for:
//   * PG max_connections (per node)
//   * citus.max_client_connections (per node)
//   * citus.max_shared_pool_size (worker-side fan-in cap)
//   * citus.local_shared_pool_size
//   * citus.max_adaptive_executor_pool_size (per-query fan-out)
//   * per-backend memory footprint (from B1 + B1b) vs node RAM
// Produces recommendations for three deployment shapes:
//   1. Coordinator-only (classic): clients connect only to the coordinator.
//   2. MX: clients can connect to any node.
//   3. PgBouncer session / transaction mode in front of the coordinator.

package tools

import (
	"context"
	"fmt"
	"math"

	"citus-mcp/internal/db"
	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/diagnostics/memory"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ConnectionCapacityInput controls the analysis.
type ConnectionCapacityInput struct {
	NodeRamBytes       int64            `json:"node_ram_bytes,omitempty"`
	NodeRamBytesByNode map[string]int64 `json:"node_ram_bytes_by_node,omitempty"`
	// SafetyFraction of node RAM usable by backends (after shared_buffers,
	// OS, etc). Default 0.60.
	SafetyFraction float64 `json:"safety_fraction,omitempty"`
	// IncludeCoordinator defaults true.
	IncludeCoordinator *bool `json:"include_coordinator,omitempty"`
	// FanoutConcurrency is the fraction of coordinator backends expected to
	// be simultaneously fanning out to workers with a full adaptive-executor
	// pool. Default 0.5. Set to 1.0 to get the absolute worst-case number;
	// set lower (e.g. 0.3) if your workload is mostly single-shard queries
	// or idle sessions (e.g. large pgbouncer-session fronting).
	// This ONLY affects the "sustainable" field in coordinator-only mode;
	// the recommended cap still defaults to the conservative worst case.
	FanoutConcurrency float64 `json:"fanout_concurrency,omitempty"`
}

// ConnectionCapacityOutput is the tool response.
type ConnectionCapacityOutput struct {
	PerNode           []NodeCapacityReport      `json:"per_node"`
	CoordinatorOnly   ModeRecommendation        `json:"coordinator_only_mode"`
	MXMode            ModeRecommendation        `json:"mx_mode"`
	PgBouncerSession  ModeRecommendation        `json:"pgbouncer_session_mode"`
	PgBouncerTxn      ModeRecommendation        `json:"pgbouncer_transaction_mode"`
	Alarms            []diagnostics.Alarm       `json:"alarms"`
	Warnings          []string                  `json:"warnings,omitempty"`
}

// NodeCapacityReport captures the raw numbers for one node.
type NodeCapacityReport struct {
	NodeName                       string  `json:"node_name"`
	NodePort                       int32   `json:"node_port"`
	Role                           string  `json:"role"`
	Reachable                      bool    `json:"reachable"`
	Error                          string  `json:"error,omitempty"`
	MaxConnections                 int     `json:"max_connections"`
	MaxClientConnections           int     `json:"citus_max_client_connections"`
	MaxSharedPoolSize              int     `json:"citus_max_shared_pool_size"`
	LocalSharedPoolSize            int     `json:"citus_local_shared_pool_size"`
	MaxAdaptiveExecutorPoolSize    int     `json:"citus_max_adaptive_executor_pool_size"`
	MaxCachedConnsPerWorker        int     `json:"citus_max_cached_conns_per_worker"`
	PerBackendBytes                int64   `json:"per_backend_bytes"`
	NodeRamBytes                   int64   `json:"node_ram_bytes,omitempty"`
	NodeRamSource                  string  `json:"node_ram_source,omitempty"`
	MemoryDerivedMax               int     `json:"memory_derived_max_backends"`
	EffectiveMaxClientBackends     int     `json:"effective_max_client_backends"`
	EffectiveBottleneck            string  `json:"effective_bottleneck"`
}

// ModeRecommendation is one deployment-shape answer.
type ModeRecommendation struct {
	RecommendedClientMax int      `json:"recommended_client_max"`
	// SustainableClientMax is a realistic steady-state number that credits
	// typical workload behavior (e.g. not every client fans out to workers
	// at the full adaptive-executor pool size at the same instant). Only
	// populated for coordinator-only mode.
	SustainableClientMax int      `json:"sustainable_client_max,omitempty"`
	Bottleneck           string   `json:"bottleneck"`
	Explanation          string   `json:"explanation"`
	Warnings             []string `json:"warnings,omitempty"`
}

// ConnectionCapacityTool implements B2.
func ConnectionCapacityTool(ctx context.Context, deps Dependencies, in ConnectionCapacityInput) (*mcp.CallToolResult, ConnectionCapacityOutput, error) {
	out := ConnectionCapacityOutput{Alarms: []diagnostics.Alarm{}, Warnings: []string{}, PerNode: []NodeCapacityReport{}}
	if in.SafetyFraction <= 0 {
		in.SafetyFraction = 0.60
	}
	if in.SafetyFraction >= 1.0 {
		return callError(serr.CodeInvalidInput, "safety_fraction must be < 1.0", ""), out, nil
	}
	if in.FanoutConcurrency <= 0 {
		in.FanoutConcurrency = 0.5
	}
	if in.FanoutConcurrency > 1.0 {
		in.FanoutConcurrency = 1.0
	}
	includeCoord := true
	if in.IncludeCoordinator != nil {
		includeCoord = *in.IncludeCoordinator
	}

	shapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}
	citusEst := memory.EstimateCitusMetadata(shapes)

	// Build single SQL to fetch all GUCs + active backends + mem total
	sql := `
SELECT
  current_setting('max_connections')::int AS max_connections,
  COALESCE(NULLIF(current_setting('citus.max_client_connections',true),'')::int, -1) AS max_client_connections,
  COALESCE(NULLIF(current_setting('citus.max_shared_pool_size',true),'')::int, 0) AS max_shared_pool_size,
  COALESCE(NULLIF(current_setting('citus.local_shared_pool_size',true),'')::int, 0) AS local_shared_pool_size,
  COALESCE(NULLIF(current_setting('citus.max_adaptive_executor_pool_size',true),'')::int, 16) AS max_adaptive_executor_pool_size,
  COALESCE(NULLIF(current_setting('citus.max_cached_conns_per_worker',true),'')::int, 1) AS max_cached_conns_per_worker,
  (SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend' AND pid <> pg_backend_pid()) AS active_backends,
  (SELECT CASE
     WHEN has_function_privilege('pg_read_file(text,bigint,bigint,boolean)', 'EXECUTE') THEN
       (SELECT CASE
          WHEN pg_read_file('/proc/meminfo', 0, 4096, true) ~ 'MemTotal:' THEN
            (regexp_match(pg_read_file('/proc/meminfo',0,4096,true),'MemTotal:\s+(\d+)'))[1]::bigint * 1024
          ELSE 0
        END)
     ELSE 0
   END) AS mem_total_bytes
`

	// Coordinator
	reports := []NodeCapacityReport{}
	if includeCoord {
		r := NodeCapacityReport{NodeName: "coordinator", NodePort: 0, Role: "coordinator", Reachable: true}
		pgEst := memory.EstimatePgCache(shapes, false)
		r.PerBackendBytes = citusEst.Bytes + pgEst.Bytes

		row := deps.Pool.QueryRow(ctx, sql)
		var maxConn, maxClient, maxShared, localShared, maxAdaptive, maxCached, activeBack int
		var memTotal int64
		if err := row.Scan(&maxConn, &maxClient, &maxShared, &localShared, &maxAdaptive, &maxCached, &activeBack, &memTotal); err != nil {
			r.Error = "query: " + err.Error()
		} else {
			r.MaxConnections = maxConn
			r.MaxClientConnections = maxClient
			if r.MaxClientConnections <= 0 {
				r.MaxClientConnections = r.MaxConnections
			}
			r.MaxSharedPoolSize = maxShared
			r.LocalSharedPoolSize = localShared
			if r.LocalSharedPoolSize == 0 {
				r.LocalSharedPoolSize = r.MaxClientConnections / 2
			}
			r.MaxAdaptiveExecutorPoolSize = maxAdaptive
			r.MaxCachedConnsPerWorker = maxCached

			// Resolve node RAM
			ram, src := int64(0), "unknown"
			if v, ok := in.NodeRamBytesByNode["coordinator"]; ok && v > 0 {
				ram, src = v, "override"
			} else if in.NodeRamBytes > 0 {
				ram, src = in.NodeRamBytes, "override"
			} else if memTotal > 0 {
				ram, src = memTotal, "proc_meminfo"
			}
			r.NodeRamBytes = ram
			r.NodeRamSource = src

			if ram > 0 && r.PerBackendBytes > 0 {
				budget := float64(ram) * in.SafetyFraction
				r.MemoryDerivedMax = int(math.Floor(budget / float64(r.PerBackendBytes)))
			}

			candidates := []struct {
				name string
				val  int
			}{
				{"max_connections", r.MaxConnections},
				{"citus.max_client_connections", r.MaxClientConnections},
			}
			if r.MemoryDerivedMax > 0 {
				candidates = append(candidates, struct {
					name string
					val  int
				}{"memory_derived", r.MemoryDerivedMax})
			}
			minVal, minName := r.MaxConnections, "max_connections"
			for _, c := range candidates {
				if c.val > 0 && c.val < minVal {
					minVal, minName = c.val, c.name
				}
			}
			r.EffectiveMaxClientBackends = minVal
			r.EffectiveBottleneck = minName
		}
		reports = append(reports, r)
	}

	// Workers via Fanout
	if deps.Fanout != nil {
		results, err := deps.Fanout.OnWorkers(ctx, sql)
		if err != nil {
			out.Warnings = append(out.Warnings, "worker fanout failed: "+err.Error())
		} else {
			// Map nodename:port -> WorkerInfo via Topology
			infos, _ := deps.WorkerManager.Topology(ctx)
			infoMap := make(map[string]db.WorkerInfo)
			for _, inf := range infos {
				key := fmt.Sprintf("%s:%d", inf.NodeName, inf.NodePort)
				infoMap[key] = inf
			}

			for _, res := range results {
				key := fmt.Sprintf("%s:%d", res.NodeName, res.NodePort)
				r := NodeCapacityReport{NodeName: res.NodeName, NodePort: res.NodePort, Role: "worker"}
				pgEst := memory.EstimatePgCache(shapes, true)
				r.PerBackendBytes = citusEst.Bytes + pgEst.Bytes

				if !res.Success {
					r.Error = res.Error
					reports = append(reports, r)
					continue
				}
				if len(res.Rows) == 0 {
					r.Error = "no data"
					reports = append(reports, r)
					continue
				}
				r.Reachable = true

				if v, ok := res.Int("max_connections"); ok {
					r.MaxConnections = int(v)
				}
				if v, ok := res.Int("max_client_connections"); ok {
					r.MaxClientConnections = int(v)
				}
				if r.MaxClientConnections <= 0 {
					r.MaxClientConnections = r.MaxConnections
				}
				if v, ok := res.Int("max_shared_pool_size"); ok {
					r.MaxSharedPoolSize = int(v)
				}
				if v, ok := res.Int("local_shared_pool_size"); ok {
					r.LocalSharedPoolSize = int(v)
				}
				if r.LocalSharedPoolSize == 0 {
					r.LocalSharedPoolSize = r.MaxClientConnections / 2
				}
				if v, ok := res.Int("max_adaptive_executor_pool_size"); ok {
					r.MaxAdaptiveExecutorPoolSize = int(v)
				}
				if v, ok := res.Int("max_cached_conns_per_worker"); ok {
					r.MaxCachedConnsPerWorker = int(v)
				}

				memTotal, _ := res.Int("mem_total_bytes")

				// Resolve node RAM
				ram, src := int64(0), "unknown"
				if v, ok := in.NodeRamBytesByNode[key]; ok && v > 0 {
					ram, src = v, "override"
				} else if v, ok := in.NodeRamBytesByNode[res.NodeName]; ok && v > 0 {
					ram, src = v, "override"
				} else if in.NodeRamBytes > 0 {
					ram, src = in.NodeRamBytes, "override"
				} else if memTotal > 0 {
					ram, src = memTotal, "proc_meminfo"
				}
				r.NodeRamBytes = ram
				r.NodeRamSource = src

				if ram > 0 && r.PerBackendBytes > 0 {
					budget := float64(ram) * in.SafetyFraction
					r.MemoryDerivedMax = int(math.Floor(budget / float64(r.PerBackendBytes)))
				}

				candidates := []struct {
					name string
					val  int
				}{
					{"max_connections", r.MaxConnections},
					{"citus.max_client_connections", r.MaxClientConnections},
				}
				if r.MemoryDerivedMax > 0 {
					candidates = append(candidates, struct {
						name string
						val  int
					}{"memory_derived", r.MemoryDerivedMax})
				}
				minVal, minName := r.MaxConnections, "max_connections"
				for _, c := range candidates {
					if c.val > 0 && c.val < minVal {
						minVal, minName = c.val, c.name
					}
				}
				r.EffectiveMaxClientBackends = minVal
				r.EffectiveBottleneck = minName

				reports = append(reports, r)
			}
		}
	}

	out.PerNode = reports

	// Per-deployment recommendations.
	var coord *NodeCapacityReport
	workerCaps := []int{}
	workerFanout := 0
	minWorkerShared := math.MaxInt
	for i := range reports {
		r := reports[i]
		if r.Role == "coordinator" && r.Reachable {
			coord = &reports[i]
		}
		if r.Role == "worker" && r.Reachable {
			workerCaps = append(workerCaps, r.EffectiveMaxClientBackends)
			if r.MaxAdaptiveExecutorPoolSize > workerFanout {
				workerFanout = r.MaxAdaptiveExecutorPoolSize
			}
			// Shared pool effective ceiling: 0 means auto=max_client_connections,
			// -1 means disabled (no cap). For the bottleneck math we treat
			// 0/auto as max_client_connections and -1 as unbounded.
			eff := r.MaxSharedPoolSize
			if eff == 0 {
				eff = r.MaxClientConnections
			}
			if eff > 0 && eff < minWorkerShared {
				minWorkerShared = eff
			}
		}
	}

	// Coordinator-only mode: clients cap = coord effective; plus worker
	// shared-pool reverse-limit = floor(min_worker_shared / max_adaptive).
	if coord != nil {
		capAtCoord := coord.EffectiveMaxClientBackends
		bottleneck := coord.EffectiveBottleneck
		explain := fmt.Sprintf(
			"Coordinator allows %d backends (%s). Each client spawns up to %d worker connections per query (citus.max_adaptive_executor_pool_size).",
			capAtCoord, bottleneck, workerFanout,
		)
		// Worker shared-pool reverse check. The HARD ceiling assumes every
		// coord backend is simultaneously fanning out at max capacity:
		//   reverse = min_worker_shared / max_adaptive_executor_pool_size
		// The SUSTAINABLE number credits the fraction of backends that are
		// actually fanning out at any instant (fanout_concurrency, default
		// 0.5 — half the backends are idle / single-shard / cached).
		sustainable := capAtCoord
		if minWorkerShared != math.MaxInt && workerFanout > 0 {
			reverse := minWorkerShared / workerFanout
			if reverse > 0 && reverse < capAtCoord {
				capAtCoord = reverse
				bottleneck = "worker citus.max_shared_pool_size / max_adaptive_executor_pool_size"
				explain += fmt.Sprintf(
					" Hard ceiling if every coord backend fans out at the full pool: %d × clients must fit under worker citus.max_shared_pool_size=%d -> cap %d.",
					workerFanout, minWorkerShared, reverse)
			}
			// Sustainable: realistic fan-out concurrency.
			effFanout := float64(workerFanout) * in.FanoutConcurrency
			if effFanout < 1.0 {
				effFanout = 1.0
			}
			sust := int(float64(minWorkerShared) / effFanout)
			if sust > coord.EffectiveMaxClientBackends {
				sust = coord.EffectiveMaxClientBackends
			}
			sustainable = sust
			explain += fmt.Sprintf(
				" Sustainable (fanout_concurrency=%.2f): %d concurrent clients assuming %.0f%% fan out at the full pool at once, the rest idle/single-shard/cached.",
				in.FanoutConcurrency, sust, in.FanoutConcurrency*100)
		}
		out.CoordinatorOnly = ModeRecommendation{
			RecommendedClientMax: capAtCoord,
			SustainableClientMax: sustainable,
			Bottleneck:           bottleneck,
			Explanation:          explain,
		}
	}

	// MX mode: every node serves clients directly. Cluster-wide total is the
	// sum of per-node caps; but remember a single client query can still
	// fan out to other nodes, so the cap is the min of (sum, min_cap × nodes).
	if coord != nil || len(workerCaps) > 0 {
		nodeCaps := []int{}
		if coord != nil {
			nodeCaps = append(nodeCaps, coord.EffectiveMaxClientBackends)
		}
		nodeCaps = append(nodeCaps, workerCaps...)
		minCap, sum := math.MaxInt, 0
		for _, c := range nodeCaps {
			if c < minCap {
				minCap = c
			}
			sum += c
		}
		mxW := []string{}
		if minCap < math.MaxInt {
			// Rule of thumb: in MX, cluster cap ≈ min(per-node cap) × N
			// because any node can serve any client and they can still fan
			// out to other nodes through shared pool.
			if minWorkerShared != math.MaxInt && workerFanout > 0 {
				mxW = append(mxW, fmt.Sprintf("Each node's worker-connection intake still limited by citus.max_shared_pool_size=%d.", minWorkerShared))
			}
			out.MXMode = ModeRecommendation{
				RecommendedClientMax: sum,
				Bottleneck:           "sum of per-node effective caps",
				Explanation: fmt.Sprintf(
					"MX: cluster-wide client cap = sum of per-node caps = %d. Per-node floor: %d. Ensure pg_hba + DNS routes clients across nodes.",
					sum, minCap,
				),
				Warnings: mxW,
			}
		}
	}

	// PgBouncer session-mode in front of coordinator: safe_client_max can be
	// much higher but pooler default_pool_size must be <= coordinator cap.
	if coord != nil {
		out.PgBouncerSession = ModeRecommendation{
			RecommendedClientMax: coord.EffectiveMaxClientBackends * 10, // 1:10 typical
			Bottleneck:           "pgbouncer default_pool_size (set to coord cap)",
			Explanation: fmt.Sprintf(
				"PgBouncer session mode preserves GUCs, prepared statements, and Citus cached worker connections. Set default_pool_size=%d and max_client_conn >= that. 10× multiplier assumes clients are often idle.",
				coord.EffectiveMaxClientBackends,
			),
		}
		out.PgBouncerTxn = ModeRecommendation{
			RecommendedClientMax: coord.EffectiveMaxClientBackends * 50,
			Bottleneck:           "pgbouncer default_pool_size (set to coord cap)",
			Explanation: fmt.Sprintf(
				"PgBouncer transaction mode allows much higher client fan-in (50× shown), but BREAKS: Citus cached worker connections (max_cached_conns_per_worker=%d) become useless; session-level SET / LOCAL temp tables / prepared statements are unsafe. Use ONLY if the app is stateless at the transaction boundary.",
				coord.MaxCachedConnsPerWorker,
			),
			Warnings: []string{
				"transaction mode invalidates Citus cached worker connections (performance loss)",
				"no session GUCs / advisory locks / prepared stmts outside a txn",
				"2PC with max_prepared_transactions requires session affinity - risky in txn mode",
			},
		}
	}

	// Alarms: memory bottleneck (users often don't realize memory is tighter
	// than max_connections).
	if deps.Alarms != nil {
		for _, r := range reports {
			if r.EffectiveBottleneck == "memory_derived" && r.MemoryDerivedMax > 0 && r.MaxConnections > r.MemoryDerivedMax {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind:     "connection_capacity.memory_bound",
					Severity: diagnostics.SeverityWarning,
					Source:   "citus_connection_capacity",
					Message: fmt.Sprintf("%s %s:%d max_connections=%d exceeds memory-safe cap %d (per-backend %d B, RAM %d B, safety %.2f)",
						r.Role, r.NodeName, r.NodePort, r.MaxConnections, r.MemoryDerivedMax,
						r.PerBackendBytes, r.NodeRamBytes, in.SafetyFraction),
					Evidence: map[string]any{
						"node":             r.NodeName,
						"port":             r.NodePort,
						"max_connections":  r.MaxConnections,
						"memory_cap":       r.MemoryDerivedMax,
						"per_backend":      r.PerBackendBytes,
						"node_ram":         r.NodeRamBytes,
						"safety_fraction":  in.SafetyFraction,
					},
					FixHint: "Lower max_connections to the memory_derived cap, or reduce per-backend footprint (partitions/shards), or add RAM.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	}

	return nil, out, nil
}
