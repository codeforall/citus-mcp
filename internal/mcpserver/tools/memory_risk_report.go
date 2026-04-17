// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B4: citus_memory_risk_report --
// Roll up every known memory consumer on each node and compare to total
// node RAM. Answers: "how close are we to OOM right now?" and "which term
// dominates the budget on which node?". Emits alarms when projected usage
// crosses 80% (warning) / 95% (critical) of node RAM.
//
// Node-RAM sources (in priority order):
//   1. Per-node explicit override (input.node_ram_bytes_by_node)
//   2. Global override (input.node_ram_bytes)
//   3. pg_read_file('/proc/meminfo', ...) -- requires pg_read_server_files
//      or superuser. Silently tolerated if unavailable.
// When no RAM figure is available for a node we still report the consumer
// breakdown but skip the risk score.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/diagnostics/memory"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MemoryRiskInput controls the report.
type MemoryRiskInput struct {
	// NodeRamBytes globally overrides detected node RAM for all nodes.
	NodeRamBytes int64 `json:"node_ram_bytes,omitempty"`
	// NodeRamBytesByNode maps node name ("host:port" or "coordinator") to
	// explicit RAM in bytes. Overrides the global value for that node.
	NodeRamBytesByNode map[string]int64 `json:"node_ram_bytes_by_node,omitempty"`
	// WorstCase, when true, projects `max_connections × per_backend` instead
	// of `active_backends × per_backend` for the backend term.
	WorstCase bool `json:"worst_case,omitempty"`
	// WarnPct / CritPct override the alarm thresholds (0-1 fraction of
	// node RAM). Default 0.80 / 0.95.
	WarnPct float64 `json:"warn_pct,omitempty"`
	CritPct float64 `json:"crit_pct,omitempty"`
	// IncludeCoordinator: default true.
	IncludeCoordinator *bool `json:"include_coordinator,omitempty"`

	// PerBackendAppOverheadBytes covers (a) the PG process baseline
	// (text+data+stack+TopMemoryContext+extension static + Citus ~3-5 MB),
	// plus (b) opaque app-side per-backend allocations the server cannot
	// introspect — chiefly the server-side prepared-statement / generic
	// plan cache used by JDBC, psycopg, pgx, libpq when apps prepare
	// statements. Default 10 MiB; raise to 50-100 MiB for workloads
	// heavy on prepared statements.
	PerBackendAppOverheadBytes int64 `json:"per_backend_app_overhead_bytes,omitempty"`

	// PlanOpsPerQuery is the assumed number of memory-using plan nodes
	// (Sort, Hash, HashAgg, Materialize) per active query. PostgreSQL
	// allocates work_mem to EACH such node, not per query. Typical:
	//   1 = pure OLTP point lookups
	//   2 = mixed / light analytical (DEFAULT)
	//   5 = heavy analytical with multi-way hash joins
	PlanOpsPerQuery int `json:"plan_ops_per_query,omitempty"`
}

// MemoryRiskOutput is the tool response.
type MemoryRiskOutput struct {
	Nodes    []NodeRiskReport    `json:"nodes"`
	Alarms   []diagnostics.Alarm `json:"alarms"`
	Warnings []string            `json:"warnings,omitempty"`
}

// NodeRiskReport is one row per node.
type NodeRiskReport struct {
	NodeName        string         `json:"node_name"`
	NodePort        int32          `json:"node_port"`
	Role            string         `json:"role"`
	Reachable       bool           `json:"reachable"`
	Error           string         `json:"error,omitempty"`
	NodeRamBytes    int64          `json:"node_ram_bytes,omitempty"`
	NodeRamSource   string         `json:"node_ram_source,omitempty"` // "override" | "proc_meminfo" | "unknown"
	MaxConnections  int            `json:"max_connections"`
	ActiveBackends  int            `json:"active_backends"`
	PerBackendBytes int64          `json:"per_backend_bytes"`
	Consumers       []RiskConsumer `json:"consumers"`
	TotalBytes      int64          `json:"total_bytes"`
	RiskPct         float64        `json:"risk_pct,omitempty"` // only set when ram known
	HeadroomBytes   int64          `json:"headroom_bytes,omitempty"`
}

// RiskConsumer is one term in the budget.
type RiskConsumer struct {
	Kind  string `json:"kind"`
	Bytes int64  `json:"bytes"`
	Note  string `json:"note,omitempty"`
}

// MemoryRiskReportTool implements B4.
func MemoryRiskReportTool(ctx context.Context, deps Dependencies, in MemoryRiskInput) (*mcp.CallToolResult, MemoryRiskOutput, error) {
	out := MemoryRiskOutput{Alarms: []diagnostics.Alarm{}, Warnings: []string{}, Nodes: []NodeRiskReport{}}
	if in.WarnPct <= 0 {
		in.WarnPct = 0.80
	}
	if in.CritPct <= 0 {
		in.CritPct = 0.95
	}
	if in.PerBackendAppOverheadBytes <= 0 {
		in.PerBackendAppOverheadBytes = 10 * 1024 * 1024 // 10 MiB default
	}
	if in.PlanOpsPerQuery <= 0 {
		in.PlanOpsPerQuery = 2 // mixed workload default
	}
	includeCoord := true
	if in.IncludeCoordinator != nil {
		includeCoord = *in.IncludeCoordinator
	}

	// One shared shape catalog (coordinator-sourced) drives both per-backend
	// estimates. Coordinator uses include_shard_rels=false; workers=true.
	shapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}
	citusEst := memory.EstimateCitusMetadata(shapes)

	// Live worker count (coord-side only): used to budget coord↔worker
	// libpq connection buffers in the adaptive executor.
	var workerCount int
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*)::int FROM pg_dist_node WHERE isactive AND noderole='primary' AND groupid <> 0`).Scan(&workerCount)

	// Citus adaptive pool (coord-side GUC).
	var maxAdaptivePool int
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('citus.max_adaptive_executor_pool_size')::int`).Scan(&maxAdaptivePool)

	// Build single SQL to fetch all GUCs + activity + mem total for any
	// node. Everything here is SAFE to run on workers via fanout.
	sql := `
SELECT
  pg_size_bytes(current_setting('shared_buffers'))               AS shared_buffers_bytes,
  pg_size_bytes(current_setting('work_mem'))                     AS work_mem_bytes,
  pg_size_bytes(current_setting('maintenance_work_mem'))         AS maintenance_work_mem_bytes,
  pg_size_bytes(current_setting('wal_buffers'))                  AS wal_buffers_bytes,
  pg_size_bytes(current_setting('temp_buffers'))                 AS temp_buffers_bytes,
  COALESCE(pg_size_bytes(NULLIF(current_setting('logical_decoding_work_mem', true),'')), 64 * 1024 * 1024) AS logical_decoding_work_mem_bytes,
  current_setting('autovacuum_max_workers')::int                 AS autovac_max,
  current_setting('max_connections')::int                        AS max_connections,
  current_setting('max_prepared_transactions')::int              AS max_prepared_xacts,
  current_setting('max_locks_per_transaction')::int              AS max_locks_per_tx,
  current_setting('max_pred_locks_per_transaction')::int         AS max_pred_locks_per_tx,
  current_setting('max_worker_processes')::int                   AS max_worker_processes,
  current_setting('max_parallel_workers_per_gather')::int        AS max_parallel_per_gather,
  current_setting('max_wal_senders')::int                        AS max_wal_senders,
  COALESCE(NULLIF(current_setting('hash_mem_multiplier', true),'')::float, 1.0) AS hash_mem_multiplier,
  (SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend' AND pid <> pg_backend_pid()) AS active_backends,
  (SELECT count(*)::int FROM pg_replication_slots WHERE slot_type='logical' AND active) AS active_logical_slots,
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
	reports := []NodeRiskReport{}
	if includeCoord {
		r := NodeRiskReport{NodeName: "coordinator", NodePort: 0, Role: "coordinator", Consumers: []RiskConsumer{}, Reachable: true}
		pgEst := memory.EstimatePgCache(shapes, false)
		r.PerBackendBytes = citusEst.Bytes + pgEst.Bytes

		row := deps.Pool.QueryRow(ctx, sql)
		g, perr := scanNodeGUCs(row)
		if perr != nil {
			r.Error = "query: " + perr.Error()
		} else {
			r.MaxConnections = g.MaxConnections
			r.ActiveBackends = g.ActiveBackends
			backends := pickBackendCount(r, in.WorstCase)
			r.Consumers = buildConsumers(g, r.PerBackendBytes, backends, in, "coordinator", workerCount, maxAdaptivePool)
			for _, c := range r.Consumers {
				r.TotalBytes += c.Bytes
			}
			ram, src := resolveRAM("coordinator", in, g.MemTotal)
			r.NodeRamBytes = ram
			r.NodeRamSource = src
			if ram > 0 {
				r.RiskPct = float64(r.TotalBytes) * 100.0 / float64(ram)
				r.HeadroomBytes = ram - r.TotalBytes
			}
		}
		reports = append(reports, r)
	}

	// Workers via Fanout
	if deps.Fanout != nil {
		results, err := deps.Fanout.OnWorkers(ctx, sql)
		if err != nil {
			out.Warnings = append(out.Warnings, "worker fanout failed: "+err.Error())
		} else {
			for _, res := range results {
				key := fmt.Sprintf("%s:%d", res.NodeName, res.NodePort)
				r := NodeRiskReport{NodeName: res.NodeName, NodePort: res.NodePort, Role: "worker", Consumers: []RiskConsumer{}}
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

				g := nodeGUCs{}
				g.SharedBuffers, _ = res.Int("shared_buffers_bytes")
				g.WorkMem, _ = res.Int("work_mem_bytes")
				g.MaintWorkMem, _ = res.Int("maintenance_work_mem_bytes")
				g.WalBuffers, _ = res.Int("wal_buffers_bytes")
				g.TempBuffers, _ = res.Int("temp_buffers_bytes")
				g.LogicalDecodingWorkMem, _ = res.Int("logical_decoding_work_mem_bytes")
				autovac, _ := res.Int("autovac_max")
				g.AutoVacMax = int(autovac)
				maxConn, _ := res.Int("max_connections")
				g.MaxConnections = int(maxConn)
				mpx, _ := res.Int("max_prepared_xacts")
				g.MaxPreparedXacts = int(mpx)
				mlt, _ := res.Int("max_locks_per_tx")
				g.MaxLocksPerTx = int(mlt)
				mplt, _ := res.Int("max_pred_locks_per_tx")
				g.MaxPredLocksPerTx = int(mplt)
				mwp, _ := res.Int("max_worker_processes")
				g.MaxWorkerProcesses = int(mwp)
				mppg, _ := res.Int("max_parallel_per_gather")
				g.MaxParallelPerGather = int(mppg)
				mws, _ := res.Int("max_wal_senders")
				g.MaxWalSenders = int(mws)
				// hash_mem_multiplier is float; pull via raw row value.
				if v, ok := res.Rows[0]["hash_mem_multiplier"]; ok {
					switch vv := v.(type) {
					case float64:
						g.HashMemMultiplier = vv
					case float32:
						g.HashMemMultiplier = float64(vv)
					}
				}
				if g.HashMemMultiplier < 1.0 {
					g.HashMemMultiplier = 1.0
				}
				ab, _ := res.Int("active_backends")
				g.ActiveBackends = int(ab)
				als, _ := res.Int("active_logical_slots")
				g.ActiveLogicalSlots = int(als)
				g.MemTotal, _ = res.Int("mem_total_bytes")

				r.MaxConnections = g.MaxConnections
				r.ActiveBackends = g.ActiveBackends
				backends := pickBackendCount(r, in.WorstCase)
				r.Consumers = buildConsumers(g, r.PerBackendBytes, backends, in, "worker", 0, 0)
				for _, c := range r.Consumers {
					r.TotalBytes += c.Bytes
				}

				ram, src := int64(0), "unknown"
				if v, ok := in.NodeRamBytesByNode[key]; ok && v > 0 {
					ram, src = v, "override"
				} else if v, ok := in.NodeRamBytesByNode[res.NodeName]; ok && v > 0 {
					ram, src = v, "override"
				} else if in.NodeRamBytes > 0 {
					ram, src = in.NodeRamBytes, "override"
				} else if g.MemTotal > 0 {
					ram, src = g.MemTotal, "proc_meminfo"
				}
				r.NodeRamBytes = ram
				r.NodeRamSource = src
				if ram > 0 {
					r.RiskPct = float64(r.TotalBytes) * 100.0 / float64(ram)
					r.HeadroomBytes = ram - r.TotalBytes
				}

				reports = append(reports, r)
			}
		}
	}

	out.Nodes = reports

	// Alarms.
	if deps.Alarms != nil {
		for _, r := range reports {
			if r.NodeRamBytes <= 0 {
				continue
			}
			frac := r.RiskPct / 100.0
			var sev diagnostics.Severity
			switch {
			case frac >= in.CritPct:
				sev = diagnostics.SeverityCritical
			case frac >= in.WarnPct:
				sev = diagnostics.SeverityWarning
			default:
				continue
			}
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind:     "memory_risk.node",
				Severity: sev,
				Source:   "citus_memory_risk_report",
				Message: fmt.Sprintf("%s %s (:%d) projected memory use %.1f%% of %d bytes RAM",
					r.Role, r.NodeName, r.NodePort, r.RiskPct, r.NodeRamBytes),
				Evidence: map[string]any{
					"node_name":      r.NodeName,
					"node_port":      r.NodePort,
					"total_bytes":    r.TotalBytes,
					"node_ram":       r.NodeRamBytes,
					"risk_pct":       r.RiskPct,
					"headroom_bytes": r.HeadroomBytes,
					"consumers":      r.Consumers,
				},
				FixHint: "Reduce max_connections; reduce per-backend cache by trimming partitions/shards; lower work_mem; add RAM.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}

	return nil, out, nil
}

// nodeGUCs holds every GUC + live count we read from a node for the
// memory budget.
type nodeGUCs struct {
	SharedBuffers          int64
	WorkMem                int64
	MaintWorkMem           int64
	WalBuffers             int64
	TempBuffers            int64
	LogicalDecodingWorkMem int64
	AutoVacMax             int
	MaxConnections         int
	MaxPreparedXacts       int
	MaxLocksPerTx          int
	MaxPredLocksPerTx      int
	MaxWorkerProcesses     int
	MaxParallelPerGather   int
	MaxWalSenders          int
	HashMemMultiplier      float64
	ActiveBackends         int
	ActiveLogicalSlots     int
	MemTotal               int64
}

// rowScanner is the subset of pgx.Row we need.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanNodeGUCs(row rowScanner) (nodeGUCs, error) {
	var g nodeGUCs
	err := row.Scan(
		&g.SharedBuffers, &g.WorkMem, &g.MaintWorkMem, &g.WalBuffers, &g.TempBuffers,
		&g.LogicalDecodingWorkMem,
		&g.AutoVacMax, &g.MaxConnections, &g.MaxPreparedXacts, &g.MaxLocksPerTx,
		&g.MaxPredLocksPerTx, &g.MaxWorkerProcesses, &g.MaxParallelPerGather,
		&g.MaxWalSenders, &g.HashMemMultiplier,
		&g.ActiveBackends, &g.ActiveLogicalSlots, &g.MemTotal)
	if g.HashMemMultiplier < 1.0 {
		g.HashMemMultiplier = 1.0
	}
	return g, err
}

func pickBackendCount(r NodeRiskReport, worstCase bool) int64 {
	b := int64(r.ActiveBackends)
	if worstCase {
		b = int64(r.MaxConnections)
	}
	if b <= 0 {
		b = int64(r.MaxConnections)
	}
	return b
}

func resolveRAM(key string, in MemoryRiskInput, memTotal int64) (int64, string) {
	if v, ok := in.NodeRamBytesByNode[key]; ok && v > 0 {
		return v, "override"
	}
	if in.NodeRamBytes > 0 {
		return in.NodeRamBytes, "override"
	}
	if memTotal > 0 {
		return memTotal, "proc_meminfo"
	}
	return 0, "unknown"
}

// buildConsumers assembles the full per-node budget. Pass role="coordinator"
// to include the coord-side Citus libpq connection buffer term (needs
// workerCount + maxAdaptivePool); workers get their own inbound estimate.
func buildConsumers(g nodeGUCs, perBackendCaches int64, backends int64, in MemoryRiskInput, role string, workerCount, maxAdaptivePool int) []RiskConsumer {
	c := []RiskConsumer{}

	// 1. Shared memory (pre-allocated at startup; independent of backends).
	c = append(c, RiskConsumer{
		Kind: "shared_buffers", Bytes: g.SharedBuffers,
		Note: "PG shared buffer pool (shared memory)",
	})
	c = append(c, RiskConsumer{
		Kind: "wal_buffers", Bytes: g.WalBuffers,
		Note: "WAL buffer (shared memory)",
	})

	// 2. Lock table (shared memory, pre-allocated).
	//    PG allocates max_locks_per_transaction × (MaxBackends) lock entries,
	//    where MaxBackends = max_connections + autovacuum_max_workers +
	//    max_worker_processes + max_wal_senders + aux ~= good enough.
	//    Each entry ≈ 270 B (LOCK + PROCLOCK + dynahash overhead).
	maxBackends := int64(g.MaxConnections + g.AutoVacMax + g.MaxWorkerProcesses + g.MaxWalSenders + g.MaxPreparedXacts + 5)
	lockTableBytes := int64(g.MaxLocksPerTx) * maxBackends * 270
	c = append(c, RiskConsumer{
		Kind:  "lock_table",
		Bytes: lockTableBytes,
		Note:  fmt.Sprintf("shared lock hash: max_locks_per_transaction=%d × MaxBackends=%d × ~270 B", g.MaxLocksPerTx, maxBackends),
	})
	// Predicate-lock table (SERIALIZABLE). Small but non-zero.
	predLockBytes := int64(g.MaxPredLocksPerTx) * int64(g.MaxConnections) * 120
	c = append(c, RiskConsumer{
		Kind:  "pred_lock_table",
		Bytes: predLockBytes,
		Note:  fmt.Sprintf("SERIALIZABLE: max_pred_locks_per_transaction=%d × max_connections=%d × ~120 B", g.MaxPredLocksPerTx, g.MaxConnections),
	})
	// 2PC GXACT state.
	gxactBytes := int64(g.MaxPreparedXacts) * 850
	if gxactBytes > 0 {
		c = append(c, RiskConsumer{
			Kind:  "prepared_xact_state",
			Bytes: gxactBytes,
			Note:  fmt.Sprintf("max_prepared_transactions=%d × ~850 B GXACT slots", g.MaxPreparedXacts),
		})
	}

	// 3. WAL senders + logical decoding reorder buffers.
	walSenderBytes := int64(g.MaxWalSenders) * 4 * 1024 * 1024 // ~4 MiB per walsnd
	logicalDecodeBytes := int64(g.ActiveLogicalSlots) * g.LogicalDecodingWorkMem
	if walSenderBytes > 0 {
		c = append(c, RiskConsumer{
			Kind:  "wal_senders",
			Bytes: walSenderBytes,
			Note:  fmt.Sprintf("max_wal_senders=%d × ~4 MiB walsnd + output buffer", g.MaxWalSenders),
		})
	}
	if logicalDecodeBytes > 0 {
		c = append(c, RiskConsumer{
			Kind:  "logical_decoding",
			Bytes: logicalDecodeBytes,
			Note:  fmt.Sprintf("active_logical_slots=%d × logical_decoding_work_mem=%d B reorder buffer", g.ActiveLogicalSlots, g.LogicalDecodingWorkMem),
		})
	}

	// 4. Background/parallel worker process baselines (not in active_backends).
	//    Each forked bgw has its own text+data+stack+TopMemoryContext.
	bgwBaseline := int64(g.MaxWorkerProcesses) * 5 * 1024 * 1024 // ~5 MiB/bgw
	c = append(c, RiskConsumer{
		Kind:  "bgworker_baseline",
		Bytes: bgwBaseline,
		Note:  fmt.Sprintf("max_worker_processes=%d × ~5 MiB process baseline", g.MaxWorkerProcesses),
	})

	// 5. Per-backend terms (scale with `backends` = active or max_connections).
	c = append(c, RiskConsumer{
		Kind:  "backend_process_baseline",
		Bytes: in.PerBackendAppOverheadBytes * backends,
		Note:  fmt.Sprintf("per_backend_app_overhead=%d B × %d backends (process text/data/stack + TopMemoryContext + Citus static + app-side prepared-statement / plan cache)", in.PerBackendAppOverheadBytes, backends),
	})
	c = append(c, RiskConsumer{
		Kind:  "per_backend_caches",
		Bytes: perBackendCaches * backends,
		Note:  fmt.Sprintf("(Citus MetadataCache + PG CacheMemoryContext) × %d backends", backends),
	})

	// 6. work_mem — multi-node-per-query × hash_mem_multiplier × parallel-worker fan-out.
	//    Per active query: work_mem × plan_ops_per_query × hash_mem_multiplier × (1 + max_parallel_workers_per_gather).
	//    Budgeted one active query per backend (conservative upper bound).
	effectiveWorkMemPerBackend := int64(float64(g.WorkMem) *
		float64(in.PlanOpsPerQuery) *
		g.HashMemMultiplier *
		float64(1+g.MaxParallelPerGather))
	c = append(c, RiskConsumer{
		Kind:  "work_mem_peak",
		Bytes: effectiveWorkMemPerBackend * backends,
		Note: fmt.Sprintf("work_mem=%d × plan_ops_per_query=%d × hash_mem_multiplier=%.2f × (1 + max_parallel_workers_per_gather=%d) × %d backends",
			g.WorkMem, in.PlanOpsPerQuery, g.HashMemMultiplier, g.MaxParallelPerGather, backends),
	})

	// 7. temp_buffers × backends
	c = append(c, RiskConsumer{
		Kind:  "temp_buffers",
		Bytes: g.TempBuffers * backends,
		Note:  fmt.Sprintf("temp_buffers=%d × %d backends", g.TempBuffers, backends),
	})

	// 8. Autovacuum budget.
	c = append(c, RiskConsumer{
		Kind:  "autovacuum_budget",
		Bytes: g.MaintWorkMem * int64(g.AutoVacMax),
		Note:  fmt.Sprintf("maintenance_work_mem=%d × autovacuum_max_workers=%d", g.MaintWorkMem, g.AutoVacMax),
	})

	// 9. Citus coord↔worker libpq connection buffers (coord role only).
	//    Each active backend on coord holds up to max_adaptive_executor_pool_size
	//    connections to each worker; each has libpq send+recv buffers
	//    (~128 KiB each direction, 256 KiB total minimum per connection).
	if role == "coordinator" && workerCount > 0 && maxAdaptivePool > 0 {
		citusLibpqBytes := backends * int64(maxAdaptivePool) * int64(workerCount) * 256 * 1024
		c = append(c, RiskConsumer{
			Kind:  "citus_libpq_buffers_coord",
			Bytes: citusLibpqBytes,
			Note:  fmt.Sprintf("coord↔worker: %d backends × max_adaptive_executor_pool_size=%d × workers=%d × 256 KiB libpq", backends, maxAdaptivePool, workerCount),
		})
	}
	if role == "worker" {
		// Workers receive inbound connections — approximate as backends × 256 KiB.
		c = append(c, RiskConsumer{
			Kind:  "citus_libpq_buffers_worker",
			Bytes: backends * 256 * 1024,
			Note:  fmt.Sprintf("inbound libpq buffers: %d backends × 256 KiB", backends),
		})
	}

	return c
}
