// citus-mcp: B2b citus_connection_fanout_simulator -- pure what-if over
// Citus's distributed-query fan-out. Given a workload descriptor
// (queries/sec × avg shards touched × deployment shape), predict the
// number of concurrent worker backends the cluster will actually need.
// This is the companion of B2 (capacity) for "planning a workload that
// doesn't exist yet."

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type FanoutWorkload struct {
	Name            string  `json:"name"`
	QpsCoordinator  float64 `json:"qps_coordinator"`            // queries/sec landing on coordinator
	AvgShardsTouched int    `json:"avg_shards_touched"`         // 1 = router, N = fanout
	AvgQueryMs      float64 `json:"avg_query_ms"`               // service time on worker
	Concurrency     int     `json:"client_concurrency,omitempty"` // override: fixed concurrent clients
}

type FanoutSimInput struct {
	Workloads              []FanoutWorkload `json:"workloads"`
	WorkerCount            int              `json:"worker_count,omitempty"`             // default: live cluster
	MaxAdaptivePool        int              `json:"max_adaptive_executor_pool_size,omitempty"` // default 16
	CachedConnsPerWorker   int              `json:"cached_conns_per_worker,omitempty"`  // default 1
	MaxSharedPoolSize      int              `json:"max_shared_pool_size,omitempty"`     // default 100
}

type FanoutWorkloadResult struct {
	Name                  string  `json:"name"`
	PredictedCoordBackends int    `json:"predicted_coordinator_backends"`
	PredictedWorkerConnsPerNode int `json:"predicted_worker_connections_per_node"`
	LimitedBy             string  `json:"limited_by"`
	Notes                 string  `json:"notes,omitempty"`
}

type FanoutSimOutput struct {
	WorkerCount int                    `json:"worker_count"`
	Results     []FanoutWorkloadResult `json:"results"`
	Totals      FanoutWorkloadResult   `json:"totals"`
	Alarms      []diagnostics.Alarm    `json:"alarms"`
	Warnings    []string               `json:"warnings,omitempty"`
}

func ConnectionFanoutSimulatorTool(ctx context.Context, deps Dependencies, in FanoutSimInput) (*mcp.CallToolResult, FanoutSimOutput, error) {
	out := FanoutSimOutput{Results: []FanoutWorkloadResult{}, Alarms: []diagnostics.Alarm{}}
	if len(in.Workloads) == 0 {
		return nil, out, fmt.Errorf("workloads required")
	}
	if in.MaxAdaptivePool == 0 {
		in.MaxAdaptivePool = 16
	}
	if in.CachedConnsPerWorker == 0 {
		in.CachedConnsPerWorker = 1
	}
	if in.MaxSharedPoolSize == 0 {
		in.MaxSharedPoolSize = 100
	}

	workers := in.WorkerCount
	if workers <= 0 && deps.WorkerManager != nil {
		infos, _ := deps.WorkerManager.Topology(ctx)
		workers = len(infos)
	}
	if workers <= 0 {
		workers = 1
	}
	out.WorkerCount = workers

	var totalCoord, totalWorker int
	for _, w := range in.Workloads {
		// Little's law: concurrent_coord_backends = qps × service_time.
		concurrency := w.Concurrency
		if concurrency == 0 {
			// service time includes fan-out round-trip; approximate.
			concurrency = int(w.QpsCoordinator*(w.AvgQueryMs/1000.0) + 0.5)
			if concurrency < 1 && w.QpsCoordinator > 0 {
				concurrency = 1
			}
		}
		// Worker fanout per coord backend:
		fanoutPerCoord := intMin(w.AvgShardsTouched, in.MaxAdaptivePool)
		workerConnsPerNode := (concurrency * fanoutPerCoord) / workers
		if workers > 0 && (concurrency*fanoutPerCoord)%workers != 0 {
			workerConnsPerNode++
		}
		limited := "qps_x_service_time"
		notes := ""
		if w.AvgShardsTouched > in.MaxAdaptivePool {
			limited = "max_adaptive_executor_pool_size"
			notes = fmt.Sprintf("avg_shards_touched=%d capped by max_adaptive_executor_pool_size=%d → queries will serialize some shard access",
				w.AvgShardsTouched, in.MaxAdaptivePool)
		}
		if concurrency*fanoutPerCoord > in.MaxSharedPoolSize {
			limited = "max_shared_pool_size"
			notes = fmt.Sprintf("total predicted connections %d exceeds max_shared_pool_size=%d → throttling expected",
				concurrency*fanoutPerCoord, in.MaxSharedPoolSize)
			if deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "fanout_sim.shared_pool_exceeded", Severity: diagnostics.SeverityWarning,
					Source:  "citus_connection_fanout_simulator",
					Message: fmt.Sprintf("Workload %q predicted %d fan-out connections > max_shared_pool_size %d", w.Name, concurrency*fanoutPerCoord, in.MaxSharedPoolSize),
					Evidence: map[string]any{"workload": w.Name, "predicted": concurrency * fanoutPerCoord, "cap": in.MaxSharedPoolSize},
					FixHint: "Increase citus.max_shared_pool_size or front with pgbouncer (see citus_pooler_advisor).",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
		r := FanoutWorkloadResult{
			Name: w.Name, PredictedCoordBackends: concurrency,
			PredictedWorkerConnsPerNode: workerConnsPerNode,
			LimitedBy: limited, Notes: notes,
		}
		out.Results = append(out.Results, r)
		totalCoord += concurrency
		totalWorker += workerConnsPerNode
	}
	out.Totals = FanoutWorkloadResult{
		Name: "__total__", PredictedCoordBackends: totalCoord,
		PredictedWorkerConnsPerNode: totalWorker, LimitedBy: "sum",
	}
	return nil, out, nil
}
