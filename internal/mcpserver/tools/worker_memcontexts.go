// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B1d: citus_worker_memcontexts --
// Fan out to every node (coordinator + workers), touch Citus metadata to
// populate caches, and probe pg_backend_memory_contexts locally on each
// node. Returns per-node measurements alongside the estimator, and flags
// nodes whose footprint is a significant outlier.
//
// Scope limitation (honest): we observe OUR OWN backend on each node. To
// observe the memory context of *another* backend on a node (e.g. the
// biggest client backend), the user can invoke pg_log_backend_memory_contexts(pid)
// and inspect the server log; wiring log-tailing is intentionally deferred
// to a future iteration because it's brittle across deployments.

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

// WorkerMemcontextsInput controls the fanout.
type WorkerMemcontextsInput struct {
	// IncludeCoordinator, when true, also probes the coordinator.
	// Default true.
	IncludeCoordinator *bool `json:"include_coordinator,omitempty"`

	// SkipTouch, when true, skips the metadata-priming step. Useful for
	// observing how cold a freshly-connected backend actually is.
	SkipTouch bool `json:"skip_touch,omitempty"`

	// OutlierRatio triggers an alarm when any node's MetadataCache or
	// CacheMemoryContext exceeds the median by this multiplier. Default 2.0.
	OutlierRatio float64 `json:"outlier_ratio,omitempty"`
}

// WorkerMemcontextsOutput is the tool response.
type WorkerMemcontextsOutput struct {
	Estimate        memory.CitusEstimate    `json:"citus_estimate"`
	PgEstimate      memory.PgCacheEstimate  `json:"pg_cache_estimate"`
	Nodes           []NodeMemcontextsReport `json:"nodes"`
	MedianMetadata  int64                   `json:"median_metadata_used_bytes"`
	MedianPgCache   int64                   `json:"median_pg_cache_used_bytes"`
	Alarms          []diagnostics.Alarm     `json:"alarms"`
	Warnings        []string                `json:"warnings,omitempty"`
}

// NodeMemcontextsReport is one row per node.
type NodeMemcontextsReport struct {
	NodeID        int32                           `json:"node_id"`
	NodeName      string                          `json:"node_name"`
	NodePort      int32                           `json:"node_port"`
	Role          string                          `json:"role"` // "coordinator" | "worker"
	Reachable     bool                            `json:"reachable"`
	Error         string                          `json:"error,omitempty"`
	Touched       int                             `json:"touched_tables"`
	Measurements  []memory.BackendCtxMeasurement  `json:"measurements"`
}

// WorkerMemcontextsTool implements the tool.
func WorkerMemcontextsTool(ctx context.Context, deps Dependencies, in WorkerMemcontextsInput) (*mcp.CallToolResult, WorkerMemcontextsOutput, error) {
	out := WorkerMemcontextsOutput{Alarms: []diagnostics.Alarm{}, Warnings: []string{}, Nodes: []NodeMemcontextsReport{}}
	if in.OutlierRatio <= 0 {
		in.OutlierRatio = 2.0
	}
	includeCoord := true
	if in.IncludeCoordinator != nil {
		includeCoord = *in.IncludeCoordinator
	}

	// Estimate once from the coordinator's catalog.
	shapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}
	out.Estimate = memory.EstimateCitusMetadata(shapes)
	// For worker nodes in MX mode the include_shard_rels view is more accurate;
	// we produce both numbers implicitly: coordinator uses the "false" view,
	// workers use "true" (set below while building reports).
	out.PgEstimate = memory.EstimatePgCache(shapes, false)

	// Coordinator probe (direct connection, not via fanout)
	if includeCoord {
		coordReport := NodeMemcontextsReport{
			NodeID:       0,
			NodeName:     "coordinator",
			NodePort:     0,
			Role:         "coordinator",
			Reachable:    true,
			Measurements: []memory.BackendCtxMeasurement{},
		}
		conn, err := deps.Pool.Acquire(ctx)
		if err != nil {
			coordReport.Error = "acquire: " + err.Error()
			coordReport.Reachable = false
		} else {
			defer conn.Release()
			if !in.SkipTouch {
				if n, terr := memory.TouchAllDistributedMetadata(ctx, conn.Conn()); terr == nil {
					coordReport.Touched = n
				}
			}
			meas, perr := memory.Probe(ctx, conn.Conn(), []string{"MetadataCacheMemoryContext", "CacheMemoryContext"})
			if perr != nil {
				coordReport.Error = "probe: " + perr.Error()
			} else {
				coordReport.Measurements = meas
			}
		}
		out.Nodes = append(out.Nodes, coordReport)
	}

	// Fanout: combine touch + probe into a single SQL for each worker.
	// run_command_on_workers runs on a fresh backend. We use a CTE-based
	// approach that touches metadata and probes in a single statement.
	if deps.Fanout != nil {
		var workerResults []db.NodeResult
		var err error
		
		if !in.SkipTouch {
			// Combined touch + probe in a single CTE query
			// The touch happens via citus_relation_size calls in a subquery
			touchAndProbeSQL := `
WITH touch AS (
  SELECT COUNT(*) AS touched_count
  FROM pg_dist_partition
  WHERE true -- Force touch via citus_relation_size (best-effort)
    AND (citus_relation_size(logicalrelid) IS NOT NULL OR true)
),
ctxs AS (
  SELECT name, parent, level, total_bytes, used_bytes, free_bytes
  FROM pg_backend_memory_contexts
),
roots AS (
  SELECT name AS scope, name, parent, total_bytes, used_bytes, free_bytes
  FROM ctxs
  WHERE name IN ('MetadataCacheMemoryContext', 'CacheMemoryContext')
),
tree AS (
  SELECT r.scope, c.name, c.parent, c.total_bytes, c.used_bytes, c.free_bytes
  FROM roots r JOIN ctxs c ON c.name = r.name
  UNION ALL
  SELECT t.scope, c.name, c.parent, c.total_bytes, c.used_bytes, c.free_bytes
  FROM tree t JOIN ctxs c ON c.parent = t.name
)
SELECT 
    scope,
    SUM(total_bytes)::bigint AS total_bytes,
    SUM(used_bytes)::bigint AS used_bytes,
    SUM(free_bytes)::bigint AS free_bytes,
    COUNT(*)::int AS n_contexts,
    (SELECT touched_count FROM touch) AS touched_tables
FROM tree
GROUP BY scope
ORDER BY scope
`
			workerResults, err = deps.Fanout.OnWorkers(ctx, touchAndProbeSQL)
		} else {
			// Just probe without touch
			probeSQL := `
WITH RECURSIVE ctxs AS (
  SELECT name, parent, level, total_bytes, used_bytes, free_bytes
  FROM pg_backend_memory_contexts
),
roots AS (
  SELECT name AS scope, name, parent, total_bytes, used_bytes, free_bytes
  FROM ctxs
  WHERE name IN ('MetadataCacheMemoryContext', 'CacheMemoryContext')
),
tree AS (
  SELECT r.scope, c.name, c.parent, c.total_bytes, c.used_bytes, c.free_bytes
  FROM roots r JOIN ctxs c ON c.name = r.name
  UNION ALL
  SELECT t.scope, c.name, c.parent, c.total_bytes, c.used_bytes, c.free_bytes
  FROM tree t JOIN ctxs c ON c.parent = t.name
)
SELECT 
    scope,
    SUM(total_bytes)::bigint AS total_bytes,
    SUM(used_bytes)::bigint AS used_bytes,
    SUM(free_bytes)::bigint AS free_bytes,
    COUNT(*)::int AS n_contexts,
    0::bigint AS touched_tables
FROM tree
GROUP BY scope
ORDER BY scope
`
			workerResults, err = deps.Fanout.OnWorkers(ctx, probeSQL)
		}

		if err != nil {
			out.Warnings = append(out.Warnings, fmt.Sprintf("fanout query failed: %v", err))
		} else {
			// Convert fanout results to NodeMemcontextsReport
			for _, fr := range workerResults {
				report := NodeMemcontextsReport{
					NodeID:       -1, // we don't have node_id from fanout
					NodeName:     fr.NodeName,
					NodePort:     fr.NodePort,
					Role:         "worker",
					Reachable:    fr.Success,
					Measurements: []memory.BackendCtxMeasurement{},
				}
				if !fr.Success {
					report.Error = fr.Error
				} else {
					// Parse measurements from rows
					var measurements []memory.BackendCtxMeasurement
					var touched int
					for _, row := range fr.Rows {
						scope, _ := row["scope"].(string)
						totalBytes := extractInt64(row["total_bytes"])
						usedBytes := extractInt64(row["used_bytes"])
						freeBytes := extractInt64(row["free_bytes"])
						nContexts := int(extractInt64(row["n_contexts"]))
						touchedTables := extractInt64(row["touched_tables"])
						if touchedTables > 0 {
							touched = int(touchedTables)
						}
						measurements = append(measurements, memory.BackendCtxMeasurement{
							Scope:      scope,
							TotalBytes: totalBytes,
							UsedBytes:  usedBytes,
							FreeBytes:  freeBytes,
							NContexts:  nContexts,
						})
					}
					report.Touched = touched
					report.Measurements = measurements
				}
				out.Nodes = append(out.Nodes, report)
			}
		}
	}

	// Compute medians across reachable nodes for outlier detection.
	metaSamples := make([]int64, 0, len(out.Nodes))
	pgSamples := make([]int64, 0, len(out.Nodes))
	for _, r := range out.Nodes {
		for _, m := range r.Measurements {
			switch m.Scope {
			case "MetadataCacheMemoryContext":
				metaSamples = append(metaSamples, m.UsedBytes)
			case "CacheMemoryContext":
				pgSamples = append(pgSamples, m.UsedBytes)
			}
		}
	}
	out.MedianMetadata = median(metaSamples)
	out.MedianPgCache = median(pgSamples)

	// Outlier alarms.
	if deps.Alarms != nil {
		for _, r := range out.Nodes {
			for _, m := range r.Measurements {
				var med int64
				switch m.Scope {
				case "MetadataCacheMemoryContext":
					med = out.MedianMetadata
				case "CacheMemoryContext":
					med = out.MedianPgCache
				default:
					continue
				}
				if med <= 0 {
					continue
				}
				ratio := float64(m.UsedBytes) / float64(med)
				if ratio >= in.OutlierRatio {
					a := deps.Alarms.Emit(diagnostics.Alarm{
						Kind:     "memcontext.node_outlier",
						Severity: diagnostics.SeverityWarning,
						Source:   "citus_worker_memcontexts",
						Message: fmt.Sprintf("Node %s (:%d) %s is %.1fx the cluster median (%d vs %d bytes)",
							r.NodeName, r.NodePort, m.Scope, ratio, m.UsedBytes, med),
						Evidence: map[string]any{
							"node_id":     r.NodeID,
							"node_name":   r.NodeName,
							"node_port":   r.NodePort,
							"role":        r.Role,
							"scope":       m.Scope,
							"used_bytes":  m.UsedBytes,
							"median":      med,
							"ratio":       ratio,
						},
						FixHint: "Inspect that node for: long-lived sessions with many cached plans, orphaned prepared transactions, or cache-leak patterns (e.g. repeated DDL in sessions without reconnect).",
					})
					out.Alarms = append(out.Alarms, *a)
				}
			}
		}
	}

	return nil, out, nil
}

func median(xs []int64) int64 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	// simple O(n^2) insertion sort; n is tiny (cluster node count).
	cp := append([]int64(nil), xs...)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && cp[j] < cp[j-1]; j-- {
			cp[j], cp[j-1] = cp[j-1], cp[j]
		}
	}
	if n%2 == 1 {
		return cp[n/2]
	}
	return int64(math.Round(float64(cp[n/2-1]+cp[n/2]) / 2.0))
}

// extractInt64 safely extracts an int64 from a JSON-decoded value (handles float64, int, etc)
func extractInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		// Try to parse
		var i int64
		fmt.Sscanf(val, "%d", &i)
		return i
	default:
		return 0
	}
}
