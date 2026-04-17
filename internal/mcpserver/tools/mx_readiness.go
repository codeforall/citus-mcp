// citus-mcp: B29 citus_mx_readiness -- assesses whether the cluster is
// ready for MX mode operation after adding a new worker. Models:
//
//  mesh_connections_per_node = (N_workers - 1) * max_adaptive_executor_pool_size
//                              + expected_clients_per_node
//  plus coordinator-back-pointers where applicable
//
// and checks each (existing + new) node's max_connections headroom.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/diagnostics/memory"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type MxReadinessInput struct {
	ExpectedClientsPerNode   int `json:"expected_clients_per_node,omitempty"`    // default 100
	TargetNodeRAMGiB         int `json:"target_node_ram_gib,omitempty"`          // for metadata cache fit
	NewNodeMaxConnections    int `json:"new_node_max_connections,omitempty"`     // default 100 (pg default)
	ConcurrentQueriesPerPeer int `json:"concurrent_queries_per_peer,omitempty"`  // default 1; models concurrent multi-shard queries originating at each peer
}

type NodeConnectionBudget struct {
	Node              string `json:"node"`
	MaxConnections    int    `json:"max_connections"`
	MeshPressure      int    `json:"estimated_mesh_pressure"`
	ClientSlots       int    `json:"reserved_client_slots"`
	Headroom          int    `json:"headroom"`
	Verdict           string `json:"verdict"` // ok | tight | overcommitted
}

type MxReadinessOutput struct {
	WorkersBeforeAdd   int                    `json:"workers_before_add"`
	WorkersAfterAdd    int                    `json:"workers_after_add"`
	MaxAdaptivePool    int                    `json:"max_adaptive_executor_pool_size"`
	MaxSharedPool      int                    `json:"max_shared_pool_size"`
	MaxClientConns     int                    `json:"citus_max_client_connections"`
	NodeBudgets        []NodeConnectionBudget `json:"node_budgets"`
	MetadataCacheMiB   int                    `json:"estimated_metadata_cache_mib_per_backend"`
	Alarms             []diagnostics.Alarm    `json:"alarms"`
	Warnings           []string               `json:"warnings,omitempty"`
	Recommendations    []string               `json:"recommendations"`
}

func MxReadinessTool(ctx context.Context, deps Dependencies, in MxReadinessInput) (*mcp.CallToolResult, MxReadinessOutput, error) {
	out := MxReadinessOutput{
		NodeBudgets: []NodeConnectionBudget{}, Alarms: []diagnostics.Alarm{},
		Warnings: []string{}, Recommendations: []string{},
	}
	if in.ExpectedClientsPerNode == 0 {
		in.ExpectedClientsPerNode = 100
	}
	if in.NewNodeMaxConnections == 0 {
		in.NewNodeMaxConnections = 100
	}
	if in.ConcurrentQueriesPerPeer == 0 {
		in.ConcurrentQueriesPerPeer = 1
	}

	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_node WHERE isactive AND noderole='primary' AND shouldhaveshards`).Scan(&out.WorkersBeforeAdd)
	out.WorkersAfterAdd = out.WorkersBeforeAdd + 1

	// Pool-size GUCs on coordinator.
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(current_setting('citus.max_adaptive_executor_pool_size', true),'')::int,16)`).Scan(&out.MaxAdaptivePool)
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(current_setting('citus.max_shared_pool_size', true),'')::int,0)`).Scan(&out.MaxSharedPool)
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(current_setting('citus.max_client_connections', true),'')::int,0)`).Scan(&out.MaxClientConns)

	// Mesh pressure per node:
	//   Inbound connections on a node = sum over every peer p of the
	//   outbound workers-pool connections p opens to this node. Each peer
	//   may run ConcurrentQueriesPerPeer multi-shard queries, each of which
	//   opens up to MaxAdaptivePool connections per target worker. When
	//   citus.max_shared_pool_size is set, it caps the TOTAL outbound
	//   connections from one peer across ALL targets, so the per-target
	//   bound from that peer is max_shared_pool_size / (peer_count - 1).
	//
	//   We include the coordinator itself as a peer (workers receive
	//   executor-pool connections from the coordinator in coord-only and
	//   hybrid deployments).
	peerCount := out.WorkersAfterAdd // = existing workers + new worker; coord adds 1 more
	perQueryFromOnePeer := out.MaxAdaptivePool
	perPeerOutbound := perQueryFromOnePeer * in.ConcurrentQueriesPerPeer
	if out.MaxSharedPool > 0 {
		// shared-pool cap divided across the other N-1 targets; at least 1 conn
		perTargetCap := out.MaxSharedPool / max1(peerCount-1)
		if perTargetCap < perPeerOutbound {
			perPeerOutbound = perTargetCap
		}
	}
	// +1 for coordinator as a peer opening outbound worker-pool conns.
	meshFanIn := perPeerOutbound * (peerCount /* other workers */ + 1 /* coordinator */ - 1 /* don't count self */)
	if meshFanIn < 0 {
		meshFanIn = 0
	}

	// Coordinator budget.
	var cMax int
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&cMax)
	coordBudget := NodeConnectionBudget{
		Node: "coordinator", MaxConnections: cMax,
		MeshPressure: meshFanIn, ClientSlots: in.ExpectedClientsPerNode,
		Headroom: cMax - meshFanIn - in.ExpectedClientsPerNode,
	}
	coordBudget.Verdict = verdict(coordBudget.Headroom, cMax)
	out.NodeBudgets = append(out.NodeBudgets, coordBudget)

	// Per-worker budget using fanout.
	if deps.Fanout != nil {
		results, err := deps.Fanout.OnWorkers(ctx, `SELECT current_setting('max_connections')::int AS max_connections`)
		if err != nil {
			out.Warnings = append(out.Warnings, fmt.Sprintf("fanout query failed: %v", err))
		} else {
			for _, r := range results {
				if !r.Success {
					out.Warnings = append(out.Warnings, fmt.Sprintf("%s:%d query failed: %s", r.NodeName, r.NodePort, r.Error))
					continue
				}
				var wMax int64
				if len(r.Rows) > 0 {
					if val, ok := r.Rows[0]["max_connections"]; ok {
						switch v := val.(type) {
						case float64:
							wMax = int64(v)
						case int64:
							wMax = v
						case int:
							wMax = int64(v)
						}
					}
				}
				b := NodeConnectionBudget{
					Node: fmt.Sprintf("%s:%d", r.NodeName, r.NodePort), MaxConnections: int(wMax),
					MeshPressure: meshFanIn, ClientSlots: in.ExpectedClientsPerNode,
					Headroom: int(wMax) - meshFanIn - in.ExpectedClientsPerNode,
				}
				b.Verdict = verdict(b.Headroom, int(wMax))
				out.NodeBudgets = append(out.NodeBudgets, b)
			}
		}
	}
	// Synthetic target worker — uses user-supplied NewNodeMaxConnections
	// (defaults to the PG default of 100). Users should set this to the
	// planned max_connections of the node being added.
	target := NodeConnectionBudget{
		Node: "<new-node>", MaxConnections: in.NewNodeMaxConnections,
		MeshPressure: meshFanIn, ClientSlots: in.ExpectedClientsPerNode,
		Headroom: in.NewNodeMaxConnections - meshFanIn - in.ExpectedClientsPerNode,
	}
	target.Verdict = verdict(target.Headroom, in.NewNodeMaxConnections)
	out.NodeBudgets = append(out.NodeBudgets, target)

	// Alarms.
	for _, b := range out.NodeBudgets {
		if b.Verdict == "overcommitted" {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "mx.connection_overcommit", Severity: diagnostics.SeverityCritical,
				Source: "citus_mx_readiness",
				Message: fmt.Sprintf("%s: mesh %d + clients %d > max_connections %d",
					b.Node, b.MeshPressure, b.ClientSlots, b.MaxConnections),
				Evidence: map[string]any{"node": b.Node, "max_connections": b.MaxConnections,
					"mesh_pressure": b.MeshPressure, "client_slots": b.ClientSlots},
				FixHint: "Raise max_connections OR put PgBouncer in front OR lower citus.max_adaptive_executor_pool_size. See citus_connection_capacity.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}
	if out.MaxClientConns <= 0 {
		out.Warnings = append(out.Warnings,
			fmt.Sprintf("citus.max_client_connections=%d (disabled) — in MX mode clients can flood an individual worker. Set to a positive value (e.g., max_connections - mesh_pressure - safety_margin) before exposing the worker.", out.MaxClientConns))
	}

	// Metadata cache footprint projection: reuse existing estimator.
	// We feed the coordinator's table shapes; the worker will cache the same.
	shapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err == nil {
		est := memory.EstimateCitusMetadata(shapes)
		out.MetadataCacheMiB = int(est.Bytes >> 20)
		if in.TargetNodeRAMGiB > 0 {
			ramMiB := in.TargetNodeRAMGiB * 1024
			connBudget := 100
			if len(out.NodeBudgets) > 0 {
				connBudget = out.NodeBudgets[len(out.NodeBudgets)-1].MaxConnections
			}
			projectedMiB := out.MetadataCacheMiB * connBudget
			if projectedMiB > ramMiB/4 {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "mx.metadata_cache_tight", Severity: diagnostics.SeverityWarning,
					Source: "citus_mx_readiness",
					Message: fmt.Sprintf("Projected metadata cache %d MiB × %d backends = %d MiB > 25%% target RAM %d MiB",
						out.MetadataCacheMiB, connBudget, projectedMiB, ramMiB),
					Evidence: map[string]any{"per_backend_mib": out.MetadataCacheMiB,
						"max_connections": connBudget, "target_ram_mib": ramMiB},
					FixHint: "Lower max_connections on the new node and front with PgBouncer; or shrink partitions/shards.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	}

	// Recommendations.
	if out.MaxSharedPool == 0 {
		out.Recommendations = append(out.Recommendations,
			"Consider setting citus.max_shared_pool_size to cap outbound connections to workers.")
	}
	if out.MaxClientConns <= 0 {
		out.Recommendations = append(out.Recommendations,
			"Set citus.max_client_connections on every MX node to bound inbound client pressure.")
	}
	return nil, out, nil
}

func verdict(headroom, maxConn int) string {
	if headroom < 0 {
		return "overcommitted"
	}
	if headroom < maxConn/10 {
		return "tight"
	}
	return "ok"
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
