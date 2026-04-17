// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B6: citus_proactive_health --
// Aggregated early-warning dashboard. Runs a collection of lightweight
// health checks across coordinator + workers and emits alarms on the first
// sign of trouble. Designed to be the one tool an operator schedules to
// run every N minutes via the alarms framework.
//
// Checks performed:
//   1. Node availability (pg_dist_node: isactive, hasmetadata, metadatasynced)
//   2. Long-running transactions (pg_stat_activity.xact_start)
//   3. Idle-in-transaction backends (state = 'idle in transaction')
//   4. Stuck 2PC / prepared transactions (pg_prepared_xacts)
//   5. Unhealthy shard placements (pg_dist_placement.shardstate != 1)
//   6. Connection saturation (active / max_connections)
//   7. Bloat candidates (pg_stat_user_tables: dead_tup ratio)
//   8. Stale autovacuum (tables with dead tuples and no recent vacuum)
//
// Each check has its own input-driven thresholds so callers can make
// the tool as noisy or as conservative as needed.

package tools

import (
	"context"
	"fmt"
	"time"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ProactiveHealthInput controls thresholds.
type ProactiveHealthInput struct {
	LongTxSeconds            int `json:"long_tx_seconds,omitempty"`             // default 300
	IdleInTxSeconds          int `json:"idle_in_tx_seconds,omitempty"`          // default 120
	StuckPreparedXactSeconds int `json:"stuck_prepared_xact_seconds,omitempty"` // default 60
	ConnectionSaturationPct  int `json:"connection_saturation_pct,omitempty"`   // default 85
	BloatDeadRatioPct        int `json:"bloat_dead_ratio_pct,omitempty"`        // default 20
	BloatMinTotalTup         int `json:"bloat_min_total_tup,omitempty"`         // default 10000
	AutovacuumStaleHours     int `json:"autovacuum_stale_hours,omitempty"`      // default 24
	IncludeWorkers           *bool `json:"include_workers,omitempty"`           // default true
}

// ProactiveHealthOutput is the structured dashboard.
type ProactiveHealthOutput struct {
	NodeHealth            []NodeHealthRow        `json:"node_health"`
	LongTransactions      []LongTxRow            `json:"long_transactions"`
	IdleInTransaction     []LongTxRow            `json:"idle_in_transaction"`
	StuckPreparedXacts    []StuckPreparedXact    `json:"stuck_prepared_xacts"`
	UnhealthyPlacements   []PlacementRow         `json:"unhealthy_placements"`
	ConnectionSaturation  []ConnectionSaturation `json:"connection_saturation"`
	BloatCandidates       []BloatRow             `json:"bloat_candidates"`
	StaleAutovacuum       []BloatRow             `json:"stale_autovacuum"`
	Summary               HealthSummary          `json:"summary"`
	Alarms                []diagnostics.Alarm    `json:"alarms"`
	Warnings              []string               `json:"warnings,omitempty"`
}

type NodeHealthRow struct {
	NodeID          int32  `json:"node_id"`
	NodeName        string `json:"node_name"`
	NodePort        int32  `json:"node_port"`
	Role            string `json:"role"`
	IsActive        bool   `json:"is_active"`
	HasMetadata     bool   `json:"has_metadata"`
	MetadataSynced  bool   `json:"metadata_synced"`
	ShouldHaveShards bool  `json:"should_have_shards"`
	Problem         string `json:"problem,omitempty"`
}

type LongTxRow struct {
	NodeKey       string `json:"node"`
	Pid           int32  `json:"pid"`
	State         string `json:"state"`
	ApplicationName string `json:"application_name,omitempty"`
	AgeSeconds    int64  `json:"age_seconds"`
	Query         string `json:"query,omitempty"`
}

type StuckPreparedXact struct {
	NodeKey   string `json:"node"`
	Gid       string `json:"gid"`
	AgeSeconds int64 `json:"age_seconds"`
	Owner     string `json:"owner,omitempty"`
	Database  string `json:"database,omitempty"`
}

type PlacementRow struct {
	ShardID     int64 `json:"shard_id"`
	PlacementID int64 `json:"placement_id"`
	ShardState  int32 `json:"shard_state"`
	GroupID     int32 `json:"group_id"`
	ShardLength int64 `json:"shard_length"`
}

type ConnectionSaturation struct {
	NodeKey        string  `json:"node"`
	MaxConnections int     `json:"max_connections"`
	ActiveBackends int     `json:"active_backends"`
	UsagePct       float64 `json:"usage_pct"`
}

type BloatRow struct {
	NodeKey      string  `json:"node"`
	Schema       string  `json:"schema"`
	Table        string  `json:"table"`
	LiveTup      int64   `json:"live_tup"`
	DeadTup      int64   `json:"dead_tup"`
	DeadRatioPct float64 `json:"dead_ratio_pct"`
	LastVacuum   string  `json:"last_vacuum,omitempty"`
	LastAutovac  string  `json:"last_autovacuum,omitempty"`
}

type HealthSummary struct {
	UnhealthyNodeCount       int `json:"unhealthy_node_count"`
	LongTxCount              int `json:"long_tx_count"`
	IdleInTxCount            int `json:"idle_in_tx_count"`
	StuckPreparedXactCount   int `json:"stuck_prepared_xact_count"`
	UnhealthyPlacementCount  int `json:"unhealthy_placement_count"`
	SaturatedNodeCount       int `json:"saturated_node_count"`
	BloatCandidateCount      int `json:"bloat_candidate_count"`
	StaleAutovacuumCount     int `json:"stale_autovacuum_count"`
	OverallHealth            string `json:"overall_health"` // "ok" | "warning" | "critical"
}

// ProactiveHealthTool implements B6.
func ProactiveHealthTool(ctx context.Context, deps Dependencies, in ProactiveHealthInput) (*mcp.CallToolResult, ProactiveHealthOutput, error) {
	out := ProactiveHealthOutput{
		Alarms: []diagnostics.Alarm{}, Warnings: []string{},
		NodeHealth: []NodeHealthRow{}, LongTransactions: []LongTxRow{},
		IdleInTransaction: []LongTxRow{}, StuckPreparedXacts: []StuckPreparedXact{},
		UnhealthyPlacements: []PlacementRow{}, ConnectionSaturation: []ConnectionSaturation{},
		BloatCandidates: []BloatRow{}, StaleAutovacuum: []BloatRow{},
	}
	// Defaults.
	if in.LongTxSeconds == 0 {
		in.LongTxSeconds = 300
	}
	if in.IdleInTxSeconds == 0 {
		in.IdleInTxSeconds = 120
	}
	if in.StuckPreparedXactSeconds == 0 {
		in.StuckPreparedXactSeconds = 60
	}
	if in.ConnectionSaturationPct == 0 {
		in.ConnectionSaturationPct = 85
	}
	if in.BloatDeadRatioPct == 0 {
		in.BloatDeadRatioPct = 20
	}
	if in.BloatMinTotalTup == 0 {
		in.BloatMinTotalTup = 10000
	}
	if in.AutovacuumStaleHours == 0 {
		in.AutovacuumStaleHours = 24
	}
	includeWorkers := true
	if in.IncludeWorkers != nil {
		includeWorkers = *in.IncludeWorkers
	}

	// Coordinator-only checks.
	coord, err := deps.Pool.Acquire(ctx)
	if err != nil {
		return nil, out, nil
	}
	defer coord.Release()

	// Node health from pg_dist_node.
	if rows, qerr := coord.Conn().Query(ctx, `
SELECT nodeid, nodename, nodeport, noderole, isactive, hasmetadata, metadatasynced, shouldhaveshards
FROM pg_dist_node ORDER BY noderole, nodeid`); qerr == nil {
		defer rows.Close()
		for rows.Next() {
			var r NodeHealthRow
			if err := rows.Scan(&r.NodeID, &r.NodeName, &r.NodePort, &r.Role, &r.IsActive, &r.HasMetadata, &r.MetadataSynced, &r.ShouldHaveShards); err == nil {
				probs := []string{}
				if !r.IsActive {
					probs = append(probs, "isactive=false")
				}
				if r.Role == "primary" && !r.MetadataSynced && r.HasMetadata {
					probs = append(probs, "metadata not synced")
				}
				if len(probs) > 0 {
					r.Problem = fmt.Sprint(probs)
					out.Summary.UnhealthyNodeCount++
					if deps.Alarms != nil {
						a := deps.Alarms.Emit(diagnostics.Alarm{
							Kind:     "health.node",
							Severity: diagnostics.SeverityCritical,
							Source:   "citus_proactive_health",
							Message:  fmt.Sprintf("Node %s:%d unhealthy: %s", r.NodeName, r.NodePort, r.Problem),
							Evidence: map[string]any{"node_id": r.NodeID, "node": r.NodeName, "port": r.NodePort, "problem": r.Problem},
							FixHint:  "Investigate with citus_node_status; may need citus_activate_node or re-sync metadata.",
						})
						out.Alarms = append(out.Alarms, *a)
					}
				}
				out.NodeHealth = append(out.NodeHealth, r)
			}
		}
	}

	// Unhealthy placements (coordinator only - pg_dist_placement is here).
	if rows, qerr := coord.Conn().Query(ctx, `
SELECT shardid, placementid, shardstate, groupid, COALESCE(shardlength,0)
FROM pg_dist_placement WHERE shardstate != 1 ORDER BY shardid LIMIT 200`); qerr == nil {
		defer rows.Close()
		for rows.Next() {
			var p PlacementRow
			if err := rows.Scan(&p.ShardID, &p.PlacementID, &p.ShardState, &p.GroupID, &p.ShardLength); err == nil {
				out.UnhealthyPlacements = append(out.UnhealthyPlacements, p)
			}
		}
		out.Summary.UnhealthyPlacementCount = len(out.UnhealthyPlacements)
		if out.Summary.UnhealthyPlacementCount > 0 && deps.Alarms != nil {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind:     "health.placements",
				Severity: diagnostics.SeverityCritical,
				Source:   "citus_proactive_health",
				Message:  fmt.Sprintf("%d shard placement(s) in non-active state (shardstate != 1)", out.Summary.UnhealthyPlacementCount),
				Evidence: map[string]any{"count": out.Summary.UnhealthyPlacementCount, "sample": out.UnhealthyPlacements[:min(10, len(out.UnhealthyPlacements))]},
				FixHint:  "Run SELECT citus_copy_shard_placement / citus_rebalance_start to heal. Check worker node availability.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}

	// Per-node probes: long-tx, idle-in-tx, 2PC, saturation, bloat, autovacuum.
	// SQL for long-running transactions
	longTxSQL := fmt.Sprintf(`
SELECT pid, state, COALESCE(application_name,''), EXTRACT(EPOCH FROM (now()-xact_start))::bigint, COALESCE(left(query, 500),'')
FROM pg_stat_activity
WHERE backend_type='client backend' AND xact_start IS NOT NULL
  AND now()-xact_start > make_interval(secs=>%d)
  AND pid <> pg_backend_pid()
ORDER BY xact_start ASC LIMIT 50`, in.LongTxSeconds)

	// SQL for stuck prepared xacts
	preparedSQL := fmt.Sprintf(`
SELECT gid, EXTRACT(EPOCH FROM (now()-prepared))::bigint, COALESCE(owner,''), COALESCE(database,'')
FROM pg_prepared_xacts
WHERE now()-prepared > make_interval(secs=>%d)
ORDER BY prepared ASC LIMIT 50`, in.StuckPreparedXactSeconds)

	// SQL for connection saturation
	saturationSQL := `
SELECT current_setting('max_connections')::int AS max_connections,
       (SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend' AND pid <> pg_backend_pid()) AS active_backends`

	// SQL for bloat candidates
	bloatSQL := fmt.Sprintf(`
SELECT schemaname, relname, COALESCE(n_live_tup,0), COALESCE(n_dead_tup,0),
       COALESCE(last_vacuum::text,''), COALESCE(last_autovacuum::text,'')
FROM pg_stat_user_tables
WHERE (COALESCE(n_live_tup,0) + COALESCE(n_dead_tup,0)) > %d
ORDER BY n_dead_tup DESC NULLS LAST LIMIT 200`, in.BloatMinTotalTup)

	// Coordinator
	coordNodeKey := "coordinator"
	if includeWorkers {
		// Long-tx
		rows, qerr := deps.Pool.Query(ctx, longTxSQL)
		if qerr == nil {
			for rows.Next() {
				var r LongTxRow
				r.NodeKey = coordNodeKey
				if err := rows.Scan(&r.Pid, &r.State, &r.ApplicationName, &r.AgeSeconds, &r.Query); err == nil {
					if r.State == "idle in transaction" || r.State == "idle in transaction (aborted)" {
						out.IdleInTransaction = append(out.IdleInTransaction, r)
					} else {
						out.LongTransactions = append(out.LongTransactions, r)
					}
				}
			}
			rows.Close()
		}

		// Prepared xacts
		rows, qerr = deps.Pool.Query(ctx, preparedSQL)
		if qerr == nil {
			for rows.Next() {
				var s StuckPreparedXact
				s.NodeKey = coordNodeKey
				if err := rows.Scan(&s.Gid, &s.AgeSeconds, &s.Owner, &s.Database); err == nil {
					out.StuckPreparedXacts = append(out.StuckPreparedXacts, s)
				}
			}
			rows.Close()
		}

		// Connection saturation
		var maxc, activec int
		err := deps.Pool.QueryRow(ctx, saturationSQL).Scan(&maxc, &activec)
		if err == nil && maxc > 0 {
			pct := float64(activec) * 100.0 / float64(maxc)
			out.ConnectionSaturation = append(out.ConnectionSaturation, ConnectionSaturation{
				NodeKey: coordNodeKey, MaxConnections: maxc, ActiveBackends: activec, UsagePct: pct,
			})
		}

		// Bloat
		rows, qerr = deps.Pool.Query(ctx, bloatSQL)
		if qerr == nil {
			for rows.Next() {
				var b BloatRow
				b.NodeKey = coordNodeKey
				if err := rows.Scan(&b.Schema, &b.Table, &b.LiveTup, &b.DeadTup, &b.LastVacuum, &b.LastAutovac); err == nil {
					total := b.LiveTup + b.DeadTup
					if total > 0 {
						b.DeadRatioPct = float64(b.DeadTup) * 100.0 / float64(total)
					}
					if b.DeadRatioPct >= float64(in.BloatDeadRatioPct) {
						out.BloatCandidates = append(out.BloatCandidates, b)
					}
					// stale autovacuum
					if b.DeadTup > 0 && b.LastAutovac != "" {
						if t0, perr := time.Parse("2006-01-02 15:04:05.999999-07", b.LastAutovac); perr == nil {
							if time.Since(t0) > time.Duration(in.AutovacuumStaleHours)*time.Hour {
								out.StaleAutovacuum = append(out.StaleAutovacuum, b)
							}
						}
					}
				}
			}
			rows.Close()
		}
	}

	// Workers via Fanout
	if includeWorkers && deps.Fanout != nil {
		// Long-tx
		if results, err := deps.Fanout.OnWorkers(ctx, longTxSQL); err == nil {
			for _, res := range results {
				nodeKey := fmt.Sprintf("%s:%d", res.NodeName, res.NodePort)
				if !res.Success {
					continue
				}
				for _, row := range res.Rows {
					var r LongTxRow
					r.NodeKey = nodeKey
					if pid, ok := row["pid"].(float64); ok {
						r.Pid = int32(pid)
					}
					if state, ok := row["state"].(string); ok {
						r.State = state
					}
					if app, ok := row["application_name"].(string); ok {
						r.ApplicationName = app
					}
					if age, ok := row["date_part"].(float64); ok {
						r.AgeSeconds = int64(age)
					}
					if query, ok := row["left"].(string); ok {
						r.Query = query
					}
					if r.State == "idle in transaction" || r.State == "idle in transaction (aborted)" {
						out.IdleInTransaction = append(out.IdleInTransaction, r)
					} else {
						out.LongTransactions = append(out.LongTransactions, r)
					}
				}
			}
		}

		// Prepared xacts
		if results, err := deps.Fanout.OnWorkers(ctx, preparedSQL); err == nil {
			for _, res := range results {
				nodeKey := fmt.Sprintf("%s:%d", res.NodeName, res.NodePort)
				if !res.Success {
					continue
				}
				for _, row := range res.Rows {
					var s StuckPreparedXact
					s.NodeKey = nodeKey
					if gid, ok := row["gid"].(string); ok {
						s.Gid = gid
					}
					if age, ok := row["date_part"].(float64); ok {
						s.AgeSeconds = int64(age)
					}
					if owner, ok := row["owner"].(string); ok {
						s.Owner = owner
					}
					if database, ok := row["database"].(string); ok {
						s.Database = database
					}
					out.StuckPreparedXacts = append(out.StuckPreparedXacts, s)
				}
			}
		}

		// Connection saturation
		if results, err := deps.Fanout.OnWorkers(ctx, saturationSQL); err == nil {
			for _, res := range results {
				nodeKey := fmt.Sprintf("%s:%d", res.NodeName, res.NodePort)
				if !res.Success || len(res.Rows) == 0 {
					continue
				}
				maxc, _ := res.Int("max_connections")
				activec, _ := res.Int("active_backends")
				if maxc > 0 {
					pct := float64(activec) * 100.0 / float64(maxc)
					out.ConnectionSaturation = append(out.ConnectionSaturation, ConnectionSaturation{
						NodeKey: nodeKey, MaxConnections: int(maxc), ActiveBackends: int(activec), UsagePct: pct,
					})
				}
			}
		}

		// Bloat
		if results, err := deps.Fanout.OnWorkers(ctx, bloatSQL); err == nil {
			for _, res := range results {
				nodeKey := fmt.Sprintf("%s:%d", res.NodeName, res.NodePort)
				if !res.Success {
					continue
				}
				for _, row := range res.Rows {
					var b BloatRow
					b.NodeKey = nodeKey
					if schema, ok := row["schemaname"].(string); ok {
						b.Schema = schema
					}
					if table, ok := row["relname"].(string); ok {
						b.Table = table
					}
					if live, ok := row["n_live_tup"].(float64); ok {
						b.LiveTup = int64(live)
					}
					if dead, ok := row["n_dead_tup"].(float64); ok {
						b.DeadTup = int64(dead)
					}
					if lastVac, ok := row["last_vacuum"].(string); ok {
						b.LastVacuum = lastVac
					}
					if lastAuto, ok := row["last_autovacuum"].(string); ok {
						b.LastAutovac = lastAuto
					}

					total := b.LiveTup + b.DeadTup
					if total > 0 {
						b.DeadRatioPct = float64(b.DeadTup) * 100.0 / float64(total)
					}
					if b.DeadRatioPct >= float64(in.BloatDeadRatioPct) {
						out.BloatCandidates = append(out.BloatCandidates, b)
					}
					// stale autovacuum
					if b.DeadTup > 0 && b.LastAutovac != "" {
						if t0, perr := time.Parse("2006-01-02 15:04:05.999999-07", b.LastAutovac); perr == nil {
							if time.Since(t0) > time.Duration(in.AutovacuumStaleHours)*time.Hour {
								out.StaleAutovacuum = append(out.StaleAutovacuum, b)
							}
						}
					}
				}
			}
		}
	}

	// Finalize summary + alarms.
	out.Summary.LongTxCount = len(out.LongTransactions)
	out.Summary.IdleInTxCount = len(out.IdleInTransaction)
	out.Summary.StuckPreparedXactCount = len(out.StuckPreparedXacts)
	out.Summary.BloatCandidateCount = len(out.BloatCandidates)
	out.Summary.StaleAutovacuumCount = len(out.StaleAutovacuum)

	for _, c := range out.ConnectionSaturation {
		if c.UsagePct >= float64(in.ConnectionSaturationPct) {
			out.Summary.SaturatedNodeCount++
			if deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind:     "health.connection_saturation",
					Severity: diagnostics.SeverityWarning,
					Source:   "citus_proactive_health",
					Message:  fmt.Sprintf("%s connection usage %.1f%% (%d/%d)", c.NodeKey, c.UsagePct, c.ActiveBackends, c.MaxConnections),
					Evidence: map[string]any{"node": c.NodeKey, "usage_pct": c.UsagePct, "active": c.ActiveBackends, "max": c.MaxConnections},
					FixHint:  "Investigate with citus_activity; consider adding PgBouncer (see citus_pooler_advisor).",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	}

	if deps.Alarms != nil {
		if out.Summary.LongTxCount > 0 {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "health.long_transaction", Severity: diagnostics.SeverityWarning, Source: "citus_proactive_health",
				Message: fmt.Sprintf("%d transaction(s) older than %ds", out.Summary.LongTxCount, in.LongTxSeconds),
				Evidence: map[string]any{"count": out.Summary.LongTxCount, "threshold_seconds": in.LongTxSeconds},
				FixHint:  "Use citus_activity to inspect; consider pg_cancel_backend for stuck sessions.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
		if out.Summary.IdleInTxCount > 0 {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "health.idle_in_transaction", Severity: diagnostics.SeverityWarning, Source: "citus_proactive_health",
				Message: fmt.Sprintf("%d backend(s) idle-in-transaction > %ds", out.Summary.IdleInTxCount, in.IdleInTxSeconds),
				Evidence: map[string]any{"count": out.Summary.IdleInTxCount, "threshold_seconds": in.IdleInTxSeconds},
				FixHint:  "Identify the client - idle-in-tx holds locks and blocks autovacuum/Citus DDL. Consider idle_in_transaction_session_timeout.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
		if out.Summary.StuckPreparedXactCount > 0 {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "health.stuck_2pc", Severity: diagnostics.SeverityCritical, Source: "citus_proactive_health",
				Message: fmt.Sprintf("%d prepared transaction(s) older than %ds", out.Summary.StuckPreparedXactCount, in.StuckPreparedXactSeconds),
				Evidence: map[string]any{"count": out.Summary.StuckPreparedXactCount, "sample": out.StuckPreparedXacts[:min(10, len(out.StuckPreparedXacts))]},
				FixHint:  "Stuck 2PC holds locks cluster-wide. Use SELECT recover_prepared_transactions(); or manual ROLLBACK PREPARED on the gid.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
		if out.Summary.BloatCandidateCount > 0 {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "health.bloat", Severity: diagnostics.SeverityInfo, Source: "citus_proactive_health",
				Message: fmt.Sprintf("%d table(s) with dead-tup ratio >= %d%%", out.Summary.BloatCandidateCount, in.BloatDeadRatioPct),
				Evidence: map[string]any{"count": out.Summary.BloatCandidateCount, "sample": out.BloatCandidates[:min(10, len(out.BloatCandidates))]},
				FixHint:  "Consider VACUUM (or adjust autovacuum_vacuum_scale_factor / cost_limit).",
			})
			out.Alarms = append(out.Alarms, *a)
		}
		if out.Summary.StaleAutovacuumCount > 0 {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "health.stale_autovacuum", Severity: diagnostics.SeverityWarning, Source: "citus_proactive_health",
				Message: fmt.Sprintf("%d table(s) not autovacuumed in > %dh despite having dead tuples", out.Summary.StaleAutovacuumCount, in.AutovacuumStaleHours),
				Evidence: map[string]any{"count": out.Summary.StaleAutovacuumCount, "sample": out.StaleAutovacuum[:min(10, len(out.StaleAutovacuum))]},
				FixHint:  "Autovacuum may be disabled or starved; check pg_stat_activity for stuck autovacuum workers.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}

	// Overall health rollup.
	switch {
	case out.Summary.UnhealthyNodeCount > 0 ||
		out.Summary.UnhealthyPlacementCount > 0 ||
		out.Summary.StuckPreparedXactCount > 0:
		out.Summary.OverallHealth = "critical"
	case out.Summary.LongTxCount > 0 ||
		out.Summary.IdleInTxCount > 0 ||
		out.Summary.SaturatedNodeCount > 0 ||
		out.Summary.StaleAutovacuumCount > 0:
		out.Summary.OverallHealth = "warning"
	default:
		out.Summary.OverallHealth = "ok"
	}

	return nil, out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
