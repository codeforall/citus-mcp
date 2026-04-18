// citus-mcp: citus_2pc_recovery_inspector
//
// Inspects prepared (two-phase-commit) transactions on coordinator + workers,
// correlates them with pg_dist_transaction, classifies each one, and emits
// a safe recovery plan with concrete COMMIT PREPARED / ROLLBACK PREPARED
// statements.
//
// Mirrors the logic in Citus's own RecoverWorkerTransactions
// (src/backend/distributed/transaction/transaction_recovery.c):
//
//   - GID format:
//       citus_<initiatorGroupId>_<pid>_<transactionNumber>_<connectionNumber>
//   - Recovery rule (per node):
//       * If (node_group_id, gid) is present in coord's pg_dist_transaction
//         → the distributed txn committed; issue COMMIT PREPARED <gid> here.
//       * Else if transactionNumber is still among
//         get_all_active_transactions() → still in flight; leave alone.
//       * Else → orphan; issue ROLLBACK PREPARED <gid> here.
//
// This tool is strictly read-only: it never issues COMMIT/ROLLBACK PREPARED
// itself. Calling `recover_prepared_transactions()` remains the user's
// responsibility.

package tools

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type TwoPCRecoveryInput struct {
	// Prepared xacts younger than this are classified as in_flight regardless
	// of dist-transaction tracking (they may still be mid-2PC). Default 60s.
	InFlightThresholdSeconds int `json:"in_flight_threshold_seconds,omitempty"`
	// Include prepared xacts with non-Citus GIDs in the output as
	// classification=non_citus. Default false (filtered out).
	IncludeNonCitus bool `json:"include_non_citus,omitempty"`
	// Emit a multi-node SQL recovery script in the output. Default: emitted.
	// Set to true to suppress.
	SuppressRecoveryScript bool `json:"suppress_recovery_script,omitempty"`
	// Age above which an orphan is escalated to a critical alarm. Default 600s.
	StuckOrphanSeconds int `json:"stuck_orphan_seconds,omitempty"`
}

type PreparedXactRow struct {
	Node               string  `json:"node"` // "coordinator" or "host:port"
	NodeGroupID        int32   `json:"node_group_id"`
	GID                string  `json:"gid"`
	IsCitusGID         bool    `json:"is_citus_gid"`
	InitiatorGroupID   int32   `json:"initiator_group_id,omitempty"`
	InitiatorPID       int32   `json:"initiator_pid,omitempty"`
	TransactionNumber  uint64  `json:"transaction_number,omitempty"`
	ConnectionNumber   uint32  `json:"connection_number,omitempty"`
	Database           string  `json:"database"`
	Owner              string  `json:"owner"`
	PreparedAt         string  `json:"prepared_at"`
	AgeSeconds         float64 `json:"age_seconds"`
	InDistTransaction  bool    `json:"in_dist_transaction"`
	StillActive        bool    `json:"still_active"`
	Classification     string  `json:"classification"` // commit_needed | rollback_needed | in_flight | foreign_initiator | non_citus | unknown
	RecommendedSQL     string  `json:"recommended_sql,omitempty"`
	Notes              string  `json:"notes,omitempty"`
}

type UntrackedDistTxn struct {
	GroupID int32  `json:"group_id"`
	GID     string `json:"gid"`
	Note    string `json:"note"`
}

type TwoPCSummary struct {
	TotalPrepared           int     `json:"total_prepared"`
	CitusPrepared           int     `json:"citus_prepared"`
	CommitNeeded            int     `json:"commit_needed"`
	RollbackNeeded          int     `json:"rollback_needed"`
	InFlight                int     `json:"in_flight"`
	ForeignInitiator        int     `json:"foreign_initiator"`
	NonCitus                int     `json:"non_citus"`
	OldestAgeSeconds        float64 `json:"oldest_age_seconds"`
	DistTransactionEntries  int     `json:"dist_transaction_entries"`
	UntrackedDistTxns       int     `json:"untracked_dist_transactions"`
	ActiveDistTransactions  int     `json:"active_dist_transactions"`
}

type TwoPCRecoveryOutput struct {
	Summary                   TwoPCSummary        `json:"summary"`
	Prepared                  []PreparedXactRow   `json:"prepared"`
	UntrackedDistTransactions []UntrackedDistTxn  `json:"untracked_dist_transactions"`
	RecoveryScript            string              `json:"recovery_script,omitempty"`
	Findings                  []string            `json:"findings"`
	Recommendations           []string            `json:"recommendations"`
	Alarms                    []diagnostics.Alarm `json:"alarms"`
	OverallStatus             string              `json:"overall_status"` // ok | warning | critical
}

// groupMapEntry maps nodename:port -> groupid; used to join fanout results
// with pg_dist_transaction.groupid.
type groupMapEntry struct {
	groupID  int32
	nodeName string
	nodePort int32
}

func TwoPCRecoveryInspectorTool(ctx context.Context, deps Dependencies, in TwoPCRecoveryInput) (*mcp.CallToolResult, TwoPCRecoveryOutput, error) {
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), TwoPCRecoveryOutput{}, nil
	}
	if in.InFlightThresholdSeconds <= 0 {
		in.InFlightThresholdSeconds = 60
	}
	if in.StuckOrphanSeconds <= 0 {
		in.StuckOrphanSeconds = 600
	}
	// default: emit the script unless SuppressRecoveryScript was set.
	generateScript := !in.SuppressRecoveryScript

	out := TwoPCRecoveryOutput{
		Prepared:                  []PreparedXactRow{},
		UntrackedDistTransactions: []UntrackedDistTxn{},
		Findings:                  []string{},
		Recommendations:           []string{},
		Alarms:                    []diagnostics.Alarm{},
		OverallStatus:             "ok",
	}

	// -------- 1. Node → group-id map (needed to correlate with pg_dist_transaction.groupid).
	nodeToGroup := map[string]int32{}      // key = "host:port"
	groupToNode := map[int32]groupMapEntry{}
	coordGroupID := int32(0)
	{
		rows, err := deps.Pool.Query(ctx,
			`SELECT groupid, nodename, nodeport FROM pg_catalog.pg_dist_node WHERE isactive`)
		if err == nil {
			for rows.Next() {
				var gid int32
				var nn string
				var np int32
				if rows.Scan(&gid, &nn, &np) == nil {
					key := fmt.Sprintf("%s:%d", nn, np)
					nodeToGroup[key] = gid
					groupToNode[gid] = groupMapEntry{groupID: gid, nodeName: nn, nodePort: np}
				}
			}
			rows.Close()
		}
		// The coordinator's own groupid is the one with groupid=0 typically, but
		// formally comes from citus_get_local_group_id().
		_ = deps.Pool.QueryRow(ctx, `SELECT citus_get_local_group_id()`).Scan(&coordGroupID)
	}

	// -------- 2. pg_dist_transaction (which GIDs the coordinator has committed
	// and expects each worker group to COMMIT PREPARED).
	distTxnSet := map[string]bool{}         // key = "groupid|gid"
	distTxnByKey := map[string]UntrackedDistTxn{} // key = "groupid|gid"
	{
		rows, err := deps.Pool.Query(ctx, `SELECT groupid, gid FROM pg_catalog.pg_dist_transaction`)
		if err == nil {
			for rows.Next() {
				var groupid int32
				var name string
				if rows.Scan(&groupid, &name) == nil {
					k := fmt.Sprintf("%d|%s", groupid, name)
					distTxnSet[k] = true
					distTxnByKey[k] = UntrackedDistTxn{GroupID: groupid, GID: name}
					out.Summary.DistTransactionEntries++
				}
			}
			rows.Close()
		}
	}

	// -------- 3. Active transaction numbers (to spot in-flight 2PCs).
	activeTxnNums := map[uint64]bool{}
	{
		// get_all_active_transactions() fans out to workers via coordinator.
		rows, err := deps.Pool.Query(ctx,
			`SELECT transaction_number FROM get_all_active_transactions() WHERE transaction_number > 0`)
		if err == nil {
			for rows.Next() {
				var n uint64
				if rows.Scan(&n) == nil {
					activeTxnNums[n] = true
				}
			}
			rows.Close()
		}
		out.Summary.ActiveDistTransactions = len(activeTxnNums)
	}

	// -------- 4. pg_prepared_xacts on coordinator.
	const preparedQuery = `SELECT gid::text,
	       coalesce(database, current_database())::text AS database,
	       owner::text,
	       to_char(prepared AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS prepared_at,
	       EXTRACT(EPOCH FROM (now() - prepared))::float8 AS age_seconds
	FROM pg_catalog.pg_prepared_xacts
	WHERE database = current_database()`

	// Coord rows.
	{
		rows, err := deps.Pool.Query(ctx, preparedQuery)
		if err == nil {
			for rows.Next() {
				r := PreparedXactRow{Node: "coordinator", NodeGroupID: coordGroupID}
				if err := rows.Scan(&r.GID, &r.Database, &r.Owner, &r.PreparedAt, &r.AgeSeconds); err != nil {
					continue
				}
				out.Prepared = append(out.Prepared, r)
			}
			rows.Close()
		}
	}

	// Worker rows via fanout.
	if deps.Fanout != nil {
		results, err := deps.Fanout.OnWorkers(ctx, preparedQuery)
		if err == nil {
			for _, nr := range results {
				if !nr.Success {
					out.Findings = append(out.Findings,
						fmt.Sprintf("fanout to %s:%d failed: %s", nr.NodeName, nr.NodePort, nr.Error))
					continue
				}
				nodeKey := fmt.Sprintf("%s:%d", nr.NodeName, nr.NodePort)
				nodeGroup := nodeToGroup[nodeKey]
				for _, row := range nr.Rows {
					pr := PreparedXactRow{Node: nodeKey, NodeGroupID: nodeGroup}
					if v, ok := row["gid"].(string); ok {
						pr.GID = v
					}
					if v, ok := row["database"].(string); ok {
						pr.Database = v
					}
					if v, ok := row["owner"].(string); ok {
						pr.Owner = v
					}
					if v, ok := row["prepared_at"].(string); ok {
						pr.PreparedAt = v
					}
					switch v := row["age_seconds"].(type) {
					case float64:
						pr.AgeSeconds = v
					case string:
						if f, err := strconv.ParseFloat(v, 64); err == nil {
							pr.AgeSeconds = f
						}
					}
					out.Prepared = append(out.Prepared, pr)
				}
			}
		} else {
			out.Findings = append(out.Findings, fmt.Sprintf("worker fanout failed: %v", err))
		}
	}

	// -------- 5. Classify + compute recommended SQL.
	seenDistTxnKeys := map[string]bool{} // (groupid, gid) pairs we've matched
	for i := range out.Prepared {
		r := &out.Prepared[i]

		gidKey := fmt.Sprintf("%d|%s", r.NodeGroupID, r.GID)
		ok, parsed := parseCitusGID(r.GID)
		r.IsCitusGID = ok
		if ok {
			r.InitiatorGroupID = parsed.groupID
			r.InitiatorPID = parsed.pid
			r.TransactionNumber = parsed.transactionNumber
			r.ConnectionNumber = parsed.connectionNumber
		}

		r.InDistTransaction = distTxnSet[gidKey]
		if r.InDistTransaction {
			seenDistTxnKeys[gidKey] = true
		}
		r.StillActive = ok && activeTxnNums[r.TransactionNumber]

		// Foreign-initiator check: Citus's RecoverWorkerTransactions
		// (transaction_recovery.c:516-526) filters prepared xacts with
		// LIKE 'citus_<localGroupId>_%'. So the coordinator should only
		// act on 2PCs it initiated (parsed.groupID == coordGroupID).
		// Worker-initiated 2PCs are recovered by that worker's own
		// maintenance daemon — we must not suggest COMMIT/ROLLBACK here.
		foreign := ok && parsed.groupID != coordGroupID

		switch {
		case !r.IsCitusGID:
			r.Classification = "non_citus"
			r.Notes = "GID does not match citus_<group>_<pid>_<txn>_<conn> — not managed by Citus recovery"
		case foreign:
			r.Classification = "foreign_initiator"
			r.Notes = fmt.Sprintf("initiator group %d is not the coordinator (group %d). This 2PC is recovered by that worker's own maintenance daemon; no action from the coordinator.",
				parsed.groupID, coordGroupID)
		case r.InDistTransaction:
			r.Classification = "commit_needed"
			r.RecommendedSQL = fmt.Sprintf("COMMIT PREPARED %s;", sqlQuote(r.GID))
			r.Notes = "coordinator committed the distributed transaction; this node must COMMIT PREPARED to durably finish"
		case r.StillActive:
			r.Classification = "in_flight"
			r.Notes = "transaction_number still present in get_all_active_transactions(); mid-2PC — leave alone"
		case r.AgeSeconds < float64(in.InFlightThresholdSeconds):
			r.Classification = "in_flight"
			r.Notes = fmt.Sprintf("younger than in_flight_threshold_seconds=%d; allow maintenance daemon to recover naturally",
				in.InFlightThresholdSeconds)
		default:
			r.Classification = "rollback_needed"
			r.RecommendedSQL = fmt.Sprintf("ROLLBACK PREPARED %s;", sqlQuote(r.GID))
			r.Notes = "orphan: no coordinator commit record, no active txn number. Safe to ROLLBACK PREPARED."
		}

		if r.AgeSeconds > out.Summary.OldestAgeSeconds {
			out.Summary.OldestAgeSeconds = r.AgeSeconds
		}
	}

	// Strip non-citus unless requested.
	if !in.IncludeNonCitus {
		filtered := out.Prepared[:0]
		for _, p := range out.Prepared {
			if p.IsCitusGID {
				filtered = append(filtered, p)
			}
		}
		out.Prepared = filtered
	}

	// Summary counters.
	out.Summary.TotalPrepared = len(out.Prepared)
	for _, p := range out.Prepared {
		if p.IsCitusGID {
			out.Summary.CitusPrepared++
		}
		switch p.Classification {
		case "commit_needed":
			out.Summary.CommitNeeded++
		case "rollback_needed":
			out.Summary.RollbackNeeded++
		case "in_flight":
			out.Summary.InFlight++
		case "foreign_initiator":
			out.Summary.ForeignInitiator++
		case "non_citus":
			out.Summary.NonCitus++
		}
	}

	// Untracked pg_dist_transaction entries (coord thinks needs commit, but
	// no prepared xact exists anywhere).
	for k, e := range distTxnByKey {
		if !seenDistTxnKeys[k] {
			out.UntrackedDistTransactions = append(out.UntrackedDistTransactions, UntrackedDistTxn{
				GroupID: e.GroupID,
				GID:     e.GID,
				Note:    "pg_dist_transaction has this entry but no matching prepared xact — already recovered, or worker unreachable",
			})
		}
	}
	out.Summary.UntrackedDistTxns = len(out.UntrackedDistTransactions)

	// -------- 6. Recommendations + alarms.
	if out.Summary.CommitNeeded > 0 {
		out.Recommendations = append(out.Recommendations,
			fmt.Sprintf("%d prepared xact(s) awaiting COMMIT PREPARED — run SELECT recover_prepared_transactions(); on the coordinator to have the maintenance daemon recover them, or use the emitted recovery_script for manual control.",
				out.Summary.CommitNeeded))
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "two_pc.commit_backlog",
			Severity: diagnostics.SeverityWarning,
			Source:   "citus_2pc_recovery_inspector",
			Message:  fmt.Sprintf("%d prepared xact(s) need COMMIT PREPARED", out.Summary.CommitNeeded),
			Evidence: map[string]any{"commit_needed": out.Summary.CommitNeeded,
				"oldest_age_seconds": out.Summary.OldestAgeSeconds},
			FixHint: "SELECT recover_prepared_transactions();",
		})
		out.Alarms = append(out.Alarms, *a)
		if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
	}

	if out.Summary.RollbackNeeded > 0 {
		// Escalate if any orphan is older than StuckOrphanSeconds.
		stuckOldest := float64(0)
		for _, p := range out.Prepared {
			if p.Classification == "rollback_needed" && p.AgeSeconds > stuckOldest {
				stuckOldest = p.AgeSeconds
			}
		}
		severity := diagnostics.SeverityWarning
		if stuckOldest > float64(in.StuckOrphanSeconds) {
			severity = diagnostics.SeverityCritical
			out.OverallStatus = "critical"
		} else if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
		out.Recommendations = append(out.Recommendations,
			fmt.Sprintf("%d orphan prepared xact(s) ready for ROLLBACK PREPARED (oldest %.0fs). Review the per-node script below; each statement must be executed on the node that owns the prepared xact.",
				out.Summary.RollbackNeeded, stuckOldest))
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "two_pc.orphans",
			Severity: severity,
			Source:   "citus_2pc_recovery_inspector",
			Message:  fmt.Sprintf("%d orphan prepared xact(s) (oldest %.0fs) — distributed tx aborted but worker never recovered", out.Summary.RollbackNeeded, stuckOldest),
			Evidence: map[string]any{"rollback_needed": out.Summary.RollbackNeeded, "oldest_age_seconds": stuckOldest},
			FixHint:  "Run the emitted recovery_script, or SELECT recover_prepared_transactions(); on the coordinator (safe: Citus replicates our classification).",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	// Very old in-flight (slow 2PC) — warn.
	for _, p := range out.Prepared {
		if p.Classification == "in_flight" && p.AgeSeconds > float64(in.StuckOrphanSeconds) {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind:     "two_pc.slow_in_flight",
				Severity: diagnostics.SeverityWarning,
				Source:   "citus_2pc_recovery_inspector",
				Message:  fmt.Sprintf("Prepared xact %s on %s still in flight after %.0fs", p.GID, p.Node, p.AgeSeconds),
				Evidence: map[string]any{"gid": p.GID, "node": p.Node, "age_seconds": p.AgeSeconds},
				FixHint:  "Investigate the originator backend (initiator_pid); it may be stuck waiting on a lock or a slow worker.",
			})
			out.Alarms = append(out.Alarms, *a)
			if out.OverallStatus == "ok" {
				out.OverallStatus = "warning"
			}
			break
		}
	}

	if out.Summary.UntrackedDistTxns > 0 {
		out.Findings = append(out.Findings,
			fmt.Sprintf("%d pg_dist_transaction entries have no matching prepared xact (already recovered, or worker unreachable).",
				out.Summary.UntrackedDistTxns))
	}

	if out.Summary.TotalPrepared == 0 && out.Summary.UntrackedDistTxns == 0 {
		out.Findings = append(out.Findings, "No prepared xacts on any node — 2PC state is clean.")
	}

	// -------- 7. Recovery script (grouped per-node for operator convenience).
	if generateScript && (out.Summary.CommitNeeded+out.Summary.RollbackNeeded) > 0 {
		out.RecoveryScript = buildRecoveryScript(out.Prepared)
	}

	// Stable ordering for deterministic output.
	sort.SliceStable(out.Prepared, func(i, j int) bool {
		if out.Prepared[i].Node != out.Prepared[j].Node {
			return out.Prepared[i].Node < out.Prepared[j].Node
		}
		return out.Prepared[i].GID < out.Prepared[j].GID
	})
	sort.SliceStable(out.UntrackedDistTransactions, func(i, j int) bool {
		if out.UntrackedDistTransactions[i].GroupID != out.UntrackedDistTransactions[j].GroupID {
			return out.UntrackedDistTransactions[i].GroupID < out.UntrackedDistTransactions[j].GroupID
		}
		return out.UntrackedDistTransactions[i].GID < out.UntrackedDistTransactions[j].GID
	})

	return nil, out, nil
}

type parsedGID struct {
	groupID           int32
	pid               int32
	transactionNumber uint64
	connectionNumber  uint32
}

// parseCitusGID mirrors ParsePreparedTransactionName() in
// src/backend/distributed/transaction/remote_transaction.c. Expected format:
// citus_<groupId>_<pid>_<transactionNumber>_<connectionNumber>
func parseCitusGID(gid string) (bool, parsedGID) {
	var p parsedGID
	if !strings.HasPrefix(gid, "citus_") {
		return false, p
	}
	parts := strings.Split(gid[len("citus_"):], "_")
	// Valid shapes are exactly 3 or 4 parts (conn optional for older/
	// worker-issued GIDs). Reject everything else to avoid treating
	// arbitrary "citus_foo_bar_baz_…" strings as citus-managed.
	if len(parts) != 3 && len(parts) != 4 {
		return false, p
	}
	g64, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil || g64 < 0 {
		return false, p
	}
	pid64, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil || pid64 < 0 {
		return false, p
	}
	txn64, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return false, p
	}
	p.groupID = int32(g64)
	p.pid = int32(pid64)
	p.transactionNumber = txn64
	if len(parts) == 4 {
		c, err := strconv.ParseUint(parts[3], 10, 32)
		if err != nil {
			return false, p
		}
		p.connectionNumber = uint32(c)
	}
	return true, p
}

// sqlQuote single-quotes a string for use in COMMIT/ROLLBACK PREPARED.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func buildRecoveryScript(prepared []PreparedXactRow) string {
	// Group commit_needed + rollback_needed per node.
	byNode := map[string][]PreparedXactRow{}
	for _, p := range prepared {
		if p.Classification != "commit_needed" && p.Classification != "rollback_needed" {
			continue
		}
		byNode[p.Node] = append(byNode[p.Node], p)
	}
	nodes := make([]string, 0, len(byNode))
	for n := range byNode {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	var b strings.Builder
	b.WriteString("-- citus-mcp 2PC recovery script\n")
	b.WriteString("-- Each block below must be executed on the indicated NODE\n")
	b.WriteString("-- (connect directly; PREPARED xact state is node-local).\n")
	b.WriteString("-- NOTE: COMMIT PREPARED / ROLLBACK PREPARED cannot run inside\n")
	b.WriteString("-- a transaction block, so each statement is a standalone command.\n")
	b.WriteString("-- Safer alternative: run `SELECT recover_prepared_transactions();`\n")
	b.WriteString("-- on the coordinator — Citus uses the same classification.\n\n")
	for _, n := range nodes {
		fmt.Fprintf(&b, "-- === Connect to %s ===\n", n)
		b.WriteString("-- (psql -h <host> -p <port> -d <database>)\n")
		for _, p := range byNode[n] {
			fmt.Fprintf(&b, "-- %s (age %.0fs): %s\n", p.Classification, p.AgeSeconds, p.Notes)
			fmt.Fprintf(&b, "%s\n", p.RecommendedSQL)
		}
		b.WriteString("\n")
	}
	return b.String()
}
