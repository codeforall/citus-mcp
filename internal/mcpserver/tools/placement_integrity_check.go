// citus-mcp: citus_placement_integrity_check
//
// Three-way cross-check between what Citus metadata claims exists and what's
// actually on disk on each worker. Detects the "ghost shard" / "orphan shard"
// class of bugs that crop up after partial rebalance failures, manual
// intervention, recovery from crashes, or when pg_dist_cleanup fails to run.
//
// Data sources (read-only):
//   - pg_dist_placement + pg_dist_shard + pg_dist_node  (the authoritative
//                                                        metadata view)
//   - pg_class / pg_namespace on each worker via run_command_on_workers
//     (the actual on-disk truth)
//   - pg_dist_cleanup  (the scheduled-but-not-yet-deleted residue)
//
// Mismatch classes surfaced:
//   - ghost_placement         : placement row exists but no physical shard
//                               relation found on the worker (catalog-vs-disk
//                               divergence; reads from this placement will
//                               error. Closest cleanup: citus_update_shard_statistics
//                               + manual reconciliation, or citus_copy_shard_placement).
//   - orphan_table            : shard-suffix table present on worker, no
//                               pg_dist_placement entry references it (and
//                               not present in pg_dist_cleanup either).
//                               Comes from: failed moves, dropped tables not
//                               propagated, disabled nodes that were
//                               re-enabled without cleanup.
//   - inactive_with_data      : shardstate != 1 but the relation is still
//                               materialised on the worker (reads/writes skip
//                               it but it consumes disk; candidate for
//                               citus_cleanup_orphaned_resources).
//   - cleanup_backlog         : pg_dist_cleanup has pending records; cross
//                               references with orphan_table list so the
//                               operator knows which are already queued.
//   - size_drift (optional)   : pg_relation_size on worker deviates more than
//                               size_drift_factor from placement.shardlength
//                               (stale statistics; relatively benign).
//
// The tool never issues DROP or UPDATE — it emits diagnostics, alarms, and a
// reconciliation playbook pointing at citus_cleanup_orphaned_resources,
// citus_copy_shard_placement, and manual drop commands gated behind the
// mutating tools.
//
// Related citusdata/citus issues: #8236, #5284, #5286, #4977, #5133.

package tools

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type PlacementIntegrityInput struct {
	// Schemas to inspect on workers. If empty, uses 'public'.
	Schemas []string `json:"schemas,omitempty"`
	// Skip size-drift detection (faster).
	SkipSizeDrift bool `json:"skip_size_drift,omitempty"`
	// Factor above which pg_relation_size vs placement.shardlength is flagged.
	// Default 2.0 (i.e. actual size > 2x recorded size or vice-versa).
	SizeDriftFactor float64 `json:"size_drift_factor,omitempty"`
	// Cap the number of mismatches returned per class (protects against
	// thundering-herd outputs on very broken clusters). Default 200.
	MaxRowsPerClass int `json:"max_rows_per_class,omitempty"`
	// Include inactive_with_data checks (shardstate != 1 but data exists on disk).
	// Default true.
	CheckInactivePlacements *bool `json:"check_inactive_placements,omitempty"`
}

type GhostPlacement struct {
	PlacementID int64  `json:"placement_id"`
	ShardID     int64  `json:"shard_id"`
	GroupID     int32  `json:"group_id"`
	NodeName    string `json:"node_name"`
	NodePort    int32  `json:"node_port"`
	Relation    string `json:"relation"`
	ExpectedRel string `json:"expected_shard_relation"`
	ShardState  int32  `json:"shard_state"`
}

type OrphanTable struct {
	NodeName      string `json:"node_name"`
	NodePort      int32  `json:"node_port"`
	Schema        string `json:"schema"`
	Relation      string `json:"relation"`
	ShardIDGuess  int64  `json:"shard_id_guess"`
	BytesOnDisk   int64  `json:"bytes_on_disk"`
	InCleanupList bool   `json:"in_cleanup_list"`
}

type InactiveWithData struct {
	PlacementID int64  `json:"placement_id"`
	ShardID     int64  `json:"shard_id"`
	ShardState  int32  `json:"shard_state"`
	NodeName    string `json:"node_name"`
	NodePort    int32  `json:"node_port"`
	Relation    string `json:"relation"`
	BytesOnDisk int64  `json:"bytes_on_disk"`
}

type SizeDrift struct {
	PlacementID  int64   `json:"placement_id"`
	ShardID      int64   `json:"shard_id"`
	NodeName     string  `json:"node_name"`
	NodePort     int32   `json:"node_port"`
	Relation     string  `json:"relation"`
	MetadataSize int64   `json:"metadata_size"`
	ActualSize   int64   `json:"actual_size"`
	DriftRatio   float64 `json:"drift_ratio"`
}

type CleanupRecord struct {
	RecordID    int64  `json:"record_id"`
	OperationID int64  `json:"operation_id"`
	ObjectType  int32  `json:"object_type"`
	ObjectName  string `json:"object_name"`
	NodeGroupID int32  `json:"node_group_id"`
	PolicyType  int32  `json:"policy_type"`
}

type PlacementIntegrityOutput struct {
	PlacementsChecked  int                `json:"placements_checked"`
	WorkersChecked     int                `json:"workers_checked"`
	SkippedWorkers     []SkippedWorker    `json:"skipped_workers"`
	PartialResults     bool               `json:"partial_results"` // true iff at least one worker fanout failed
	GhostPlacements    []GhostPlacement   `json:"ghost_placements"`
	OrphanTables       []OrphanTable      `json:"orphan_tables"`
	InactiveWithData   []InactiveWithData `json:"inactive_with_data"`
	SizeDrifts         []SizeDrift        `json:"size_drifts"`
	StaleStats         []SizeDrift        `json:"stale_stats"` // metadata shardlength=0 but bytes>0
	CleanupRecords     []CleanupRecord    `json:"cleanup_records"`
	Truncated          map[string]bool    `json:"truncated"` // per-class truncation flag
	Summary            string             `json:"summary"`
	Playbook           []string           `json:"playbook"`
	Recommendations    []string           `json:"recommendations"`
	Alarms             []diagnostics.Alarm `json:"alarms"`
	OverallStatus      string             `json:"overall_status"` // ok | warning | critical
}

type SkippedWorker struct {
	NodeName string `json:"node_name"`
	NodePort int32  `json:"node_port"`
	Error    string `json:"error"`
}

var shardSuffixRE = regexp.MustCompile(`_(\d+)$`)

func PlacementIntegrityCheckTool(ctx context.Context, deps Dependencies, in PlacementIntegrityInput) (*mcp.CallToolResult, PlacementIntegrityOutput, error) {
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), PlacementIntegrityOutput{}, nil
	}

	if in.SizeDriftFactor <= 0 {
		in.SizeDriftFactor = 2.0
	}
	if in.MaxRowsPerClass <= 0 {
		in.MaxRowsPerClass = 200
	}
	if len(in.Schemas) == 0 {
		in.Schemas = []string{"public"}
	}
	checkInactive := true
	if in.CheckInactivePlacements != nil {
		checkInactive = *in.CheckInactivePlacements
	}

	out := PlacementIntegrityOutput{
		SkippedWorkers:   []SkippedWorker{},
		GhostPlacements:  []GhostPlacement{},
		OrphanTables:     []OrphanTable{},
		InactiveWithData: []InactiveWithData{},
		SizeDrifts:       []SizeDrift{},
		StaleStats:       []SizeDrift{},
		CleanupRecords:   []CleanupRecord{},
		Truncated:        map[string]bool{},
		Playbook:         []string{},
		Recommendations:  []string{},
		Alarms:           []diagnostics.Alarm{},
		OverallStatus:    "ok",
	}

	if deps.Fanout == nil {
		out.Summary = "Fanout unavailable (no active workers); placement integrity check requires worker access."
		return nil, out, nil
	}

	// -------- 1. Expected placements from metadata.
	type expected struct {
		placementID int64
		shardID     int64
		groupID     int32
		shardState  int32
		relation    string // qualified regclass text (e.g. public.events)
		shardLen    int64
		nodeName    string
		nodePort    int32
	}
	// Keyed by (nodename:port, relname_shardid) for fast lookup vs on-disk.
	expectedByKey := map[string]*expected{}
	// Also keyed by shardid per node to detect inactive-with-data.
	expectedByShardNode := map[string]*expected{}
	// Set of (lowercased base name | shardid) pairs that metadata knows
	// about. An on-disk `<base>_<shardid>` relation is only a Citus-shard
	// candidate if this pair exists — otherwise it's a user table whose
	// name happens to match `<distributed-table-base>_<digits>` (e.g.
	// an events_123456 table where 123456 isn't a real shardid).
	validBasePairs := map[string]bool{}

	rows, err := deps.Pool.Query(ctx, `
		SELECT p.placementid, p.shardid, p.groupid, p.shardstate::int,
		       s.logicalrelid::regclass::text,
		       COALESCE(p.shardlength, 0),
		       n.nodename, n.nodeport
		FROM pg_catalog.pg_dist_placement p
		JOIN pg_catalog.pg_dist_shard s USING(shardid)
		JOIN pg_catalog.pg_dist_node n ON n.groupid = p.groupid
		WHERE n.isactive AND n.noderole = 'primary' AND n.groupid != 0
		ORDER BY p.shardid, p.groupid`)
	if err != nil {
		return callError(serr.CodeInternalError, fmt.Sprintf("query pg_dist_placement: %v", err), ""), out, nil
	}
	defer rows.Close()

	for rows.Next() {
		var e expected
		if err := rows.Scan(&e.placementID, &e.shardID, &e.groupID, &e.shardState,
			&e.relation, &e.shardLen, &e.nodeName, &e.nodePort); err != nil {
			continue
		}
		out.PlacementsChecked++
		// Normalize: regclass::text drops "public." for tables on search_path,
		// which would break our lowercased lookup against "<schema>.<name>"
		// keys built from pg_class. Always ensure a "schema." prefix.
		if !strings.Contains(e.relation, ".") {
			e.relation = "public." + e.relation
		}
		ek := e
		expectedByKey[placementKey(e.nodeName, e.nodePort, e.relation, e.shardID)] = &ek
		expectedByShardNode[fmt.Sprintf("%s:%d|%d", e.nodeName, e.nodePort, e.shardID)] = &ek
		validBasePairs[fmt.Sprintf("%s|%d", strings.ToLower(e.relation), e.shardID)] = true
	}

	// -------- 2. Fan out to workers: enumerate shard-suffix relations with sizes.
	schemaLiteralList := strings.Join(quoteLiteralList(in.Schemas), ", ")
	// NOTE: Citus installs a pg_class visibility hook that hides shard tables
	// from regular pg_class queries (controlled by citus.override_table_visibility
	// and citus.show_shards_for_app_name_prefixes). We MUST disable the hook
	// here or we'd see zero shard relations and flag every placement as a ghost.
	// SET LOCAL within a transaction is safe inside run_command_on_workers.
	workerSQL := fmt.Sprintf(`
		WITH _reset AS (
			SELECT set_config('citus.override_table_visibility', 'off', true) AS _x
		)
		SELECT n.nspname AS schema,
		       c.relname AS relname,
		       pg_catalog.pg_relation_size(c.oid) AS bytes
		FROM _reset, pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','f','p')
		  AND n.nspname IN (%s)
		  AND c.relname ~ '_[0-9]+$'`, schemaLiteralList)
	workerResults, err := deps.Fanout.OnWorkers(ctx, workerSQL)
	if err != nil {
		return callError(serr.CodeInternalError, fmt.Sprintf("fanout: %v", err), ""), out, nil
	}
	out.WorkersChecked = len(workerResults)

	// Keyed the same way as expectedByKey.
	actualByKey := map[string]struct {
		schema  string
		relname string
		bytes   int64
	}{}

	// Track workers whose fanout failed — we must NOT classify any
	// placement on those nodes as ghost (the on-disk truth is unknown).
	skippedNodes := map[string]bool{} // key = "nodename:port"

	for _, wr := range workerResults {
		if !wr.Success {
			nodeKey := fmt.Sprintf("%s:%d", wr.NodeName, wr.NodePort)
			skippedNodes[nodeKey] = true
			out.SkippedWorkers = append(out.SkippedWorkers, SkippedWorker{
				NodeName: wr.NodeName, NodePort: wr.NodePort, Error: wr.Error,
			})
			out.PartialResults = true
			continue
		}
		for _, row := range wr.Rows {
			schema, _ := row["schema"].(string)
			relname, _ := row["relname"].(string)
			bytes := coerceInt64(row["bytes"])
			m := shardSuffixRE.FindStringSubmatch(relname)
			if m == nil {
				continue
			}
			shardID, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				continue
			}
			baseName := strings.TrimSuffix(relname, "_"+m[1])
			qualified := fmt.Sprintf("%s.%s", schema, baseName)
			// A relation is a genuine Citus shard only if BOTH the base
			// name matches a known distributed table AND the (base,
			// shardid) pair exists in metadata. The pair check rules out
			// user tables like `events_123456` where 123456 isn't a real
			// shard id.
			if !validBasePairs[fmt.Sprintf("%s|%d", strings.ToLower(qualified), shardID)] {
				continue
			}
			key := placementKey(wr.NodeName, wr.NodePort, qualified, shardID)
			actualByKey[key] = struct {
				schema  string
				relname string
				bytes   int64
			}{schema, relname, bytes}
		}
	}

	// -------- 3. pg_dist_cleanup (cross-ref for orphan classification).
	cleanupByName := map[string]bool{}
	{
		cRows, err := deps.Pool.Query(ctx, `
			SELECT record_id, operation_id, object_type, object_name,
			       node_group_id, policy_type
			FROM pg_catalog.pg_dist_cleanup
			ORDER BY record_id`)
		if err == nil {
			defer cRows.Close()
			for cRows.Next() {
				var cr CleanupRecord
				if err := cRows.Scan(&cr.RecordID, &cr.OperationID, &cr.ObjectType,
					&cr.ObjectName, &cr.NodeGroupID, &cr.PolicyType); err != nil {
					continue
				}
				out.CleanupRecords = append(out.CleanupRecords, cr)
				// object_name is something like "public.events_1002" — normalize.
				cleanupByName[strings.ToLower(cr.ObjectName)] = true
			}
		}
	}

	// -------- 4. Detect ghost_placements: expected entry with no on-disk match.
	for k, e := range expectedByKey {
		if _, ok := actualByKey[k]; ok {
			continue
		}
		if e.shardState != 1 {
			// Not active — treated separately (inactive_with_data covers the
			// other direction). No ghost alert for intentionally inactive rows.
			continue
		}
		// If the worker's fanout failed we don't actually know whether the
		// shard relation is on-disk or not — treating it as a ghost would
		// trigger one false-positive per placement on that node.
		if skippedNodes[fmt.Sprintf("%s:%d", e.nodeName, e.nodePort)] {
			continue
		}
		baseName := e.relation
		if dot := strings.IndexByte(baseName, '.'); dot >= 0 {
			baseName = baseName[dot+1:]
		}
		out.GhostPlacements = append(out.GhostPlacements, GhostPlacement{
			PlacementID: e.placementID,
			ShardID:     e.shardID,
			GroupID:     e.groupID,
			NodeName:    e.nodeName,
			NodePort:    e.nodePort,
			Relation:    e.relation,
			ExpectedRel: fmt.Sprintf("%s_%d", baseName, e.shardID),
			ShardState:  e.shardState,
		})
		if len(out.GhostPlacements) >= in.MaxRowsPerClass {
			out.Truncated["ghost_placements"] = true
			break
		}
	}

	// -------- 5. Detect orphan_tables: on-disk relation with no placement.
	for k, a := range actualByKey {
		if _, ok := expectedByKey[k]; ok {
			continue
		}
		// Reparse key → nodename:port|schema.name|shardid
		nodeName, nodePort, _, shardID := parsePlacementKey(k)
		fqName := strings.ToLower(a.schema + "." + a.relname)
		out.OrphanTables = append(out.OrphanTables, OrphanTable{
			NodeName:      nodeName,
			NodePort:      nodePort,
			Schema:        a.schema,
			Relation:      a.relname,
			ShardIDGuess:  shardID,
			BytesOnDisk:   a.bytes,
			InCleanupList: cleanupByName[fqName],
		})
		if len(out.OrphanTables) >= in.MaxRowsPerClass {
			out.Truncated["orphan_tables"] = true
			break
		}
	}

	// -------- 6. inactive_with_data: shardstate != 1 but relation is present.
	if checkInactive {
		for k, e := range expectedByKey {
			if e.shardState == 1 {
				continue
			}
			a, ok := actualByKey[k]
			if !ok {
				continue
			}
			out.InactiveWithData = append(out.InactiveWithData, InactiveWithData{
				PlacementID: e.placementID,
				ShardID:     e.shardID,
				ShardState:  e.shardState,
				NodeName:    e.nodeName,
				NodePort:    e.nodePort,
				Relation:    a.schema + "." + a.relname,
				BytesOnDisk: a.bytes,
			})
			if len(out.InactiveWithData) >= in.MaxRowsPerClass {
				out.Truncated["inactive_with_data"] = true
				break
			}
		}
	}

	// -------- 7. size_drift: placement.shardlength vs pg_relation_size.
	if !in.SkipSizeDrift {
		for k, e := range expectedByKey {
			if e.shardState != 1 {
				continue
			}
			a, ok := actualByKey[k]
			if !ok {
				continue
			}
			// Special-case the stale-stats pattern: metadata shardlength=0
			// (never populated or stale after citus_update_shard_statistics
			// was skipped) but disk has real bytes. Surface it separately
			// instead of suppressing — this is the most common "why is my
			// rebalancer making bad decisions?" signal.
			if e.shardLen == 0 && a.bytes > 0 {
				if !out.Truncated["stale_stats"] {
					out.StaleStats = append(out.StaleStats, SizeDrift{
						PlacementID:  e.placementID,
						ShardID:      e.shardID,
						NodeName:     e.nodeName,
						NodePort:     e.nodePort,
						Relation:     a.schema + "." + a.relname,
						MetadataSize: 0,
						ActualSize:   a.bytes,
						DriftRatio:   0, // undefined; metadata=0
					})
					if len(out.StaleStats) >= in.MaxRowsPerClass {
						out.Truncated["stale_stats"] = true
					}
				}
				continue
			}
			if e.shardLen == 0 || a.bytes == 0 {
				continue
			}
			ratio := float64(a.bytes) / float64(e.shardLen)
			if ratio < 1 {
				ratio = 1 / ratio
			}
			if ratio >= in.SizeDriftFactor {
				out.SizeDrifts = append(out.SizeDrifts, SizeDrift{
					PlacementID:  e.placementID,
					ShardID:      e.shardID,
					NodeName:     e.nodeName,
					NodePort:     e.nodePort,
					Relation:     a.schema + "." + a.relname,
					MetadataSize: e.shardLen,
					ActualSize:   a.bytes,
					DriftRatio:   ratio,
				})
				if len(out.SizeDrifts) >= in.MaxRowsPerClass {
					out.Truncated["size_drifts"] = true
					break
				}
			}
		}
	}

	// -------- 8. Summary + classification + alarms.
	summaryParts := fmt.Sprintf(
		"Checked %d placement(s) across %d worker(s): %d ghost, %d orphan, %d inactive-with-data, %d size-drift, %d stale-stats, %d pending cleanup record(s).",
		out.PlacementsChecked, out.WorkersChecked,
		len(out.GhostPlacements), len(out.OrphanTables),
		len(out.InactiveWithData), len(out.SizeDrifts),
		len(out.StaleStats), len(out.CleanupRecords))
	if out.PartialResults {
		summaryParts += fmt.Sprintf(" PARTIAL RESULTS: %d worker(s) unreachable — ghost/orphan detection skipped for those nodes.", len(out.SkippedWorkers))
	}
	out.Summary = summaryParts

	if out.PartialResults {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "placement.partial_results", Severity: diagnostics.SeverityWarning,
			Source:  "citus_placement_integrity_check",
			Message: fmt.Sprintf("%d worker(s) unreachable via fanout; results are incomplete", len(out.SkippedWorkers)),
			Evidence: map[string]any{"skipped_workers": out.SkippedWorkers},
			FixHint:  "Verify all workers are reachable (citus_check_cluster_node_health()) and re-run this tool.",
		})
		out.Alarms = append(out.Alarms, *a)
		if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
	}

	if len(out.StaleStats) > 0 {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "placement.stale_stats", Severity: diagnostics.SeverityWarning,
			Source:  "citus_placement_integrity_check",
			Message: fmt.Sprintf("%d placement(s) have shardlength=0 in metadata but data on disk — rebalancer decisions will be wrong", len(out.StaleStats)),
			Evidence: map[string]any{"count": len(out.StaleStats)},
			FixHint:  "SELECT citus_update_shard_statistics(shardid) for each affected shard, or SELECT citus_update_table_statistics(logicalrelid) per table.",
		})
		out.Alarms = append(out.Alarms, *a)
		if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
	}

	if len(out.GhostPlacements) > 0 {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "placement.ghost", Severity: diagnostics.SeverityCritical,
			Source:  "citus_placement_integrity_check",
			Message: fmt.Sprintf("%d placement(s) reference non-existent shard relations", len(out.GhostPlacements)),
			Evidence: map[string]any{
				"ghost_count":        len(out.GhostPlacements),
				"sample_placement":   out.GhostPlacements[0],
			},
			FixHint: "Reads routed to these shards WILL fail. Remediate with SELECT citus_copy_shard_placement(<shardid>, <src_group>, <dst_group>) from a known-good replica, or delete the orphaned placement row after careful verification. See playbook.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.OverallStatus = "critical"
	}

	if len(out.OrphanTables) > 0 {
		sev := diagnostics.SeverityWarning
		unqueued := 0
		for _, o := range out.OrphanTables {
			if !o.InCleanupList {
				unqueued++
			}
		}
		if unqueued > 0 {
			sev = diagnostics.SeverityCritical
		}
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "placement.orphan_tables", Severity: sev,
			Source:  "citus_placement_integrity_check",
			Message: fmt.Sprintf("%d shard-suffix table(s) on workers have no placement metadata (%d not queued in pg_dist_cleanup)", len(out.OrphanTables), unqueued),
			Evidence: map[string]any{
				"orphan_count":    len(out.OrphanTables),
				"not_in_cleanup":  unqueued,
				"total_bytes":     sumOrphanBytes(out.OrphanTables),
			},
			FixHint: "SELECT citus_cleanup_orphaned_resources(); will drop those queued in pg_dist_cleanup. Un-queued orphans require manual drop (see playbook).",
		})
		out.Alarms = append(out.Alarms, *a)
		if out.OverallStatus == "ok" {
			out.OverallStatus = string(sev)
		} else if sev == diagnostics.SeverityCritical {
			out.OverallStatus = "critical"
		}
	}

	if len(out.InactiveWithData) > 0 {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "placement.inactive_with_data", Severity: diagnostics.SeverityWarning,
			Source:  "citus_placement_integrity_check",
			Message: fmt.Sprintf("%d inactive placement(s) still have data on disk (reclaim opportunity)", len(out.InactiveWithData)),
			Evidence: map[string]any{
				"count":           len(out.InactiveWithData),
				"reclaimable_bytes": sumInactiveBytes(out.InactiveWithData),
			},
			FixHint: "SELECT citus_cleanup_orphaned_resources(); or drop the inactive placements after verifying the active replica is healthy.",
		})
		out.Alarms = append(out.Alarms, *a)
		if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
	}

	// -------- 9. Playbook.
	buildPlacementPlaybook(&out)

	if len(out.GhostPlacements)+len(out.OrphanTables)+len(out.InactiveWithData)+
		len(out.SizeDrifts)+len(out.StaleStats) == 0 &&
		len(out.CleanupRecords) == 0 && !out.PartialResults {
		out.Recommendations = append(out.Recommendations,
			"Placement metadata and on-disk state are fully consistent.")
	}

	return nil, out, nil
}

func buildPlacementPlaybook(out *PlacementIntegrityOutput) {
	if out.PartialResults {
		out.Playbook = append(out.Playbook,
			"PARTIAL RESULTS — one or more workers were unreachable during fanout:",
			"  Ghost/orphan/size-drift detection was suppressed for those nodes.",
			"  Verify worker health and re-run:",
			"    SELECT * FROM citus_check_cluster_node_health();")
	}
	if len(out.GhostPlacements) > 0 {
		out.Playbook = append(out.Playbook,
			"GHOST PLACEMENTS — metadata references shards that don't exist on disk:",
			"  1. Verify a healthy replica exists for each ghost shard:",
			"     SELECT shardid, groupid, shardstate FROM pg_dist_placement WHERE shardid = <ID>;",
			"  2. If another replica has state=1 (active) with data, repair this one:",
			"     SELECT citus_copy_shard_placement(<shardid>, <src_groupid>, <dst_groupid>);",
			"  3. If NO healthy replica exists, restore from backup and/or delete the ghost placement row",
			"     (requires superuser + very careful verification).")
		out.Recommendations = append(out.Recommendations,
			"Ghost placements cause query errors — investigate immediately. If from a failed node re-add, run SELECT citus_disable_node(); then re-add.")
	}
	if len(out.OrphanTables) > 0 {
		out.Playbook = append(out.Playbook,
			"ORPHAN TABLES — shard-suffix tables on workers without placement metadata:",
			"  1. First pass (queued orphans):  SELECT citus_cleanup_orphaned_resources();",
			"  2. Re-check: SELECT COUNT(*) FROM pg_dist_cleanup;  should go to zero.",
			"  3. For un-queued orphans (rows with in_cleanup_list=false below), verify shardid not",
			"     referenced anywhere, then manually drop via run_command_on_workers:",
			"     SELECT run_command_on_workers(format('DROP TABLE IF EXISTS %I.%I', '<schema>', '<relation>'));",
			"     Use the mutating citus_cleanup_orphaned tool (requires approval) for governance.")
	}
	if len(out.InactiveWithData) > 0 {
		out.Playbook = append(out.Playbook,
			"INACTIVE WITH DATA — shardstate != 1 but data still materialised:",
			"  SELECT citus_cleanup_orphaned_resources();  -- reclaims disk")
	}
	if len(out.SizeDrifts) > 0 {
		out.Playbook = append(out.Playbook,
			"SIZE DRIFT — placement.shardlength is stale compared to on-disk size:",
			"  SELECT citus_update_shard_statistics(<shardid>);  -- refreshes shardlength")
	}
	if len(out.StaleStats) > 0 {
		out.Playbook = append(out.Playbook,
			"STALE STATS — placement.shardlength = 0 but disk has data:",
			"  Rebalancer decisions depend on shardlength; refresh statistics before any rebalance:",
			"    SELECT citus_update_shard_statistics(<shardid>);      -- per shard, or",
			"    SELECT citus_update_table_statistics('<relation>');   -- per table.")
	}
	if len(out.GhostPlacements)+len(out.OrphanTables)+len(out.InactiveWithData)+
		len(out.SizeDrifts)+len(out.StaleStats) == 0 && !out.PartialResults {
		out.Playbook = append(out.Playbook, "No integrity issues found; metadata and on-disk state agree.")
	}
}

// placementKey normalises the (node, qualified-relation, shardid) tuple to a
// single string used for cross-referencing expected vs actual maps.
func placementKey(nodeName string, nodePort int32, qualifiedRel string, shardID int64) string {
	return fmt.Sprintf("%s:%d|%s|%d", strings.ToLower(nodeName), nodePort,
		strings.ToLower(qualifiedRel), shardID)
}

func parsePlacementKey(k string) (nodeName string, nodePort int32, qualifiedRel string, shardID int64) {
	parts := strings.Split(k, "|")
	if len(parts) != 3 {
		return "", 0, "", 0
	}
	hp := strings.SplitN(parts[0], ":", 2)
	nodeName = hp[0]
	if len(hp) == 2 {
		if v, err := strconv.ParseInt(hp[1], 10, 32); err == nil {
			nodePort = int32(v)
		}
	}
	qualifiedRel = parts[1]
	if v, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
		shardID = v
	}
	return
}

func coerceInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		if n, err := strconv.ParseInt(x, 10, 64); err == nil {
			return n
		}
	}
	return 0
}

func quoteLiteralList(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, "'"+strings.ReplaceAll(s, "'", "''")+"'")
	}
	return out
}

func sumOrphanBytes(os []OrphanTable) int64 {
	var total int64
	for _, o := range os {
		total += o.BytesOnDisk
	}
	return total
}

func sumInactiveBytes(xs []InactiveWithData) int64 {
	var total int64
	for _, x := range xs {
		total += x.BytesOnDisk
	}
	return total
}
