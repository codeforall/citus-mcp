// citus-mcp: B28 citus_metadata_sync_risk -- estimate the work that
// citus_activate_node will do on the new node and predict whether the
// default transactional metadata sync mode will succeed.
//
// Grounded in Citus source (metadata_sync.c:SyncDistributedObjects):
// activation sends, in a single coordinated tx by default:
//   - Node-wide objects (roles with ALTER ROLE SET)
//   - Shell-table + pg_dist_* DELETEs (idempotency)
//   - Colocation INSERTs (#colocations)
//   - All pg_dist_object dependencies (extensions/schemas/types/sequences/funcs)
//   - Shell CREATEs + pg_dist_partition INSERTs (#distributed tables)
//   - pg_dist_shard + pg_dist_placement INSERTs
//   - Tenant schema metadata
//   - Inter-table FK relationships
//
// shared_library_init.c:2166 — citus.metadata_sync_mode GUC docstring:
//   "When we hit memory problems at workers, we have alternative
//    nontransactional mode where we send each command with separate
//    transaction."

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type MetadataSyncRiskInput struct {
	TargetMaxLocksPerTransaction int `json:"target_max_locks_per_transaction,omitempty"` // what-if
	TargetNodeRAMGiB             int `json:"target_node_ram_gib,omitempty"`              // for OOM check
	AssumeSyncMode               string `json:"assume_sync_mode,omitempty"`              // transactional | nontransactional
}

type SyncCounts struct {
	DistributedTables    int `json:"distributed_tables"`
	PartitionedChildren  int `json:"partitioned_children"`
	Shards               int `json:"shards"`
	Placements           int `json:"placements"`
	Colocations          int `json:"colocations"`
	DistributedObjects   int `json:"distributed_objects"`
	DistributedSchemas   int `json:"distributed_schemas"`
	DistributedSequences int `json:"distributed_sequences"`
	DistributedTypes     int `json:"distributed_types"`
	DistributedFunctions int `json:"distributed_functions"`
	DistributedExtensions int `json:"distributed_extensions"`
	RoleSetCommands      int `json:"role_set_commands"`
}

type CoordinatorGUCs struct {
	MaxLocksPerTx           int    `json:"max_locks_per_transaction"`
	MaxConnections          int    `json:"max_connections"`
	StatementTimeoutMs      int    `json:"statement_timeout_ms"`
	LockTimeoutMs           int    `json:"lock_timeout_ms"`
	IdleInTxSessionTimeoutMs int   `json:"idle_in_transaction_session_timeout_ms"`
	MetadataSyncMode        string `json:"metadata_sync_mode"`
	MaxWalSenders           int    `json:"max_wal_senders"`
	MaxReplicationSlots     int    `json:"max_replication_slots"`
}

type MetadataSyncRiskOutput struct {
	Counts                SyncCounts      `json:"counts"`
	Coordinator           CoordinatorGUCs `json:"coordinator_gucs"`
	EstimatedDDLCommands  int             `json:"estimated_ddl_commands"`
	EstimatedLocksInTx    int             `json:"estimated_locks_in_tx"`
	EstimatedMemoryMiB    int             `json:"estimated_worker_memory_mib"`
	EstimatedDurationSec  int             `json:"estimated_duration_sec"`
	// RecommendedMaxLocksPerTransaction is a concrete value that would
	// keep the activation's estimated lock count under the "soft" cap
	// (max_locks_per_transaction × 16). Rounded to the next multiple of 64.
	// Returned only when the current value is insufficient.
	RecommendedMaxLocksPerTransaction int `json:"recommended_max_locks_per_transaction,omitempty"`
	Verdict               string          `json:"verdict"` // ok | nontransactional_recommended | blocked
	Recommendations       []string        `json:"recommendations"`
	Alarms                []diagnostics.Alarm `json:"alarms"`
	Warnings              []string        `json:"warnings,omitempty"`
}

func MetadataSyncRiskTool(ctx context.Context, deps Dependencies, in MetadataSyncRiskInput) (*mcp.CallToolResult, MetadataSyncRiskOutput, error) {
	out := MetadataSyncRiskOutput{
		Recommendations: []string{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{},
	}
	if in.AssumeSyncMode == "" {
		in.AssumeSyncMode = "transactional"
	}

	// Counts.
	c := &out.Counts
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_partition`).Scan(&c.DistributedTables)
	_ = deps.Pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_inherits i
WHERE EXISTS (SELECT 1 FROM pg_dist_partition p WHERE p.logicalrelid=i.inhparent)`).Scan(&c.PartitionedChildren)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_shard`).Scan(&c.Shards)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_placement`).Scan(&c.Placements)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_colocation`).Scan(&c.Colocations)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_object`).Scan(&c.DistributedObjects)
	// Break down pg_dist_object by classid (best-effort; some classids may not exist as regclass on all PG versions).
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_object WHERE classid = 'pg_namespace'::regclass`).Scan(&c.DistributedSchemas)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_object WHERE classid = 'pg_class'::regclass AND objid IN (SELECT oid FROM pg_class WHERE relkind='S')`).Scan(&c.DistributedSequences)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_object WHERE classid = 'pg_type'::regclass`).Scan(&c.DistributedTypes)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_object WHERE classid = 'pg_proc'::regclass`).Scan(&c.DistributedFunctions)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_object WHERE classid = 'pg_extension'::regclass`).Scan(&c.DistributedExtensions)
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_db_role_setting`).Scan(&c.RoleSetCommands)

	// Coordinator GUCs.
	g := &out.Coordinator
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_locks_per_transaction')::int`).Scan(&g.MaxLocksPerTx)
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&g.MaxConnections)
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(current_setting('statement_timeout'),'')::int,0)`).Scan(&g.StatementTimeoutMs)
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(current_setting('lock_timeout'),'')::int,0)`).Scan(&g.LockTimeoutMs)
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(current_setting('idle_in_transaction_session_timeout'),'')::int,0)`).Scan(&g.IdleInTxSessionTimeoutMs)
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(current_setting('citus.metadata_sync_mode', true),'transactional')`).Scan(&g.MetadataSyncMode)
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_wal_senders')::int`).Scan(&g.MaxWalSenders)
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_replication_slots')::int`).Scan(&g.MaxReplicationSlots)

	// DDL command estimate. NOTE: pg_dist_partition already contains partition
	// children (Citus distributes each child), so c.DistributedTables is the
	// total count of shell CREATEs and pg_dist_partition INSERTs. We add
	// c.PartitionedChildren once for the separate ATTACH PARTITION / FK
	// rebuild step that ties each child back to its parent.
	out.EstimatedDDLCommands =
		6 + // deletion preamble commands
			c.RoleSetCommands +
			c.Colocations +
			c.DistributedObjects +
			c.DistributedTables + // shell CREATE (includes partition children)
			c.DistributedTables + // pg_dist_partition INSERT
			c.Shards + // pg_dist_shard INSERTs
			c.Placements + // pg_dist_placement INSERTs
			c.PartitionedChildren // ATTACH PARTITION / inter-table FK rebuild

	// Locks-in-tx estimate: each CREATE + INSERT acquires at least 1 relation
	// lock on the new worker. c.DistributedTables already includes partition
	// children, so DO NOT add c.PartitionedChildren here. Indexes add ~0.5.
	// Round up with 1.5× fudge.
	out.EstimatedLocksInTx = int(float64(c.DistributedTables+c.DistributedObjects+c.Colocations) * 1.5)
	if out.EstimatedLocksInTx < 64 {
		out.EstimatedLocksInTx = 64
	}

	// Memory-on-worker estimate (very rough):
	//   ~4 KiB relcache/plancache per table touched, plus the full metadata
	//   cache grown to match coordinator.
	// Reuse the existing metadata estimator's per-shard cost (~1 KiB) as proxy.
	perTableBytes := int64(4 << 10)
	out.EstimatedMemoryMiB = int(
		(perTableBytes*int64(c.DistributedTables) +
			int64(c.Shards)*512 +
			int64(c.DistributedObjects)*1024) >> 20)
	if out.EstimatedMemoryMiB < 1 {
		out.EstimatedMemoryMiB = 1
	}

	// Duration estimate: ~10 ms per DDL in transactional mode. Empirically
	// Citus metadata sync over local TCP processes roughly 100 DDL/sec;
	// remote/WAN links can be 2–5× slower. The previous 2000/sec (0.5 ms
	// per DDL) was an order of magnitude too optimistic and effectively
	// prevented the timeout-risk alarm from ever firing.
	out.EstimatedDurationSec = out.EstimatedDDLCommands / 100
	if out.EstimatedDurationSec < 1 {
		out.EstimatedDurationSec = 1
	}

	// Correlate with GUCs and emit alarms.
	verdict := "ok"

	// Lock exhaustion: a single backend can, in the worst case, consume
	// max_locks_per_transaction × (max_connections + autovacuum + aux) lock
	// slots — that's the shared lock-table size. In practice a long metadata
	// sync shares that table with the rest of the cluster, so the safe
	// single-tx cap is much lower. We use two bands:
	//   - softLockCap = max_locks_per_transaction × 16
	//     (empirical: Citus activation past this triggers LWLock waits)
	//   - hardLockCap = max_locks_per_transaction × max_connections
	//     (absolute shared lock-table ceiling; beyond this the tx will
	//      fail even if no one else is holding locks)
	hardLockCap := g.MaxLocksPerTx * g.MaxConnections
	if in.TargetMaxLocksPerTransaction > 0 {
		hardLockCap = in.TargetMaxLocksPerTransaction * g.MaxConnections
	}
	softLockCap := g.MaxLocksPerTx * 16
	if in.TargetMaxLocksPerTransaction > 0 {
		softLockCap = in.TargetMaxLocksPerTransaction * 16
	}

	// Compute a concrete recommended max_locks_per_transaction value: the
	// smallest multiple of 64 that puts the estimated lock count under the
	// soft cap (×16) with a 25% safety margin.
	if out.EstimatedLocksInTx > softLockCap && in.AssumeSyncMode == "transactional" {
		needed := int(float64(out.EstimatedLocksInTx) * 1.25 / 16.0)
		// Round up to next multiple of 64.
		if needed%64 != 0 {
			needed = ((needed / 64) + 1) * 64
		}
		if needed < 256 {
			needed = 256
		}
		if needed > g.MaxLocksPerTx {
			out.RecommendedMaxLocksPerTransaction = needed
		}
	}

	if out.EstimatedLocksInTx > hardLockCap && in.AssumeSyncMode == "transactional" {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "metadata_sync.locks_exhausted", Severity: diagnostics.SeverityCritical,
			Source: "citus_metadata_sync_risk",
			Message: fmt.Sprintf("Estimated %d locks in single tx > max_locks_per_transaction×max_connections=%d",
				out.EstimatedLocksInTx, hardLockCap),
			Evidence: map[string]any{"estimated_locks": out.EstimatedLocksInTx, "cap": hardLockCap,
				"max_locks_per_transaction": g.MaxLocksPerTx, "max_connections": g.MaxConnections,
				"recommended_max_locks_per_transaction": out.RecommendedMaxLocksPerTransaction},
			FixHint: fmt.Sprintf("Raise max_locks_per_transaction to %d on every node (restart required) OR SET citus.metadata_sync_mode='nontransactional'; before citus_activate_node (must not be inside a BEGIN).",
				out.RecommendedMaxLocksPerTransaction),
		})
		out.Alarms = append(out.Alarms, *a)
		verdict = "blocked"
		if out.RecommendedMaxLocksPerTransaction > 0 {
			out.Recommendations = append(out.Recommendations,
				fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d; (every node, restart required) — brings soft cap to %d, covering estimated %d locks with safety margin",
					out.RecommendedMaxLocksPerTransaction,
					out.RecommendedMaxLocksPerTransaction*16,
					out.EstimatedLocksInTx))
		}
		out.Recommendations = append(out.Recommendations,
			"Alternative: SET citus.metadata_sync_mode = 'nontransactional'; (superuser only, outside a tx) — avoids raising max_locks_per_transaction but sync is no longer atomic")
	} else if out.EstimatedLocksInTx > softLockCap && in.AssumeSyncMode == "transactional" {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "metadata_sync.locks_tight", Severity: diagnostics.SeverityWarning,
			Source: "citus_metadata_sync_risk",
			Message: fmt.Sprintf("Estimated %d locks in single tx approaches max_locks_per_transaction×16=%d",
				out.EstimatedLocksInTx, softLockCap),
			Evidence: map[string]any{"estimated_locks": out.EstimatedLocksInTx, "soft_cap": softLockCap,
				"recommended_max_locks_per_transaction": out.RecommendedMaxLocksPerTransaction},
			FixHint: fmt.Sprintf("Raise max_locks_per_transaction to %d on every node (restart required) OR use nontransactional metadata sync mode.",
				out.RecommendedMaxLocksPerTransaction),
		})
		out.Alarms = append(out.Alarms, *a)
		if out.RecommendedMaxLocksPerTransaction > 0 {
			out.Recommendations = append(out.Recommendations,
				fmt.Sprintf("Recommended: ALTER SYSTEM SET max_locks_per_transaction = %d; (every node, restart required)",
					out.RecommendedMaxLocksPerTransaction))
		}
		if verdict == "ok" {
			verdict = "nontransactional_recommended"
		}
	}

	// OOM risk on target.
	if in.TargetNodeRAMGiB > 0 {
		ramMiB := in.TargetNodeRAMGiB * 1024
		if out.EstimatedMemoryMiB > ramMiB/4 && in.AssumeSyncMode == "transactional" {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "metadata_sync.worker_oom_risk", Severity: diagnostics.SeverityCritical,
				Source: "citus_metadata_sync_risk",
				Message: fmt.Sprintf("Estimated sync memory %d MiB exceeds 25%% of target RAM (%d MiB)",
					out.EstimatedMemoryMiB, ramMiB),
				Evidence: map[string]any{"estimated_mib": out.EstimatedMemoryMiB, "target_ram_mib": ramMiB},
				FixHint: "Use nontransactional mode so memory is released between commands.",
			})
			out.Alarms = append(out.Alarms, *a)
			verdict = "blocked"
		}
	}

	// Timeout correlations.
	if g.StatementTimeoutMs > 0 && out.EstimatedDurationSec*1000 > g.StatementTimeoutMs && in.AssumeSyncMode == "transactional" {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "metadata_sync.timeout_risk", Severity: diagnostics.SeverityWarning,
			Source: "citus_metadata_sync_risk",
			Message: fmt.Sprintf("Estimated sync %ds exceeds statement_timeout=%dms",
				out.EstimatedDurationSec, g.StatementTimeoutMs),
			Evidence: map[string]any{"estimated_sec": out.EstimatedDurationSec, "statement_timeout_ms": g.StatementTimeoutMs},
			FixHint: "SET statement_timeout = 0; for the activation session, or use nontransactional mode.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.Recommendations = append(out.Recommendations, "SET statement_timeout = 0; in the activation session")
	}
	if g.IdleInTxSessionTimeoutMs > 0 && out.EstimatedDurationSec*1000 > g.IdleInTxSessionTimeoutMs {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "metadata_sync.idle_in_tx_risk", Severity: diagnostics.SeverityWarning,
			Source: "citus_metadata_sync_risk",
			Message: fmt.Sprintf("idle_in_transaction_session_timeout=%dms may trip a slow sync",
				g.IdleInTxSessionTimeoutMs),
			Evidence: map[string]any{"idle_in_tx_ms": g.IdleInTxSessionTimeoutMs, "estimated_sec": out.EstimatedDurationSec},
			FixHint: "SET idle_in_transaction_session_timeout = 0; in the activation session.",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	if in.AssumeSyncMode != "nontransactional" && verdict != "ok" {
		out.Recommendations = append(out.Recommendations,
			"Activation flow: SET citus.metadata_sync_mode='nontransactional'; SELECT citus_activate_node('host',port);")
		out.Recommendations = append(out.Recommendations,
			"Non-transactional mode REFUSES to run inside an explicit BEGIN/COMMIT block; run as a single top-level statement.")
	}
	if g.MetadataSyncMode != "transactional" {
		out.Warnings = append(out.Warnings,
			fmt.Sprintf("citus.metadata_sync_mode is already set to %q on the coordinator", g.MetadataSyncMode))
	}

	out.Verdict = verdict
	return nil, out, nil
}
