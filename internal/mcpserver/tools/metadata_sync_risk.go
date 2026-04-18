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
	MaxWorkerProcesses      int    `json:"max_worker_processes"`
	AutovacuumMaxWorkers    int    `json:"autovacuum_max_workers"`
	MaxPreparedTransactions int    `json:"max_prepared_transactions"`
	// MaxBackends is the effective value PostgreSQL uses for NLOCKENTS:
	//   MaxBackends = max_connections + autovacuum_max_workers
	//               + max_worker_processes + max_wal_senders
	// (src/backend/utils/init/postinit.c:InitializeMaxBackends)
	MaxBackends int `json:"max_backends"`
	// LockTableCapacity is the shared lock-hash size:
	//   max_locks_per_transaction * (MaxBackends + max_prepared_transactions)
	// (src/backend/storage/lmgr/lock.c: NLOCKENTS macro)
	LockTableCapacity int `json:"lock_table_capacity"`
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
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_worker_processes')::int`).Scan(&g.MaxWorkerProcesses)
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('autovacuum_max_workers')::int`).Scan(&g.AutovacuumMaxWorkers)
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('max_prepared_transactions')::int`).Scan(&g.MaxPreparedTransactions)
	// Compute MaxBackends / lock-table capacity per PG's NLOCKENTS (lock.c).
	g.MaxBackends = g.MaxConnections + g.AutovacuumMaxWorkers + g.MaxWorkerProcesses + g.MaxWalSenders
	g.LockTableCapacity = g.MaxLocksPerTx * (g.MaxBackends + g.MaxPreparedTransactions)

	// Average indexes per distributed relation (incl. partition children).
	// Used by the lock-demand model: a single CREATE TABLE + CREATE INDEX
	// transaction on the candidate holds (tables) * (1 + avg_idx) relation
	// locks plus catalog slack.
	var avgIdx float64
	_ = deps.Pool.QueryRow(ctx, `
SELECT COALESCE(AVG(idx_count), 0)::float
FROM (
  SELECT p.logicalrelid AS relid, COUNT(i.indexrelid) AS idx_count
  FROM pg_dist_partition p
  LEFT JOIN pg_index i ON i.indrelid = p.logicalrelid
  GROUP BY p.logicalrelid
) t`).Scan(&avgIdx)
	if avgIdx < 0 {
		avgIdx = 0
	}

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

	// Locks-in-tx estimate: on the candidate, metadata-sync runs one tx that
	// creates every shell distributed relation (c.DistributedTables already
	// counts partition children, per pg_dist_partition) plus their indexes,
	// and touches pg_dist_* catalogs. Peak held locks:
	//   dist_tables * (1 + avg_indexes_per_table)                 (table + index relation locks)
	//   + distributed_objects                                      (types/schemas/seqs/funcs/exts)
	//   + 16                                                       (pg_dist_* catalog slack)
	// No artificial floor — report the true predicted number so operators
	// can compare directly against lpt * (MaxBackends + max_prepared_xacts).
	out.EstimatedLocksInTx = int(float64(c.DistributedTables)*(1.0+avgIdx)) + c.DistributedObjects + 16

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

	// Lock exhaustion: the actual PostgreSQL shared lock-hash capacity is
	//   NLOCKENTS = max_locks_per_transaction
	//             × (MaxBackends + max_prepared_transactions)
	// where MaxBackends = max_connections + autovacuum_max_workers
	//                   + max_worker_processes + max_wal_senders
	// (see src/backend/storage/lmgr/lock.c and postinit.c).
	// The *hard* ceiling is that whole number — beyond it the new lock
	// entry can't be admitted even if no one else holds locks. The *soft*
	// cap is a safety fraction of the hard cap so we don't starve other
	// concurrent work during a long activation.
	effLpt := g.MaxLocksPerTx
	if in.TargetMaxLocksPerTransaction > 0 {
		effLpt = in.TargetMaxLocksPerTransaction
	}
	softLockCap, hardLockCap := computeLockCaps(effLpt, g.MaxBackends, g.MaxPreparedTransactions)

	// Compute a concrete recommended max_locks_per_transaction value that
	// keeps the estimated lock count under the soft cap with a 25% safety
	// margin. Solve: locks × 1.25 ≤ lpt × (MaxBackends + mpx) / 2
	//           →   lpt ≥ 2.5 × locks / (MaxBackends + mpx)
	// Round up to the next multiple of 64 (PG's alignment granularity).
	if out.EstimatedLocksInTx > softLockCap && in.AssumeSyncMode == "transactional" {
		denom := g.MaxBackends + g.MaxPreparedTransactions
		if denom < 1 {
			denom = 1
		}
		needed := int(2.5 * float64(out.EstimatedLocksInTx) / float64(denom))
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
			Message: fmt.Sprintf("Estimated %d locks in single tx exceeds NLOCKENTS = max_locks_per_transaction(%d) × (MaxBackends(%d) + max_prepared_transactions(%d)) = %d",
				out.EstimatedLocksInTx, effLpt, g.MaxBackends, g.MaxPreparedTransactions, hardLockCap),
			Evidence: map[string]any{"estimated_locks": out.EstimatedLocksInTx, "cap": hardLockCap,
				"max_locks_per_transaction":   g.MaxLocksPerTx,
				"max_backends":                g.MaxBackends,
				"max_prepared_transactions":   g.MaxPreparedTransactions,
				"recommended_max_locks_per_transaction": out.RecommendedMaxLocksPerTransaction},
			FixHint: fmt.Sprintf("Raise max_locks_per_transaction to %d on every node (restart required) OR SET citus.metadata_sync_mode='nontransactional'; before citus_activate_node (must not be inside a BEGIN).",
				out.RecommendedMaxLocksPerTransaction),
		})
		out.Alarms = append(out.Alarms, *a)
		verdict = "blocked"
		if out.RecommendedMaxLocksPerTransaction > 0 {
			newSoft := out.RecommendedMaxLocksPerTransaction * (g.MaxBackends + g.MaxPreparedTransactions) / 2
			out.Recommendations = append(out.Recommendations,
				fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d; (every node, restart required) — brings soft cap to %d (= lpt × (MaxBackends+max_prepared_xacts) / 2), covering estimated %d locks with safety margin",
					out.RecommendedMaxLocksPerTransaction,
					newSoft,
					out.EstimatedLocksInTx))
		}
		out.Recommendations = append(out.Recommendations,
			"Alternative: SET citus.metadata_sync_mode = 'nontransactional'; (superuser only, outside a tx) — avoids raising max_locks_per_transaction but sync is no longer atomic")
	} else if out.EstimatedLocksInTx > softLockCap && in.AssumeSyncMode == "transactional" {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "metadata_sync.locks_tight", Severity: diagnostics.SeverityWarning,
			Source: "citus_metadata_sync_risk",
			Message: fmt.Sprintf("Estimated %d locks in single tx approaches NLOCKENTS soft cap (%d = 50%% of lpt × (MaxBackends+max_prepared_xacts))",
				out.EstimatedLocksInTx, softLockCap),
			Evidence: map[string]any{"estimated_locks": out.EstimatedLocksInTx, "soft_cap": softLockCap,
				"hard_cap": hardLockCap,
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

// computeLockCaps returns the soft and hard ceilings for locks held by a
// single metadata-sync transaction, both derived from PostgreSQL's actual
// lock-hash sizing formula (src/backend/storage/lmgr/lock.c, NLOCKENTS):
//
//	hard = max_locks_per_transaction × (MaxBackends + max_prepared_transactions)
//	soft = hard / 2                                 (leaves half for other work)
//
// with a small-cluster floor of lpt × 16 to avoid reporting implausibly tiny
// soft caps on dev installs. Exported via the unexported path for testing.
func computeLockCaps(lpt, maxBackends, maxPrepared int) (soft, hard int) {
	if lpt < 1 {
		lpt = 1
	}
	hard = lpt * (maxBackends + maxPrepared)
	soft = hard / 2
	if floor := lpt * 16; soft < floor {
		soft = floor
	}
	return soft, hard
}
