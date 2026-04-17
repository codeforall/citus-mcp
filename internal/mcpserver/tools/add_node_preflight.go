// citus-mcp: B27 citus_add_node_preflight -- CHECKLIST-ONLY advisory.
// Does NOT connect to the target host. Instead, reads the coordinator's
// state (PG version, citus version, required extensions, DB locale/
// encoding, distributed extensions in pg_dist_object) and emits an
// explicit preparation checklist the operator must satisfy on the new
// node BEFORE running citus_add_node + citus_activate_node.
//
// Refs (Citus source):
//  node_metadata.c:ActivateNodeList   -> EnsureCoordinator, EnsureSuperUser
//  metadata_sync.c:SyncDistributedObjects  -> pg_dist_object propagation
//  node_metadata.c:AddNodeMetadata          -> EnsureCoordinator

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type AddNodePreflightInput struct {
	NodeRole string `json:"node_role,omitempty"` // "worker" (default) or "mx-worker"
}

type PreflightItem struct {
	Key          string `json:"key"`
	Requirement  string `json:"requirement"`
	Reason       string `json:"reason"`
	Check        string `json:"check"`            // SQL/shell snippet the operator runs ON THE TARGET
	Severity     string `json:"severity"`         // critical / warning / info
}

type AddNodePreflightOutput struct {
	PgMajorVersion        int             `json:"pg_major_version_required"`
	CitusVersion          string          `json:"citus_version_required"`
	DatabaseName          string          `json:"database_name"`
	Encoding              string          `json:"encoding"`
	Collation             string          `json:"collation"`
	CType                 string          `json:"ctype"`
	RequiredExtensions    []string        `json:"required_extensions"`
	DistributedExtensions []string        `json:"distributed_extensions"`
	Checklist             []PreflightItem `json:"checklist"`
	Alarms                []diagnostics.Alarm `json:"alarms"`
	Warnings              []string        `json:"warnings,omitempty"`
}

func AddNodePreflightTool(ctx context.Context, deps Dependencies, in AddNodePreflightInput) (*mcp.CallToolResult, AddNodePreflightOutput, error) {
	out := AddNodePreflightOutput{
		RequiredExtensions:    []string{},
		DistributedExtensions: []string{},
		Checklist:             []PreflightItem{},
		Alarms:                []diagnostics.Alarm{},
		Warnings:              []string{},
	}
	if in.NodeRole == "" {
		in.NodeRole = "worker"
	}

	// 1) PG major version.
	var serverVersionNum int
	_ = deps.Pool.QueryRow(ctx, `SHOW server_version_num`).Scan(new(string))
	_ = deps.Pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&serverVersionNum)
	out.PgMajorVersion = serverVersionNum / 10000

	// 2) Citus version.
	_ = deps.Pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT extversion FROM pg_extension WHERE extname='citus'),'')`).Scan(&out.CitusVersion)
	if out.CitusVersion == "" {
		out.Warnings = append(out.Warnings, "citus extension not found on coordinator — preflight cannot continue")
		return nil, out, nil
	}

	// 3) Database locale/encoding.
	_ = deps.Pool.QueryRow(ctx, `
SELECT datname, pg_encoding_to_char(encoding), datcollate, datctype
FROM pg_database WHERE datname = current_database()`).
		Scan(&out.DatabaseName, &out.Encoding, &out.Collation, &out.CType)

	// 4) Required extensions = everything installed on coordinator.
	rows, err := deps.Pool.Query(ctx,
		`SELECT extname || ' ' || extversion FROM pg_extension ORDER BY extname`)
	if err == nil {
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err == nil {
				out.RequiredExtensions = append(out.RequiredExtensions, s)
			}
		}
		rows.Close()
	}

	// 5) Distributed extensions from pg_dist_object.
	rows, err = deps.Pool.Query(ctx, `
SELECT DISTINCT e.extname
FROM pg_dist_object d
JOIN pg_extension e ON e.oid = d.objid
WHERE d.classid = 'pg_extension'::regclass
ORDER BY 1`)
	if err == nil {
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err == nil {
				out.DistributedExtensions = append(out.DistributedExtensions, s)
			}
		}
		rows.Close()
	}

	// 6) Build the checklist.
	add := func(key, req, reason, check, sev string) {
		out.Checklist = append(out.Checklist, PreflightItem{
			Key: key, Requirement: req, Reason: reason, Check: check, Severity: sev,
		})
	}

	add("pg_major_version",
		fmt.Sprintf("PostgreSQL major version %d", out.PgMajorVersion),
		"All nodes in a Citus cluster must run the same PG major version. Minor versions can differ but major must match.",
		`SELECT current_setting('server_version_num')::int / 10000;  -- must equal coordinator`,
		"critical")

	add("citus_extension_version",
		fmt.Sprintf("citus extension version %s", out.CitusVersion),
		"Citus requires EXACT same citus extension version on every node. Version mismatch is unsupported and will corrupt metadata sync.",
		fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS citus VERSION '%s';  -- if installed, verify: SELECT extversion FROM pg_extension WHERE extname='citus';`, out.CitusVersion),
		"critical")

	add("shared_preload_libraries",
		"shared_preload_libraries contains 'citus'",
		"citus must be loaded at postmaster start; loading via LOAD after start is insufficient.",
		`SHOW shared_preload_libraries;  -- must contain 'citus'`,
		"critical")

	add("database_exists",
		fmt.Sprintf("database %q exists with encoding=%s collate=%s ctype=%s", out.DatabaseName, out.Encoding, out.Collation, out.CType),
		"The target database must exist on the new node with matching encoding/collation. Mismatch causes data corruption or silent sort-order bugs.",
		fmt.Sprintf(`CREATE DATABASE %s WITH ENCODING '%s' LC_COLLATE '%s' LC_CTYPE '%s' TEMPLATE template0;`, out.DatabaseName, out.Encoding, out.Collation, out.CType),
		"critical")

	add("superuser_access",
		"Coordinator can connect to new node as a superuser",
		"citus_activate_node propagates non-owned objects and REQUIRES a superuser connection (EnsureSuperUser in ActivateNodeList).",
		`-- On new node:
CREATE ROLE citus_admin LOGIN SUPERUSER PASSWORD '…';
-- In pg_hba.conf on new node, allow coordinator IP with this user.`,
		"critical")

	// Required extensions — distinguish distributed (auto-propagated but must be available) vs plain required.
	if len(out.DistributedExtensions) > 0 {
		add("distributed_extensions_available",
			fmt.Sprintf("pg_available_extensions on new node includes: %v", out.DistributedExtensions),
			"Extensions referenced by pg_dist_object (distributed extensions) must be INSTALLABLE on the new node. Citus will CREATE them during activation — if the shared object isn't on disk, activation will fail mid-sync.",
			`SELECT name FROM pg_available_extensions WHERE name = ANY(ARRAY[…]);  -- must return all distributed extensions`,
			"critical")
	}
	add("all_extensions_available",
		fmt.Sprintf("pg_available_extensions covers coordinator set: %v", out.RequiredExtensions),
		"Any extension present on coordinator but not on the new node becomes a drift source. Install the .so/.control files BEFORE activating.",
		`-- Compare (on new node): SELECT name || ' ' || default_version FROM pg_available_extensions ORDER BY 1;`,
		"warning")

	// Reverse-path auth is required for MX (workers connect to each other).
	if in.NodeRole == "mx-worker" {
		add("mx_reverse_auth",
			"New node can INITIATE connections to every other worker and the coordinator",
			"In MX mode any worker can act as a query entry point and will open connections to every other worker. Coordinator-only pg_hba.conf is insufficient.",
			`-- On every existing worker + coordinator, ensure pg_hba.conf allows the new node's IP.
-- Test from new node:  psql -h <other_worker> -U <user> -c 'SELECT 1'`,
			"critical")
		add("mx_hasmetadata",
			"After add, expect hasmetadata=true and metadatasynced=true on the new node",
			"MX mode requires the full pg_dist_* catalog on every worker. hasmetadata=false disables MX routing for that node.",
			`-- After citus_activate_node:
SELECT nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node WHERE noderole='primary';`,
			"info")
	}

	// Key GUCs the new node must satisfy (subset; metadata_sync_risk covers the detail).
	add("wal_level",
		"wal_level=logical (recommended for later shard moves)",
		"citus_move_shard_placement uses logical replication; wal_level<logical means subsequent rebalance is impossible without restart.",
		`SHOW wal_level;  -- must be 'logical'`,
		"warning")
	add("replication_slots",
		"max_wal_senders and max_replication_slots allow incoming shard moves",
		"Default values (10 each on modern PG) are usually fine; tiny tuned instances miss this.",
		`SHOW max_wal_senders; SHOW max_replication_slots;  -- each >= 10 recommended`,
		"info")
	add("max_connections_floor",
		"max_connections >= coordinator's max_connections (MX: >= fleet mesh pressure)",
		"In MX mode the new node receives connections from every other node + clients. Under-sized max_connections causes fanout failures.",
		`SHOW max_connections;`,
		"warning")
	add("max_locks_per_transaction",
		"max_locks_per_transaction must match (or exceed) the coordinator's value",
		"Citus locks every shard within a single transaction during metadata sync, rebalance, and multi-shard DDL. A new node with a lower value will fail 'out of shared memory' when those operations reach it. Recommend >=256 for <=1k shards, >=1024 for 1k-10k shards, >=2048 above that.",
		`SHOW max_locks_per_transaction;  -- compare to coordinator, restart required to change`,
		"critical")
	add("max_worker_processes_floor",
		"max_worker_processes must match (or exceed) the coordinator's value",
		"Citus parallel executor workers are counted against max_worker_processes on every node. A lower value on the new node silently reduces cluster-wide parallelism and may stall metadata sync.",
		`SHOW max_worker_processes;  -- compare to coordinator`,
		"warning")

	return nil, out, nil
}
