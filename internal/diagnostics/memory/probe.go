// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// SQL probes that gather TableShape inputs from the coordinator, and live
// measurements of MetadataCacheMemoryContext / CacheMemoryContext on the
// current backend via pg_backend_memory_contexts.

package memory

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fetchShapesSQL returns one row per distributed table (including every
// partition child, including reference tables). Runs on the coordinator.
// Counts:
//   - nshards: from pg_dist_shard
//   - replication_factor: actual placements per shard (max), which for
//     hash-distributed tables is normally 1 and for reference tables is
//     the number of nodes.
//   - nattrs: live attributes (pg_attribute.attnum > 0 and not dropped)
//   - nindexes: non-dropped indexes
//   - nfk_in / nfk_out: FK edges counted from pg_constraint
//   - n_partition_children: from pg_inherits for partitioned parents
//   - is_partition_parent, parent_oid: partitioning metadata
const fetchShapesSQL = `
WITH dist AS (
  SELECT p.logicalrelid AS oid,
         p.partmethod,
         p.repmodel
  FROM pg_dist_partition p
),
shards AS (
  SELECT logicalrelid AS oid, count(*) AS nshards
  FROM pg_dist_shard GROUP BY logicalrelid
),
placements AS (
  -- max placements observed on any shard of the table (models replication_factor
  -- better than AVG because reference tables replicate to all nodes).
  SELECT s.logicalrelid AS oid, max(pc.c) AS max_placements
  FROM pg_dist_shard s
  JOIN (
    SELECT shardid, count(*) AS c FROM pg_dist_placement GROUP BY shardid
  ) pc ON pc.shardid = s.shardid
  GROUP BY s.logicalrelid
),
attrs AS (
  SELECT attrelid AS oid, count(*) AS n
  FROM pg_attribute
  WHERE attnum > 0 AND NOT attisdropped
  GROUP BY attrelid
),
idx AS (
  SELECT indrelid AS oid, count(*) AS n FROM pg_index GROUP BY indrelid
),
fk_out AS (
  SELECT conrelid AS oid, count(*) AS n FROM pg_constraint WHERE contype='f' GROUP BY conrelid
),
fk_in AS (
  SELECT confrelid AS oid, count(*) AS n FROM pg_constraint WHERE contype='f' GROUP BY confrelid
),
partkids AS (
  SELECT inhparent AS oid, count(*) AS n FROM pg_inherits GROUP BY inhparent
)
SELECT
  d.oid::oid::bigint AS oid,
  c.relname,
  n.nspname,
  d.partmethod = 'n'                           AS is_reference,
  c.relkind = 'p'                              AS is_partition_parent,
  COALESCE((SELECT i.inhparent::bigint FROM pg_inherits i WHERE i.inhrelid = d.oid LIMIT 1), 0) AS parent_oid,
  COALESCE(s.nshards, 0)                       AS nshards,
  COALESCE(pl.max_placements, 1)               AS replication_factor,
  COALESCE(a.n, 0)                             AS nattrs,
  COALESCE(ix.n, 0)                            AS nindexes,
  COALESCE(fo.n, 0)                            AS nfk_out,
  COALESCE(fi.n, 0)                            AS nfk_in,
  COALESCE(pk.n, 0)                            AS n_partition_children
FROM dist d
JOIN pg_class c ON c.oid = d.oid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN shards     s  ON s.oid  = d.oid
LEFT JOIN placements pl ON pl.oid = d.oid
LEFT JOIN attrs      a  ON a.oid  = d.oid
LEFT JOIN idx        ix ON ix.oid = d.oid
LEFT JOIN fk_out     fo ON fo.oid = d.oid
LEFT JOIN fk_in      fi ON fi.oid = d.oid
LEFT JOIN partkids   pk ON pk.oid = d.oid
ORDER BY n.nspname, c.relname;
`

// FetchShapes returns one TableShape per distributed table / partition /
// reference table known to the coordinator.
func FetchShapes(ctx context.Context, pool *pgxpool.Pool) ([]TableShape, error) {
	rows, err := pool.Query(ctx, fetchShapesSQL)
	if err != nil {
		return nil, fmt.Errorf("fetch shapes: %w", err)
	}
	defer rows.Close()
	var out []TableShape
	for rows.Next() {
		var s TableShape
		var oid, parentOid int64
		if err := rows.Scan(
			&oid, &s.Name, &s.Schema, &s.IsReference, &s.IsPartitionParent,
			&parentOid, &s.NShards, &s.ReplicationFactor, &s.NAttributes,
			&s.NIndexes, &s.NFKOut, &s.NFKIn, &s.NPartitionChildren,
		); err != nil {
			return nil, err
		}
		s.Oid = oid
		s.ParentOid = parentOid
		out = append(out, s)
	}
	return out, rows.Err()
}

// BackendCtxMeasurement captures observed bytes for a single memory context
// scope (e.g. "MetadataCacheMemoryContext" including all children).
type BackendCtxMeasurement struct {
	Scope        string `json:"scope"`
	TotalBytes   int64  `json:"total_bytes"`
	UsedBytes    int64  `json:"used_bytes"`
	FreeBytes    int64  `json:"free_bytes"`
	NContexts    int    `json:"n_contexts"`
}

// Probe queries pg_backend_memory_contexts on the given connection for the
// subtrees we care about. Returns the aggregated bytes for:
//   - MetadataCacheMemoryContext (Citus)
//   - CacheMemoryContext (PG)
//   - MessageContext, ExecutorState, etc. optionally by filter
//
// NOTE: this observes the CURRENT backend only. To observe *other* backends,
// use pg_log_backend_memory_contexts(pid) and tail the log (covered by B1d).
func Probe(ctx context.Context, conn *pgx.Conn, scopes []string) ([]BackendCtxMeasurement, error) {
	if len(scopes) == 0 {
		scopes = []string{"MetadataCacheMemoryContext", "CacheMemoryContext"}
	}
	// We aggregate each scope as "the named context and all its descendants".
	// pg_backend_memory_contexts lacks a 'path' column in PG 17, so we walk
	// parents iteratively in SQL using a recursive CTE.
	const q = `
WITH RECURSIVE ctxs AS (
  SELECT name, parent, level, total_bytes, used_bytes, free_bytes
  FROM pg_backend_memory_contexts
),
roots AS (
  SELECT name AS scope, name, parent, total_bytes, used_bytes, free_bytes
  FROM ctxs
  WHERE name = ANY($1)
),
tree AS (
  SELECT r.scope, c.name, c.parent, c.total_bytes, c.used_bytes, c.free_bytes
  FROM roots r JOIN ctxs c ON c.name = r.name
  UNION ALL
  SELECT t.scope, c.name, c.parent, c.total_bytes, c.used_bytes, c.free_bytes
  FROM tree t JOIN ctxs c ON c.parent = t.name
)
SELECT scope,
       SUM(total_bytes)::bigint,
       SUM(used_bytes)::bigint,
       SUM(free_bytes)::bigint,
       COUNT(*)::int
FROM tree
GROUP BY scope
ORDER BY scope;
`
	rows, err := conn.Query(ctx, q, scopes)
	if err != nil {
		return nil, fmt.Errorf("probe memory contexts: %w", err)
	}
	defer rows.Close()
	var out []BackendCtxMeasurement
	for rows.Next() {
		var m BackendCtxMeasurement
		if err := rows.Scan(&m.Scope, &m.TotalBytes, &m.UsedBytes, &m.FreeBytes, &m.NContexts); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// TouchAllDistributedMetadata forces population of the MetadataCacheMemoryContext
// on the current backend by asking Citus about every distributed table.
// Returns number of distributed tables touched. Intended for "before/after"
// measurements against a cold connection.
func TouchAllDistributedMetadata(ctx context.Context, conn *pgx.Conn) (int, error) {
	// A single SELECT from citus_tables / pg_dist_partition is NOT enough to
	// populate per-table cache entries; Citus lazily builds them on use.
	// Use citus_table_size on every dist table to force entry construction.
	// Failures are tolerated (e.g. permission); we report best-effort count.
	var touched int
	rows, err := conn.Query(ctx, `SELECT logicalrelid::regclass::text FROM pg_dist_partition`)
	if err != nil {
		return 0, err
	}
	var rels []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return 0, err
		}
		rels = append(rels, s)
	}
	rows.Close()
	for _, r := range rels {
		// Use a cheap metadata-touching function instead of scanning data.
		// citus_relation_size touches CitusTableCacheEntry without reading rows.
		if _, err := conn.Exec(ctx, "SELECT citus_relation_size($1)", r); err == nil {
			touched++
		}
	}
	return touched, nil
}
