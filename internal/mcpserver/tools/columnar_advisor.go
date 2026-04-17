// citus-mcp: B18 citus_columnar_advisor -- scans partitions of distributed
// tables looking for candidates to convert to the citus_columnar access
// method. A good candidate is:
//   - A time-range partition on a distributed table
//   - Not the "current" partition (older, write-cold)
//   - Large (dominates the parent table's size)
//   - Has few updates/deletes (n_tup_upd + n_tup_del) in pg_stat_user_tables
//
// Output is a ranked list; we do not convert anything automatically.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ColumnarAdvisorInput struct {
	MinPartitionBytes int64 `json:"min_partition_bytes,omitempty"` // default 1 GiB
	TopN              int   `json:"top_n,omitempty"`               // default 20
}

type ColumnarCandidate struct {
	Parent            string  `json:"parent"`
	Partition         string  `json:"partition"`
	TotalBytes        int64   `json:"total_bytes"`
	CurrentAM         string  `json:"current_access_method"`
	UpdateDeleteShare float64 `json:"update_delete_share"`
	Reason            string  `json:"reason"`
}

type ColumnarAdvisorOutput struct {
	Candidates []ColumnarCandidate `json:"candidates"`
	Alarms     []diagnostics.Alarm `json:"alarms"`
	Warnings   []string            `json:"warnings,omitempty"`
}

func ColumnarAdvisorTool(ctx context.Context, deps Dependencies, in ColumnarAdvisorInput) (*mcp.CallToolResult, ColumnarAdvisorOutput, error) {
	out := ColumnarAdvisorOutput{Candidates: []ColumnarCandidate{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.MinPartitionBytes == 0 {
		in.MinPartitionBytes = 1 << 30
	}
	if in.TopN == 0 {
		in.TopN = 20
	}
	// citus_columnar must be present.
	var hasColumnar bool
	_ = deps.Pool.QueryRow(ctx,
		`SELECT count(*)>0 FROM pg_extension WHERE extname='citus_columnar'`).Scan(&hasColumnar)
	if !hasColumnar {
		out.Warnings = append(out.Warnings, "citus_columnar extension not installed — columnar conversion unavailable")
	}

	// Find partitions of distributed tables with total size + stats.
	q := `
WITH parts AS (
  SELECT
    i.inhparent::regclass::text AS parent,
    i.inhrelid::regclass::text  AS partition,
    c.relam, am.amname AS access_method,
    pg_catalog.pg_total_relation_size(i.inhrelid) AS bytes
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_am am ON am.oid = c.relam
  WHERE EXISTS (SELECT 1 FROM pg_dist_partition p WHERE p.logicalrelid = i.inhparent)
),
stats AS (
  SELECT (schemaname||'.'||relname)::text AS q, n_tup_ins, n_tup_upd, n_tup_del
  FROM pg_stat_user_tables
)
SELECT parts.parent, parts.partition, parts.bytes, parts.access_method,
       COALESCE(s.n_tup_upd,0)::bigint, COALESCE(s.n_tup_del,0)::bigint,
       COALESCE(s.n_tup_ins,0)::bigint
FROM parts
LEFT JOIN stats s ON s.q = parts.partition
WHERE parts.bytes >= $1
ORDER BY parts.bytes DESC
LIMIT $2`
	rows, err := deps.Pool.Query(ctx, q, in.MinPartitionBytes, in.TopN)
	if err != nil {
		return nil, out, fmt.Errorf("partition scan failed: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c ColumnarCandidate
		var upd, del, ins int64
		if err := rows.Scan(&c.Parent, &c.Partition, &c.TotalBytes, &c.CurrentAM, &upd, &del, &ins); err != nil {
			continue
		}
		total := upd + del + ins
		if total > 0 {
			c.UpdateDeleteShare = float64(upd+del) / float64(total)
		}
		switch {
		case c.CurrentAM == "columnar":
			c.Reason = "already columnar"
		case c.UpdateDeleteShare > 0.05:
			c.Reason = fmt.Sprintf("too many updates/deletes (%.1f%%) — columnar is append-only friendly", c.UpdateDeleteShare*100)
		default:
			c.Reason = fmt.Sprintf("good candidate: %d MiB, %.1f%% upd/del", c.TotalBytes>>20, c.UpdateDeleteShare*100)
		}
		out.Candidates = append(out.Candidates, c)
	}
	return nil, out, nil
}
