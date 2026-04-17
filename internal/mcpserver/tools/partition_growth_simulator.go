// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B1c: citus_partition_growth_simulator --
// Pure what-if: given proposed changes to partition counts on distributed
// partitioned tables, project the resulting per-backend and cluster-wide
// memory footprint of MetadataCacheMemoryContext + PG CacheMemoryContext.
// This is the tool that would have answered "is doubling partitions on
// these N tables safe?" before the user hit OOM.

package tools

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/diagnostics/memory"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// PartitionChange describes a what-if change.
type PartitionChange struct {
	// Table identifies the partitioned parent. Accepts "schema.name" or
	// "name" (implicit "public").
	Table string `json:"table"`

	// NewPartitionCount is the target number of partition children.
	// Required. Must be >= 0.
	NewPartitionCount int `json:"new_partition_count"`
}

// PartitionGrowthInput controls the simulation.
type PartitionGrowthInput struct {
	// Changes lists specific tables + target partition counts. Mutually
	// exclusive with GlobalMultiplier.
	Changes []PartitionChange `json:"changes,omitempty"`

	// GlobalMultiplier scales the partition count of EVERY partitioned
	// distributed table by this factor. E.g. 2.0 simulates "doubling
	// partitions everywhere". Ignored if Changes is non-empty.
	GlobalMultiplier float64 `json:"global_multiplier,omitempty"`

	// IncludeShardRels models a worker-in-MX view for PG CacheMemoryContext.
	IncludeShardRels bool `json:"include_shard_rels,omitempty"`

	// ActiveBackendsOverride: if >0, use for cluster projection instead of
	// probing pg_stat_activity.
	ActiveBackendsOverride int `json:"active_backends_override,omitempty"`
}

// PartitionGrowthOutput is the tool response.
type PartitionGrowthOutput struct {
	Baseline          GrowthSnapshot       `json:"baseline"`
	Projected         GrowthSnapshot       `json:"projected"`
	Delta             GrowthDelta          `json:"delta"`
	AppliedChanges    []AppliedChange      `json:"applied_changes"`
	Alarms            []diagnostics.Alarm  `json:"alarms"`
	Warnings          []string             `json:"warnings,omitempty"`
}

// GrowthSnapshot pairs both estimators with cluster projection.
type GrowthSnapshot struct {
	Citus              memory.CitusEstimate   `json:"citus"`
	PgCache            memory.PgCacheEstimate `json:"pg_cache"`
	PerBackendBytes    int64                  `json:"per_backend_bytes"`
	MaxConnectionBytes int64                  `json:"max_connection_bytes"`
	ActiveBackendBytes int64                  `json:"active_backend_bytes"`
}

// GrowthDelta reports the difference projected − baseline.
type GrowthDelta struct {
	PerBackendBytes    int64  `json:"per_backend_bytes"`
	MaxConnectionBytes int64  `json:"max_connection_bytes"`
	ActiveBackendBytes int64  `json:"active_backend_bytes"`
	PerBackendPct      float64 `json:"per_backend_pct"`
}

// AppliedChange records what the simulator actually did per table.
type AppliedChange struct {
	Table              string `json:"table"`
	OldPartitionCount  int    `json:"old_partition_count"`
	NewPartitionCount  int    `json:"new_partition_count"`
	PerChildShards     int    `json:"per_child_shards"`
	ReplicationFactor  int    `json:"replication_factor"`
	AddedEntries       int    `json:"added_cache_entries"`
	AddedShards        int    `json:"added_shards"`
}

// PartitionGrowthTool implements the what-if simulator.
func PartitionGrowthTool(ctx context.Context, deps Dependencies, in PartitionGrowthInput) (*mcp.CallToolResult, PartitionGrowthOutput, error) {
	out := PartitionGrowthOutput{
		Alarms: []diagnostics.Alarm{}, Warnings: []string{}, AppliedChanges: []AppliedChange{},
	}
	if len(in.Changes) == 0 && in.GlobalMultiplier == 0 {
		return callError(serr.CodeInvalidInput, "provide changes[] or global_multiplier", ""), out, nil
	}
	if in.GlobalMultiplier < 0 {
		return callError(serr.CodeInvalidInput, "global_multiplier must be >= 0", ""), out, nil
	}

	baseShapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}

	// Build lookups on baseShapes.
	// 1) parents by qualified name, 2) children grouped by parent oid.
	type parentInfo struct {
		idx            int // index in baseShapes
		qualifiedName  string
		currentKids    []int // indices in baseShapes
	}
	parents := make(map[string]*parentInfo) // key = "schema.name"
	childrenByParent := make(map[int64][]int)
	oidToIdx := make(map[int64]int, len(baseShapes))
	for i, s := range baseShapes {
		oidToIdx[s.Oid] = i
		if s.IsPartitionParent {
			qn := s.Schema + "." + s.Name
			parents[qn] = &parentInfo{idx: i, qualifiedName: qn}
			// second pass to attach kids (below)
		}
	}
	for i, s := range baseShapes {
		if s.ParentOid != 0 {
			childrenByParent[s.ParentOid] = append(childrenByParent[s.ParentOid], i)
		}
	}
	for _, p := range parents {
		p.currentKids = childrenByParent[baseShapes[p.idx].Oid]
	}

	// Resolve requested changes into a concrete plan.
	// If GlobalMultiplier is set and Changes is empty, expand to per-parent.
	plan := make(map[string]int) // key = qualified "schema.name", value = target child count
	if len(in.Changes) > 0 {
		for _, c := range in.Changes {
			qn := normalizeTable(c.Table)
			if _, ok := parents[qn]; !ok {
				out.Warnings = append(out.Warnings, fmt.Sprintf("%q is not a partitioned distributed table; skipped", c.Table))
				continue
			}
			if c.NewPartitionCount < 0 {
				return callError(serr.CodeInvalidInput, fmt.Sprintf("%s: new_partition_count must be >= 0", qn), ""), out, nil
			}
			plan[qn] = c.NewPartitionCount
		}
	} else {
		for qn, p := range parents {
			cur := len(p.currentKids)
			plan[qn] = int(float64(cur) * in.GlobalMultiplier)
		}
	}

	// Baseline snapshot.
	out.Baseline = memSnapshot(baseShapes, in.IncludeShardRels)

	// Project: produce a new shapes slice reflecting the plan. We keep the
	// parent row unchanged, adjust NPartitionChildren on the parent, and
	// add/remove child rows that inherit the parent's shape (same shard
	// count, RF, attrs, indexes — a reasonable simplifying assumption for
	// new partitions added via CREATE TABLE ... PARTITION OF).
	projShapes := make([]memory.TableShape, 0, len(baseShapes)+32)
	removed := make(map[int]bool) // indices of baseShapes to drop (trimming children)
	for qn, target := range plan {
		p := parents[qn]
		parent := baseShapes[p.idx]
		cur := len(p.currentKids)
		switch {
		case target < cur:
			// Mark the last (target..cur) children for removal.
			for _, idx := range p.currentKids[target:] {
				removed[idx] = true
			}
		}
		// (additions handled below after we've written existing rows)
		_ = parent
	}

	for i, s := range baseShapes {
		if removed[i] {
			continue
		}
		if s.IsPartitionParent {
			qn := s.Schema + "." + s.Name
			if tgt, ok := plan[qn]; ok {
				s.NPartitionChildren = tgt
			}
		}
		projShapes = append(projShapes, s)
	}

	// Now add new children for any parent where target > current.
	for qn, target := range plan {
		p := parents[qn]
		cur := len(p.currentKids)
		if target <= cur {
			continue
		}
		// Template from first existing child if any, else from the parent
		// itself (parent has the same NShards/RF as each child by Citus
		// semantics: distributed partitioned tables have same shard count
		// per child).
		var tmpl memory.TableShape
		if cur > 0 {
			tmpl = baseShapes[p.currentKids[0]]
		} else {
			tmpl = baseShapes[p.idx]
			tmpl.IsPartitionParent = false
		}
		tmpl.ParentOid = baseShapes[p.idx].Oid
		tmpl.IsPartitionParent = false

		added := target - cur
		for i := 0; i < added; i++ {
			nc := tmpl
			nc.Name = fmt.Sprintf("%s_wi%d", baseShapes[p.idx].Name, i+1)
			projShapes = append(projShapes, nc)
		}

		rf := tmpl.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		out.AppliedChanges = append(out.AppliedChanges, AppliedChange{
			Table: qn, OldPartitionCount: cur, NewPartitionCount: target,
			PerChildShards: tmpl.NShards, ReplicationFactor: rf,
			AddedEntries: target - cur, AddedShards: (target - cur) * tmpl.NShards,
		})
	}

	// Record shrinks too (negative deltas).
	for qn, target := range plan {
		p := parents[qn]
		cur := len(p.currentKids)
		if target >= cur {
			continue
		}
		var tmpl memory.TableShape
		if cur > 0 {
			tmpl = baseShapes[p.currentKids[0]]
		} else {
			tmpl = baseShapes[p.idx]
		}
		rf := tmpl.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		out.AppliedChanges = append(out.AppliedChanges, AppliedChange{
			Table: qn, OldPartitionCount: cur, NewPartitionCount: target,
			PerChildShards: tmpl.NShards, ReplicationFactor: rf,
			AddedEntries: target - cur,             // negative
			AddedShards:  (target - cur) * tmpl.NShards, // negative
		})
	}

	out.Projected = memSnapshot(projShapes, in.IncludeShardRels)

	// Connection counts for cluster projection.
	activeBackends := in.ActiveBackendsOverride
	maxConns := 0
	if conn, aerr := deps.Pool.Acquire(ctx); aerr == nil {
		defer conn.Release()
		_ = conn.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&maxConns)
		if activeBackends <= 0 {
			_ = conn.QueryRow(ctx, `SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend'`).Scan(&activeBackends)
		}
	}
	out.Baseline.MaxConnectionBytes = out.Baseline.PerBackendBytes * int64(maxConns)
	out.Baseline.ActiveBackendBytes = out.Baseline.PerBackendBytes * int64(activeBackends)
	out.Projected.MaxConnectionBytes = out.Projected.PerBackendBytes * int64(maxConns)
	out.Projected.ActiveBackendBytes = out.Projected.PerBackendBytes * int64(activeBackends)

	out.Delta = GrowthDelta{
		PerBackendBytes:    out.Projected.PerBackendBytes - out.Baseline.PerBackendBytes,
		MaxConnectionBytes: out.Projected.MaxConnectionBytes - out.Baseline.MaxConnectionBytes,
		ActiveBackendBytes: out.Projected.ActiveBackendBytes - out.Baseline.ActiveBackendBytes,
	}
	if out.Baseline.PerBackendBytes > 0 {
		out.Delta.PerBackendPct = float64(out.Delta.PerBackendBytes) * 100.0 / float64(out.Baseline.PerBackendBytes)
	}

	// Alarms: big jump detection.
	if deps.Alarms != nil && out.Baseline.PerBackendBytes > 0 {
		switch {
		case out.Delta.PerBackendPct >= 100.0:
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind:     "partition_growth.impact_high",
				Severity: diagnostics.SeverityCritical,
				Source:   "citus_partition_growth_simulator",
				Message:  fmt.Sprintf("Proposed change more than doubles per-backend cache footprint (+%.0f%% / +%d bytes)", out.Delta.PerBackendPct, out.Delta.PerBackendBytes),
				Evidence: map[string]any{
					"delta_per_backend":    out.Delta.PerBackendBytes,
					"delta_max_conn":       out.Delta.MaxConnectionBytes,
					"per_backend_pct":      out.Delta.PerBackendPct,
					"applied_changes":      out.AppliedChanges,
				},
				FixHint: "Reconsider partition count; consider fewer shards per table; validate max_connections budget.",
			})
			out.Alarms = append(out.Alarms, *a)
		case out.Delta.PerBackendPct >= 25.0:
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind:     "partition_growth.impact_notable",
				Severity: diagnostics.SeverityWarning,
				Source:   "citus_partition_growth_simulator",
				Message:  fmt.Sprintf("Proposed change raises per-backend cache footprint by %.0f%% (+%d bytes)", out.Delta.PerBackendPct, out.Delta.PerBackendBytes),
				Evidence: map[string]any{
					"delta_per_backend": out.Delta.PerBackendBytes,
					"per_backend_pct":   out.Delta.PerBackendPct,
				},
				FixHint: "Validate OOM headroom: delta × max_connections should fit node RAM after shared_buffers and OS.",
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}

	return nil, out, nil
}

func memSnapshot(shapes []memory.TableShape, includeShardRels bool) GrowthSnapshot {
	c := memory.EstimateCitusMetadata(shapes)
	p := memory.EstimatePgCache(shapes, includeShardRels)
	return GrowthSnapshot{
		Citus: c, PgCache: p,
		PerBackendBytes: c.Bytes + p.Bytes,
	}
}

func normalizeTable(t string) string {
	t = strings.TrimSpace(t)
	if !strings.Contains(t, ".") {
		return "public." + t
	}
	return t
}
