// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B1b: citus_pg_cache_footprint --
// Estimate and optionally measure the per-backend PG CacheMemoryContext
// (relcache + catcache + partcache + plancache) footprint driven by the
// shape of distributed tables + partitions + shards. Runs separately from
// B1 because these two caches grow under different drivers and are often
// the larger term when users add partitions to distributed tables.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/diagnostics/memory"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// PgCacheFootprintInput controls the probe.
type PgCacheFootprintInput struct {
	// Measure queries pg_backend_memory_contexts for CacheMemoryContext
	// (and descendants) on the MCP's own backend.
	Measure bool `json:"measure,omitempty"`

	// IncludeShardRels models a worker backend in MX mode, where shard
	// tables are real local relations. Default false (coordinator view).
	IncludeShardRels bool `json:"include_shard_rels,omitempty"`

	// ActiveBackendsOverride, WarnBytes, CriticalBytes: same semantics as
	// the B1 tool.
	ActiveBackendsOverride int   `json:"active_backends_override,omitempty"`
	WarnBytes              int64 `json:"warn_bytes,omitempty"`
	CriticalBytes          int64 `json:"critical_bytes,omitempty"`
}

// PgCacheFootprintOutput is the tool response.
type PgCacheFootprintOutput struct {
	Estimate         memory.PgCacheEstimate        `json:"estimate"`
	Measured         *memory.BackendCtxMeasurement `json:"measured,omitempty"`
	ActiveBackends   int                           `json:"active_backends"`
	MaxConnections   int                           `json:"max_connections"`
	ProjectedCluster ProjectedClusterFootprint     `json:"projected_cluster"`
	Alarms           []diagnostics.Alarm           `json:"alarms"`
	Notes            []string                      `json:"notes,omitempty"`
}

// PgCacheFootprintTool implements the tool.
func PgCacheFootprintTool(ctx context.Context, deps Dependencies, in PgCacheFootprintInput) (*mcp.CallToolResult, PgCacheFootprintOutput, error) {
	out := PgCacheFootprintOutput{Alarms: []diagnostics.Alarm{}, Notes: []string{}}
	if in.WarnBytes <= 0 {
		in.WarnBytes = 256 * 1024 * 1024
	}
	if in.CriticalBytes <= 0 {
		in.CriticalBytes = 1024 * 1024 * 1024
	}

	shapes, err := memory.FetchShapes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}
	out.Estimate = memory.EstimatePgCache(shapes, in.IncludeShardRels)

	conn, err := deps.Pool.Acquire(ctx)
	if err == nil {
		defer conn.Release()
		_ = conn.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&out.MaxConnections)
		_ = conn.QueryRow(ctx, `SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend'`).Scan(&out.ActiveBackends)

		if in.Measure {
			// touching metadata also populates relcache/catcache for the
			// dist-table logical rels, giving a more realistic measurement.
			_, _ = memory.TouchAllDistributedMetadata(ctx, conn.Conn())
			meas, perr := memory.Probe(ctx, conn.Conn(), []string{"CacheMemoryContext"})
			if perr != nil {
				out.Notes = append(out.Notes, fmt.Sprintf("probe failed: %v", perr))
			}
			for i := range meas {
				if meas[i].Scope == "CacheMemoryContext" {
					m := meas[i]
					out.Measured = &m
				}
			}
		}
	}

	effBackends := in.ActiveBackendsOverride
	if effBackends <= 0 {
		effBackends = out.ActiveBackends
	}
	out.ProjectedCluster = ProjectedClusterFootprint{
		PerBackendBytes:    out.Estimate.Bytes,
		ActiveBackendBytes: out.Estimate.Bytes * int64(effBackends),
		MaxConnectionBytes: out.Estimate.Bytes * int64(out.MaxConnections),
		Note:               "per_backend × {active_backends, max_connections}; PG caches are per-process.",
	}

	// Alarms
	severity := ""
	switch {
	case out.Estimate.Bytes >= in.CriticalBytes:
		severity = string(diagnostics.SeverityCritical)
	case out.Estimate.Bytes >= in.WarnBytes:
		severity = string(diagnostics.SeverityWarning)
	}
	if severity != "" && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "pg_cache.footprint",
			Severity: diagnostics.Severity(severity),
			Source:   "citus_pg_cache_footprint",
			Message:  fmt.Sprintf("PG CacheMemoryContext estimated at %d bytes per backend (%d rels, %d indexes, %d attrs)", out.Estimate.Bytes, out.Estimate.NRelations, out.Estimate.NIndexes, out.Estimate.NAttrs),
			Evidence: map[string]any{
				"per_backend_bytes":   out.Estimate.Bytes,
				"n_relations":         out.Estimate.NRelations,
				"n_indexes":           out.Estimate.NIndexes,
				"max_connections":     out.MaxConnections,
				"projected_max_total": out.ProjectedCluster.MaxConnectionBytes,
				"include_shard_rels":  in.IncludeShardRels,
			},
			FixHint: "Reduce partition count on distributed tables; reduce shard count; reduce index count per table; lower max_connections.",
			DocsURL: "https://www.postgresql.org/docs/current/catalog-pg-statistic.html",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	return nil, out, nil
}
