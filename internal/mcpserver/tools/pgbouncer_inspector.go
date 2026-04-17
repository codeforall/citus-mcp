// citus-mcp: B12 citus_pgbouncer_inspector -- parse pgbouncer admin
// console output (SHOW POOLS/STATS/CLIENTS/SERVERS/DATABASES) and
// correlate with citus_remote_connection_stats on the coordinator.
// Requires admin DSN. Read-only.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type PgBouncerInspectorInput struct {
	AdminDSN            string `json:"admin_dsn"` // required
	HighActivePct       int    `json:"high_active_pct,omitempty"` // default 80
	LongWaitMs          int    `json:"long_wait_ms,omitempty"`     // default 1000
}

type BouncerPool struct {
	Database   string `json:"database"`
	User       string `json:"user"`
	ClActive   int    `json:"cl_active"`
	ClWaiting  int    `json:"cl_waiting"`
	SvActive   int    `json:"sv_active"`
	SvIdle     int    `json:"sv_idle"`
	SvUsed     int    `json:"sv_used"`
	MaxWaitMs  int    `json:"maxwait_ms"`
	PoolMode   string `json:"pool_mode"`
}

type BouncerStats struct {
	Database       string  `json:"database"`
	TotalXactCount int64   `json:"total_xact_count"`
	TotalQueryCount int64  `json:"total_query_count"`
	TotalWaitTimeUs int64  `json:"total_wait_time_us"`
	AvgWaitUs      int64   `json:"avg_wait_us"`
}

type PgBouncerInspectorOutput struct {
	Pools    []BouncerPool       `json:"pools"`
	Stats    []BouncerStats      `json:"stats"`
	CitusRemoteConns []map[string]any `json:"citus_remote_connection_stats,omitempty"`
	Alarms   []diagnostics.Alarm `json:"alarms"`
	Warnings []string            `json:"warnings,omitempty"`
}

func PgBouncerInspectorTool(ctx context.Context, deps Dependencies, in PgBouncerInspectorInput) (*mcp.CallToolResult, PgBouncerInspectorOutput, error) {
	out := PgBouncerInspectorOutput{Pools: []BouncerPool{}, Stats: []BouncerStats{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.AdminDSN == "" {
		return nil, out, fmt.Errorf("admin_dsn required")
	}
	if in.HighActivePct == 0 {
		in.HighActivePct = 80
	}
	if in.LongWaitMs == 0 {
		in.LongWaitMs = 1000
	}

	cfg, err := pgx.ParseConfig(in.AdminDSN)
	if err != nil {
		return nil, out, fmt.Errorf("parse admin_dsn: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, out, fmt.Errorf("connect pgbouncer admin: %w", err)
	}
	defer conn.Close(ctx)

	// SHOW POOLS
	if rows, qerr := conn.Query(ctx, "SHOW POOLS"); qerr == nil {
		defer rows.Close()
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			m := map[string]any{}
			for i, fd := range rows.FieldDescriptions() {
				m[string(fd.Name)] = vals[i]
			}
			p := BouncerPool{
				Database:  stringOf(m["database"]),
				User:      stringOf(m["user"]),
				ClActive:  intOf(m["cl_active"]),
				ClWaiting: intOf(m["cl_waiting"]),
				SvActive:  intOf(m["sv_active"]),
				SvIdle:    intOf(m["sv_idle"]),
				SvUsed:    intOf(m["sv_used"]),
				MaxWaitMs: intOf(m["maxwait_us"]) / 1000,
				PoolMode:  stringOf(m["pool_mode"]),
			}
			out.Pools = append(out.Pools, p)
			total := p.SvActive + p.SvIdle + p.SvUsed
			if total > 0 && p.SvActive*100/total >= in.HighActivePct && deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "pgbouncer.pool_saturated", Severity: diagnostics.SeverityWarning,
					Source: "citus_pgbouncer_inspector",
					Message: fmt.Sprintf("Pool %s/%s: sv_active=%d of %d (%.0f%%)", p.Database, p.User, p.SvActive, total, float64(p.SvActive*100)/float64(total)),
					Evidence: map[string]any{"database": p.Database, "user": p.User, "sv_active": p.SvActive, "total": total},
					FixHint: "Raise default_pool_size or reduce client concurrency.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
			if p.MaxWaitMs >= in.LongWaitMs && deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "pgbouncer.high_wait", Severity: diagnostics.SeverityWarning,
					Source: "citus_pgbouncer_inspector",
					Message: fmt.Sprintf("Pool %s/%s: maxwait %d ms exceeds threshold", p.Database, p.User, p.MaxWaitMs),
					Evidence: map[string]any{"database": p.Database, "maxwait_ms": p.MaxWaitMs},
					FixHint: "Clients are waiting too long for a backend; raise pool size or lower client_concurrency.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
	}

	// SHOW STATS
	if rows, qerr := conn.Query(ctx, "SHOW STATS"); qerr == nil {
		defer rows.Close()
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			m := map[string]any{}
			for i, fd := range rows.FieldDescriptions() {
				m[string(fd.Name)] = vals[i]
			}
			s := BouncerStats{
				Database:        stringOf(m["database"]),
				TotalXactCount:  int64Of(m["total_xact_count"]),
				TotalQueryCount: int64Of(m["total_query_count"]),
				TotalWaitTimeUs: int64Of(m["total_wait_time"]),
				AvgWaitUs:       int64Of(m["avg_wait_time"]),
			}
			out.Stats = append(out.Stats, s)
		}
	}

	// Correlate with citus_remote_connection_stats if available.
	var ok bool
	if err := deps.Pool.QueryRow(ctx, "SELECT to_regclass('pg_catalog.citus_remote_connection_stats') IS NOT NULL").Scan(&ok); err == nil && ok {
		if rows, qerr := deps.Pool.Query(ctx, "SELECT hostname, port, database_name, connection_count_to_node FROM citus_remote_connection_stats ORDER BY connection_count_to_node DESC"); qerr == nil {
			defer rows.Close()
			for rows.Next() {
				vals, _ := rows.Values()
				m := map[string]any{}
				for i, fd := range rows.FieldDescriptions() {
					m[string(fd.Name)] = vals[i]
				}
				out.CitusRemoteConns = append(out.CitusRemoteConns, m)
			}
		}
	}
	return nil, out, nil
}

// Small coercion helpers for the dynamic admin-console shape.
func stringOf(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
func intOf(v any) int {
	if v == nil {
		return 0
	}
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case string:
		var n int
		_, _ = fmt.Sscanf(x, "%d", &n)
		return n
	}
	return 0
}
func int64Of(v any) int64 { return int64(intOf(v)) }
