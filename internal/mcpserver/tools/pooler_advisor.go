// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// B2c: citus_pooler_advisor --
// PgBouncer-aware guidance. Does THREE things:
//   1. Detects pooler presence from pg_stat_activity (application_name).
//   2. If pgbouncer_admin_dsn is supplied, connects to the pgbouncer admin
//      console and reads SHOW CONFIG / SHOW POOLS / SHOW STATS; flags
//      deviations from best practice for Citus.
//   3. Emits a recommended pgbouncer.ini config stanza grounded in real
//      numbers from the connected Citus cluster (max_connections,
//      citus.max_cached_conns_per_worker, B2's coordinator cap).
//
// Key rules we enforce, each grounded in Citus source/behaviour:
//   * pool_mode=transaction breaks Citus cached worker connections
//     (max_cached_conns_per_worker), session GUCs, and 2PC affinity.
//     Prefer session mode for Citus unless application is stateless per
//     transaction.
//   * default_pool_size MUST be <= coordinator's effective cap from B2.
//   * server_lifetime too short (<5 min) kills cached worker conns churn.
//   * reserve_pool_size should be small (<= 10% of default_pool_size) for
//     distributed queries to avoid storms.

package tools

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// PoolerAdvisorInput controls the analysis.
type PoolerAdvisorInput struct {
	// PgBouncerAdminDsn, optional. When set, we connect and read the admin
	// console (SHOW CONFIG, SHOW POOLS, SHOW DATABASES).
	// Typical: "postgres://pgbouncer@host:6432/pgbouncer?sslmode=disable"
	PgBouncerAdminDsn string `json:"pgbouncer_admin_dsn,omitempty"`
	// TargetClientMax is the client-concurrency target you want pgbouncer
	// to handle. If 0 we use B2-style default (10x coord cap).
	TargetClientMax int `json:"target_client_max,omitempty"`
}

// PoolerAdvisorOutput is the tool response.
type PoolerAdvisorOutput struct {
	Detection           PoolerDetection       `json:"detection"`
	AdminSnapshot       *PgBouncerSnapshot    `json:"admin_snapshot,omitempty"`
	Findings            []PoolerFinding       `json:"findings"`
	RecommendedConfig   string                `json:"recommended_config_ini"`
	ClusterNumbersUsed  map[string]any        `json:"cluster_numbers_used"`
	Alarms              []diagnostics.Alarm   `json:"alarms"`
	Warnings            []string              `json:"warnings,omitempty"`
}

// PoolerDetection summarizes what we can tell about the in-path pooler
// without an admin DSN.
type PoolerDetection struct {
	LikelyPgBouncerSessionsSeen int      `json:"likely_pgbouncer_sessions_seen"`
	SampleApplicationNames      []string `json:"sample_application_names,omitempty"`
	Conclusion                  string   `json:"conclusion"`
}

// PgBouncerSnapshot is populated only when admin DSN is reachable.
type PgBouncerSnapshot struct {
	Version    string                  `json:"version,omitempty"`
	Config     map[string]string       `json:"config,omitempty"`
	Pools      []map[string]any        `json:"pools,omitempty"`
	Databases  []map[string]any        `json:"databases,omitempty"`
	Reachable  bool                    `json:"reachable"`
	Error      string                  `json:"error,omitempty"`
}

// PoolerFinding captures one rule evaluation.
type PoolerFinding struct {
	Rule     string `json:"rule"`
	Severity string `json:"severity"` // "info" | "warning" | "critical"
	Message  string `json:"message"`
	FixHint  string `json:"fix_hint,omitempty"`
}

// PoolerAdvisorTool implements B2c.
func PoolerAdvisorTool(ctx context.Context, deps Dependencies, in PoolerAdvisorInput) (*mcp.CallToolResult, PoolerAdvisorOutput, error) {
	out := PoolerAdvisorOutput{
		Findings: []PoolerFinding{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{},
		ClusterNumbersUsed: map[string]any{},
	}

	conn, err := deps.Pool.Acquire(ctx)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), out, nil
	}
	defer conn.Release()

	maxConns := int(readGucInt(ctx, conn.Conn(), "max_connections"))
	maxClient := int(readGucInt(ctx, conn.Conn(), "citus.max_client_connections"))
	if maxClient <= 0 {
		maxClient = maxConns
	}
	maxCachedPerWorker := int(readGucInt(ctx, conn.Conn(), "citus.max_cached_conns_per_worker"))
	maxAdaptive := int(readGucInt(ctx, conn.Conn(), "citus.max_adaptive_executor_pool_size"))
	out.ClusterNumbersUsed["max_connections"] = maxConns
	out.ClusterNumbersUsed["citus.max_client_connections"] = maxClient
	out.ClusterNumbersUsed["citus.max_cached_conns_per_worker"] = maxCachedPerWorker
	out.ClusterNumbersUsed["citus.max_adaptive_executor_pool_size"] = maxAdaptive

	// 1) Detection: look at pg_stat_activity for pgbouncer-like sessions.
	out.Detection = detectPgBouncer(ctx, conn.Conn())

	// 2) If admin DSN given, try to connect and pull SHOW CONFIG / POOLS.
	if in.PgBouncerAdminDsn != "" {
		snap := pullPgBouncerSnapshot(ctx, in.PgBouncerAdminDsn)
		out.AdminSnapshot = &snap
		if snap.Reachable {
			out.Findings = append(out.Findings, evaluatePgBouncerConfig(snap, maxClient, maxCachedPerWorker)...)
		}
	}

	// 3) Recommended config. Derive numbers.
	coordCap := maxClient
	if coordCap <= 0 {
		coordCap = 100
	}
	target := in.TargetClientMax
	if target <= 0 {
		target = coordCap * 10
	}
	// default_pool_size floor: 10 (pgbouncer default). Cap at coord capacity.
	defaultPoolSize := coordCap
	if defaultPoolSize > maxConns-5 { // leave headroom for superuser + admin
		defaultPoolSize = maxConns - 5
	}
	reservePool := defaultPoolSize / 10
	if reservePool < 2 {
		reservePool = 2
	}
	out.ClusterNumbersUsed["derived_default_pool_size"] = defaultPoolSize
	out.ClusterNumbersUsed["derived_reserve_pool_size"] = reservePool
	out.ClusterNumbersUsed["derived_max_client_conn"] = target

	out.RecommendedConfig = fmt.Sprintf(`; citus-mcp recommended pgbouncer.ini (session mode)
; Derived from: max_connections=%d, citus.max_client_connections=%d,
; citus.max_cached_conns_per_worker=%d, target_client_max=%d.
; Session mode preserves Citus cached worker connections, session GUCs,
; and 2PC affinity. Use transaction mode ONLY for stateless app tiers.

[databases]
; Example for one database; add one per application DB.
citus = host=<coordinator> port=5432 dbname=citus pool_size=%d

[pgbouncer]
listen_port         = 6432
listen_addr         = *
auth_type           = scram-sha-256
auth_file           = /etc/pgbouncer/userlist.txt
admin_users         = pgbouncer

pool_mode           = session
max_client_conn     = %d
default_pool_size   = %d
reserve_pool_size   = %d
reserve_pool_timeout = 3

; Keep server connections hot so Citus cached worker conns stay warm.
server_lifetime     = 3600
server_idle_timeout = 600
server_connect_timeout = 5

; Helpful for long-running distributed queries:
query_timeout       = 0
query_wait_timeout  = 120

; If you MUST use transaction mode, also set:
;   pool_mode = transaction
;   max_prepared_statements = 100   ; requires pgbouncer >= 1.21
; and accept: no Citus cached worker conns, no session GUCs,
; no session-level advisory locks / prepared stmts outside a txn.
`, maxConns, maxClient, maxCachedPerWorker, target,
		defaultPoolSize, target, defaultPoolSize, reservePool)

	// Emit an info alarm when a pooler is detected but admin DSN wasn't
	// provided -- user can then pass it to get deeper findings.
	if deps.Alarms != nil && out.Detection.LikelyPgBouncerSessionsSeen > 0 && in.PgBouncerAdminDsn == "" {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "pooler.admin_unused",
			Severity: diagnostics.SeverityInfo,
			Source:   "citus_pooler_advisor",
			Message:  "pgbouncer-like sessions observed but pgbouncer_admin_dsn not supplied; advisor is running config-only",
			Evidence: map[string]any{"pgbouncer_sessions_seen": out.Detection.LikelyPgBouncerSessionsSeen, "samples": out.Detection.SampleApplicationNames},
			FixHint:  "Re-run with pgbouncer_admin_dsn='postgres://pgbouncer@host:6432/pgbouncer' for deeper findings.",
		})
		out.Alarms = append(out.Alarms, *a)
	}
	return nil, out, nil
}

func detectPgBouncer(ctx context.Context, conn *pgx.Conn) PoolerDetection {
	d := PoolerDetection{SampleApplicationNames: []string{}}
	rows, err := conn.Query(ctx, `
SELECT DISTINCT application_name
FROM pg_stat_activity
WHERE application_name ILIKE '%pgbouncer%' OR application_name ILIKE '%pgcat%' OR application_name ILIKE '%odyssey%'
LIMIT 20`)
	if err != nil {
		d.Conclusion = "unable to inspect pg_stat_activity: " + err.Error()
		return d
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err == nil && s != "" {
			d.SampleApplicationNames = append(d.SampleApplicationNames, s)
		}
	}
	// Count sessions matching
	var c int
	_ = conn.QueryRow(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE application_name ILIKE '%pgbouncer%' OR application_name ILIKE '%pgcat%' OR application_name ILIKE '%odyssey%'`).Scan(&c)
	d.LikelyPgBouncerSessionsSeen = c
	switch {
	case c == 0:
		d.Conclusion = "no pooler detected via application_name; cluster may be reached directly."
	case c > 0:
		d.Conclusion = fmt.Sprintf("%d session(s) with pooler-like application_name; a connection pooler is likely in front of this node.", c)
	}
	return d
}

// pullPgBouncerSnapshot connects to the pgbouncer admin console using a
// dedicated single-connection pool (pgbouncer admin is NOT real PG -- some
// pgx features like prepared statements may misbehave; we keep it strict).
func pullPgBouncerSnapshot(ctx context.Context, dsn string) PgBouncerSnapshot {
	snap := PgBouncerSnapshot{Reachable: false, Config: map[string]string{}}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		snap.Error = "parse dsn: " + err.Error()
		return snap
	}
	// pgbouncer admin console speaks a tiny SQL subset; disable prepared stmts
	// and simple protocol to stay on the safe path.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		snap.Error = "connect: " + err.Error()
		return snap
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		snap.Error = "acquire: " + err.Error()
		return snap
	}
	defer conn.Release()

	// SHOW VERSION;
	if rows, err := conn.Conn().Query(ctx, "SHOW VERSION"); err == nil {
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err == nil {
				snap.Version = v
			}
		}
		rows.Close()
	}
	// SHOW CONFIG;
	if rows, err := conn.Conn().Query(ctx, "SHOW CONFIG"); err == nil {
		for rows.Next() {
			// pgbouncer returns (key, value, default, changeable) typically;
			// be defensive and read first two cols as strings.
			vals, err := rows.Values()
			if err != nil || len(vals) < 2 {
				continue
			}
			k, _ := vals[0].(string)
			v, _ := vals[1].(string)
			if k != "" {
				snap.Config[k] = v
			}
		}
		rows.Close()
		snap.Reachable = true
	} else {
		snap.Error = "SHOW CONFIG: " + err.Error()
		return snap
	}
	// SHOW POOLS;
	if rows, err := conn.Conn().Query(ctx, "SHOW POOLS"); err == nil {
		cols := rows.FieldDescriptions()
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			m := map[string]any{}
			for i, c := range cols {
				if i < len(vals) {
					m[string(c.Name)] = vals[i]
				}
			}
			snap.Pools = append(snap.Pools, m)
		}
		rows.Close()
	}
	// SHOW DATABASES;
	if rows, err := conn.Conn().Query(ctx, "SHOW DATABASES"); err == nil {
		cols := rows.FieldDescriptions()
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			m := map[string]any{}
			for i, c := range cols {
				if i < len(vals) {
					m[string(c.Name)] = vals[i]
				}
			}
			snap.Databases = append(snap.Databases, m)
		}
		rows.Close()
	}
	return snap
}

// evaluatePgBouncerConfig applies Citus-specific rules to a live pgbouncer
// config snapshot.
func evaluatePgBouncerConfig(s PgBouncerSnapshot, maxClient, maxCachedPerWorker int) []PoolerFinding {
	f := []PoolerFinding{}
	poolMode := strings.ToLower(s.Config["pool_mode"])
	if poolMode == "transaction" {
		f = append(f, PoolerFinding{
			Rule: "pool_mode.transaction_vs_citus", Severity: "warning",
			Message: "pool_mode=transaction BREAKS Citus cached worker connections (max_cached_conns_per_worker=" +
				fmt.Sprint(maxCachedPerWorker) + "), session GUCs, 2PC, and session-level advisory locks.",
			FixHint: "Switch to pool_mode=session if the app relies on any session state; otherwise accept the trade-off consciously.",
		})
	} else if poolMode == "statement" {
		f = append(f, PoolerFinding{
			Rule: "pool_mode.statement_incompatible", Severity: "critical",
			Message: "pool_mode=statement is NOT compatible with Citus multi-statement transactions or distributed queries.",
			FixHint: "Use pool_mode=session or pool_mode=transaction.",
		})
	}
	if v, ok := s.Config["default_pool_size"]; ok {
		if n, err := parseIntStr(v); err == nil && n > maxClient {
			f = append(f, PoolerFinding{
				Rule: "default_pool_size.exceeds_server_cap", Severity: "critical",
				Message: fmt.Sprintf("default_pool_size=%d exceeds citus.max_client_connections=%d; pool will be cut off.", n, maxClient),
				FixHint: fmt.Sprintf("Set default_pool_size <= %d.", maxClient),
			})
		}
	}
	if v, ok := s.Config["server_lifetime"]; ok {
		if n, err := parseIntStr(v); err == nil && n < 300 && n != 0 {
			f = append(f, PoolerFinding{
				Rule: "server_lifetime.too_short", Severity: "warning",
				Message: fmt.Sprintf("server_lifetime=%ds is aggressive; Citus cached worker connections churn on every reconnect.", n),
				FixHint: "Use server_lifetime=3600 (1h) or higher for Citus; 0 disables.",
			})
		}
	}
	if v, ok := s.Config["max_prepared_statements"]; ok && poolMode == "transaction" {
		if n, err := parseIntStr(v); err == nil && n == 0 {
			f = append(f, PoolerFinding{
				Rule: "prepared_statements.incompatible_with_txn", Severity: "warning",
				Message: "pool_mode=transaction with max_prepared_statements=0 breaks app-level prepared statement reuse.",
				FixHint: "Upgrade pgbouncer to >=1.21 and set max_prepared_statements>=100, or move to pool_mode=session.",
			})
		}
	}
	return f
}

func parseIntStr(s string) (int, error) {
	// pgbouncer sometimes reports values with trailing unit / whitespace.
	s = strings.TrimSpace(s)
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
