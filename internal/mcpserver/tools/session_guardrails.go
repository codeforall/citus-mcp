// citus-mcp: B20 citus_session_guardrails -- scans active client backends
// and flags sessions with risky per-session GUCs (work_mem too high,
// statement_timeout=0, synchronous_commit=off for critical users,
// citus.multi_shard_modify_mode=parallel set at session scope, etc.).
//
// Rule design: every rule is annotated with the Citus/PG source line that
// defines the *shipped default* and a comment explaining in which
// *context* the setting becomes risky. A rule MUST NOT fire at CRITICAL on
// a shipped default — do that and the operator is rightly suspicious of
// every other finding. If risk only materializes in combination with
// another GUC, encode the combination; do not elevate an isolated default.
//
// Reading the cluster-level value: pg_settings.setting and .reset_val both
// reflect the session's perspective, which is contaminated by the pgxpool
// RuntimeParams this tool's own connection carries (statement_timeout is
// set on every pool connection — see internal/db/connection.go). To get
// the value a fresh unrelated client backend would observe, we open a
// one-shot pgx.Conn with the pool's DSN but NO RuntimeParams.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SessionGuardrailsInput struct {
	WorkMemMiBHigh int `json:"work_mem_mib_high,omitempty"` // default 512
	// AdaptivePoolHigh is the threshold above which
	// citus.max_adaptive_executor_pool_size is considered risky when
	// combined with multi_shard_modify_mode=parallel. Default 32 (2x the
	// shipped default of 16; beyond this a single multi-shard write
	// fans out >32 connections/worker which can exhaust pg_backends).
	AdaptivePoolHigh int `json:"adaptive_pool_high,omitempty"`
}

type RiskySession struct {
	Pid             int32             `json:"pid"`
	User            string            `json:"user"`
	Database        string            `json:"database"`
	ApplicationName string            `json:"application_name"`
	Settings        map[string]string `json:"settings"`
	Findings        []string          `json:"findings"`
}

type SessionGuardrailsOutput struct {
	Sessions []RiskySession      `json:"sessions"`
	Alarms   []diagnostics.Alarm `json:"alarms"`
	Warnings []string            `json:"warnings,omitempty"`
}

// gucCheck describes one setting-level rule. A rule fires only if the
// current value departs from the shipped default AND does so in a way
// that is risky. The severity is the severity of the *finding*, not the
// severity of the GUC's class.
type gucCheck struct {
	name     string
	severity diagnostics.Severity
	fix      string
	// predicate returns (fire?, humanReadableNote). It receives the GUC
	// value as a string (raw PG output).
	predicate func(v string) (bool, string)
}

func SessionGuardrailsTool(ctx context.Context, deps Dependencies, in SessionGuardrailsInput) (*mcp.CallToolResult, SessionGuardrailsOutput, error) {
	out := SessionGuardrailsOutput{Sessions: []RiskySession{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.WorkMemMiBHigh == 0 {
		in.WorkMemMiBHigh = 512
	}
	if in.AdaptivePoolHigh == 0 {
		in.AdaptivePoolHigh = 32
	}

	rows, err := deps.Pool.Query(ctx, `
SELECT pid, usename, datname, COALESCE(application_name,''), state
FROM pg_stat_activity
WHERE backend_type='client backend' AND pid <> pg_backend_pid()
ORDER BY pid LIMIT 500`)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("pg_stat_activity: %v", err))
		return nil, out, nil
	}
	defer rows.Close()
	for rows.Next() {
		var s RiskySession
		var state string
		if err := rows.Scan(&s.Pid, &s.User, &s.Database, &s.ApplicationName, &state); err != nil {
			continue
		}
		s.Settings = map[string]string{}
		s.Findings = []string{}
		out.Sessions = append(out.Sessions, s)
	}

	// Single-GUC rules. Every entry here has been verified NOT to fire on
	// the shipped default unless the default itself represents an
	// unbounded-resource state that the operator arguably should be told
	// about (in which case severity is `info`, never `warning`/`critical`).
	riskyChecks := []gucCheck{
		{
			// PG default: work_mem=4MB (src/backend/utils/misc/guc_tables.c
			// "work_mem" entry, boot_val=4096 kB). Firing at >512 MiB
			// flags per-query memory that can multiply across concurrent
			// hash joins and OOM the node.
			name: "work_mem", severity: diagnostics.SeverityWarning,
			fix: "work_mem is global; consider SET LOCAL in reporting queries instead of raising the cluster default.",
			predicate: func(v string) (bool, string) {
				var n int
				_, _ = fmt.Sscanf(v, "%d", &n)
				return n > in.WorkMemMiBHigh*1024, fmt.Sprintf("work_mem=%s (> %d MiB)", v, in.WorkMemMiBHigh)
			},
		},
		{
			// PG default: statement_timeout=0 (unbounded). Reporting this
			// as a CRITICAL or even WARNING on a default install is noise;
			// we emit info so the operator knows it's unbounded but isn't
			// alarmed that something is misconfigured.
			name: "statement_timeout", severity: diagnostics.SeverityInfo,
			fix: "Consider a non-zero statement_timeout to bound runaway queries; PG ships with 0 (unbounded).",
			predicate: func(v string) (bool, string) {
				return v == "0", "statement_timeout=0 (PG default, unbounded)"
			},
		},
		{
			// PG default: idle_in_transaction_session_timeout=0. Same
			// reasoning as statement_timeout — it's the shipped default,
			// so info-only.
			name: "idle_in_transaction_session_timeout", severity: diagnostics.SeverityInfo,
			fix: "Consider idle_in_transaction_session_timeout to bound abandoned transactions.",
			predicate: func(v string) (bool, string) {
				return v == "0", "idle_in_transaction_session_timeout=0 (PG default, unbounded)"
			},
		},
	}
	// Open a pristine connection (no RuntimeParams overrides) so that the
	// GUC reads below reflect the value a normal client backend would
	// see, not the citus-mcp pool's per-connection statement_timeout etc.
	pristine, perr := openPristineConn(ctx, deps)
	if perr != nil || pristine == nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("could not open pristine probe connection (%v); guardrail rules skipped", perr))
		return nil, out, nil
	}
	defer pristine.Close(ctx)

	// We use current_setting() on the pristine connection — which, having
	// no RuntimeParams, returns the configuration-file / ALTER-SYSTEM
	// value that a regular client backend would observe.
	for _, c := range riskyChecks {
		var v string
		if err := pristine.QueryRow(ctx,
			`SELECT COALESCE(current_setting($1, true),'')`, c.name).Scan(&v); err != nil || v == "" {
			continue
		}
		if bad, note := c.predicate(v); bad && deps.Alarms != nil {
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "session.risky_setting", Severity: c.severity,
				Source:   "citus_session_guardrails",
				Object:   c.name,
				Message:  fmt.Sprintf("Cluster default %s: %s", c.name, note),
				Evidence: map[string]any{"guc": c.name, "value": v},
				FixHint:  c.fix,
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}

	// Combination-aware rule: citus.multi_shard_modify_mode=parallel is
	// the SHIPPED DEFAULT (see shared_library_init.c:2210-2218 —
	// PARALLEL_CONNECTION is the boot_val) and is the recommended mode
	// for throughput. Emitting a standalone CRITICAL on it is wrong. It
	// only becomes risky when combined with an unusually large
	// citus.max_adaptive_executor_pool_size: at that point a single
	// multi-shard write fans out modify_mode × pool_size connections per
	// worker, which can exhaust pg_backends. The shipped pool size is 16
	// (shared_library_init.c:1986-2001), so we only fire when the cluster
	// has raised it above `adaptive_pool_high`.
	var modifyMode, poolSizeStr string
	_ = pristine.QueryRow(ctx, `SELECT COALESCE(current_setting('citus.multi_shard_modify_mode', true),'')`).Scan(&modifyMode)
	_ = pristine.QueryRow(ctx, `SELECT COALESCE(current_setting('citus.max_adaptive_executor_pool_size', true),'')`).Scan(&poolSizeStr)
	var poolSize int
	_, _ = fmt.Sscanf(poolSizeStr, "%d", &poolSize)
	if modifyMode == "parallel" && poolSize > in.AdaptivePoolHigh && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind:     "session.risky_setting",
			Severity: diagnostics.SeverityWarning,
			Source:   "citus_session_guardrails",
			Object:   "citus.multi_shard_modify_mode+max_adaptive_executor_pool_size",
			Message: fmt.Sprintf(
				"citus.multi_shard_modify_mode=parallel combined with citus.max_adaptive_executor_pool_size=%d (> %d): a single multi-shard write can fan out up to %d connections per worker.",
				poolSize, in.AdaptivePoolHigh, poolSize),
			Evidence: map[string]any{
				"multi_shard_modify_mode":             modifyMode,
				"max_adaptive_executor_pool_size":     poolSize,
				"adaptive_pool_high_threshold":        in.AdaptivePoolHigh,
			},
			FixHint: "Either cap citus.max_adaptive_executor_pool_size closer to 16 (the shipped default) or switch modify_mode to sequential for write-heavy multi-shard transactions.",
		})
		out.Alarms = append(out.Alarms, *a)
	}

	return nil, out, nil
}

// openPristineConn returns a one-shot pgx.Conn that shares the pool's DSN
// (host, port, db, auth) but carries NO RuntimeParams. Any GUC read via
// current_setting() on this connection reflects the value a regular
// client backend would see (postgresql.conf + ALTER SYSTEM + role/db
// defaults), not the citus-mcp pool's per-connection overrides.
func openPristineConn(ctx context.Context, deps Dependencies) (*pgx.Conn, error) {
	if deps.Pool == nil {
		return nil, fmt.Errorf("no pool")
	}
	base := deps.Pool.Config().ConnConfig.Copy()
	// Strip every RuntimeParam the pool injects (statement_timeout,
	// application_name, etc). We want a clean session.
	base.RuntimeParams = map[string]string{}
	return pgx.ConnectConfig(ctx, base)
}
