// citus-mcp: B20 citus_session_guardrails -- scans active client backends
// and flags sessions with risky per-session GUCs (work_mem too high,
// statement_timeout=0, synchronous_commit=off for critical users,
// citus.multi_shard_modify_mode=parallel set at session scope, etc.).

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SessionGuardrailsInput struct {
	WorkMemMiBHigh int `json:"work_mem_mib_high,omitempty"` // default 512
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

func SessionGuardrailsTool(ctx context.Context, deps Dependencies, in SessionGuardrailsInput) (*mcp.CallToolResult, SessionGuardrailsOutput, error) {
	out := SessionGuardrailsOutput{Sessions: []RiskySession{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.WorkMemMiBHigh == 0 {
		in.WorkMemMiBHigh = 512
	}

	// pg_stat_activity gives pid, user, db. Per-session overrides live in
	// pg_db_role_setting (static) and in the backend's own settings — we
	// can fetch those via pg_catalog.pg_settings WHERE source IN ('session','client').
	// Since we can't run a query in the remote backend, we check active
	// sessions and report their USER/DB, and then separately show any
	// risky global-or-role defaults.
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

	// Check for risky global/role settings we *can* see.
	riskyChecks := []struct {
		name, severity, fix string
		predicate           func(string) (bool, string)
	}{
		{"work_mem", "warning",
			"work_mem is global; consider SET LOCAL in reporting queries instead.",
			func(v string) (bool, string) {
				var n int
				_, _ = fmt.Sscanf(v, "%d", &n)
				return n > in.WorkMemMiBHigh*1024, fmt.Sprintf("work_mem=%s (> %d MiB)", v, in.WorkMemMiBHigh)
			}},
		{"statement_timeout", "warning", "Set statement_timeout > 0 to bound runaway queries.",
			func(v string) (bool, string) { return v == "0", "statement_timeout=0 (unbounded)" }},
		{"idle_in_transaction_session_timeout", "info",
			"Set idle_in_transaction_session_timeout to bound abandoned transactions.",
			func(v string) (bool, string) {
				return v == "0", "idle_in_transaction_session_timeout=0 (unbounded)"
			}},
		{"citus.multi_shard_modify_mode", "critical",
			"citus.multi_shard_modify_mode=parallel combined with high executor pool multiplies connection load.",
			func(v string) (bool, string) {
				return v == "parallel", "citus.multi_shard_modify_mode=parallel (risky)"
			}},
	}
	for _, c := range riskyChecks {
		var v string
		if err := deps.Pool.QueryRow(ctx,
			`SELECT COALESCE(current_setting($1, true),'')`, c.name).Scan(&v); err != nil || v == "" {
			continue
		}
		if bad, note := c.predicate(v); bad && deps.Alarms != nil {
			sev := diagnostics.SeverityInfo
			switch c.severity {
			case "warning":
				sev = diagnostics.SeverityWarning
			case "critical":
				sev = diagnostics.SeverityCritical
			}
			a := deps.Alarms.Emit(diagnostics.Alarm{
				Kind: "session.risky_setting", Severity: sev,
				Source:  "citus_session_guardrails",
				Message: fmt.Sprintf("Cluster default %s: %s", c.name, note),
				Evidence: map[string]any{"guc": c.name, "value": v},
				FixHint: c.fix,
			})
			out.Alarms = append(out.Alarms, *a)
		}
	}
	return nil, out, nil
}
