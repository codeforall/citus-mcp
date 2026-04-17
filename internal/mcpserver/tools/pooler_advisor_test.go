// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT

package tools

import "testing"

func TestEvaluatePgBouncerConfig(t *testing.T) {
	mk := func(cfg map[string]string) PgBouncerSnapshot {
		return PgBouncerSnapshot{Reachable: true, Config: cfg}
	}

	tests := []struct {
		name       string
		cfg        map[string]string
		maxClient  int
		maxCached  int
		wantRules  []string
	}{
		{
			name:      "transaction_mode_warns",
			cfg:       map[string]string{"pool_mode": "transaction", "default_pool_size": "10", "max_prepared_statements": "100"},
			maxClient: 100, maxCached: 1,
			wantRules: []string{"pool_mode.transaction_vs_citus"},
		},
		{
			name:      "statement_mode_critical",
			cfg:       map[string]string{"pool_mode": "statement"},
			maxClient: 100, maxCached: 1,
			wantRules: []string{"pool_mode.statement_incompatible"},
		},
		{
			name:      "default_pool_too_big",
			cfg:       map[string]string{"pool_mode": "session", "default_pool_size": "500"},
			maxClient: 100, maxCached: 1,
			wantRules: []string{"default_pool_size.exceeds_server_cap"},
		},
		{
			name:      "server_lifetime_too_short",
			cfg:       map[string]string{"pool_mode": "session", "server_lifetime": "60"},
			maxClient: 100, maxCached: 1,
			wantRules: []string{"server_lifetime.too_short"},
		},
		{
			name:      "txn_mode_and_no_prepared",
			cfg:       map[string]string{"pool_mode": "transaction", "max_prepared_statements": "0"},
			maxClient: 100, maxCached: 1,
			wantRules: []string{"pool_mode.transaction_vs_citus", "prepared_statements.incompatible_with_txn"},
		},
		{
			name:      "clean_session_mode",
			cfg:       map[string]string{"pool_mode": "session", "default_pool_size": "50", "server_lifetime": "3600"},
			maxClient: 100, maxCached: 1,
			wantRules: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := evaluatePgBouncerConfig(mk(tc.cfg), tc.maxClient, tc.maxCached)
			got := make([]string, 0, len(findings))
			for _, f := range findings {
				got = append(got, f.Rule)
			}
			if len(got) != len(tc.wantRules) {
				t.Fatalf("want rules %v got %v", tc.wantRules, got)
			}
			for i, r := range tc.wantRules {
				if got[i] != r {
					t.Errorf("rule[%d]: want %s got %s", i, r, got[i])
				}
			}
		})
	}
}

func TestParseIntStr(t *testing.T) {
	cases := map[string]int{"100": 100, "  42 ": 42, "0": 0}
	for in, want := range cases {
		got, err := parseIntStr(in)
		if err != nil {
			t.Errorf("parseIntStr(%q) err=%v", in, err)
		}
		if got != want {
			t.Errorf("parseIntStr(%q)=%d want %d", in, got, want)
		}
	}
}
