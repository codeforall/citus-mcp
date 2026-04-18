// citus-mcp: unit tests for 2PC recovery inspector helpers.

package tools

import "testing"

func TestParseCitusGID(t *testing.T) {
	cases := []struct {
		name        string
		gid         string
		wantOK      bool
		wantGroup   int32
		wantPID     int32
		wantTxnNum  uint64
		wantConnNum uint32
	}{
		{
			name:        "coord-issued 4 parts",
			gid:         "citus_0_12345_678_2",
			wantOK:      true,
			wantGroup:   0,
			wantPID:     12345,
			wantTxnNum:  678,
			wantConnNum: 2,
		},
		{
			name:        "worker-issued 3 parts (no conn)",
			gid:         "citus_3_4567_89",
			wantOK:      true,
			wantGroup:   3,
			wantPID:     4567,
			wantTxnNum:  89,
			wantConnNum: 0,
		},
		{
			name:   "missing prefix",
			gid:    "my_custom_gid_123",
			wantOK: false,
		},
		{
			name:   "no underscores after prefix",
			gid:    "citus_",
			wantOK: false,
		},
		{
			name:   "non-numeric group",
			gid:    "citus_abc_1_2",
			wantOK: false,
		},
		{
			name:   "empty string",
			gid:    "",
			wantOK: false,
		},
		{
			name:        "large txn number",
			gid:         "citus_1_100_18446744073709551000",
			wantOK:      true,
			wantGroup:   1,
			wantPID:     100,
			wantTxnNum:  18446744073709551000,
			wantConnNum: 0,
		},
		{
			name:   "too many parts (5)",
			gid:    "citus_1_2_3_4_5",
			wantOK: false,
		},
		{
			name:   "four parts with unparseable conn",
			gid:    "citus_1_2_3_bad",
			wantOK: false,
		},
		{
			name:   "negative group",
			gid:    "citus_-1_100_200_3",
			wantOK: false,
		},
		{
			name:   "negative pid",
			gid:    "citus_0_-100_200_3",
			wantOK: false,
		},
		{
			name:   "two parts (no txn)",
			gid:    "citus_0_100",
			wantOK: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, p := parseCitusGID(c.gid)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !ok {
				return
			}
			if p.groupID != c.wantGroup {
				t.Errorf("groupID = %d, want %d", p.groupID, c.wantGroup)
			}
			if p.pid != c.wantPID {
				t.Errorf("pid = %d, want %d", p.pid, c.wantPID)
			}
			if p.transactionNumber != c.wantTxnNum {
				t.Errorf("txnNumber = %d, want %d", p.transactionNumber, c.wantTxnNum)
			}
			if p.connectionNumber != c.wantConnNum {
				t.Errorf("connNumber = %d, want %d", p.connectionNumber, c.wantConnNum)
			}
		})
	}
}

func TestSqlQuote(t *testing.T) {
	cases := map[string]string{
		"citus_0_1_2":   `'citus_0_1_2'`,
		"with'quote":    `'with''quote'`,
		"":              `''`,
		"''nested''":    `'''''nested'''''`,
	}
	for in, want := range cases {
		if got := sqlQuote(in); got != want {
			t.Errorf("sqlQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildRecoveryScriptEmpty(t *testing.T) {
	if s := buildRecoveryScript(nil); s == "" {
		// An empty non-classified list should produce a header-only script,
		// not a hard error.
		t.Logf("empty script -> %q", s)
	}
}

func TestBuildRecoveryScriptGrouping(t *testing.T) {
	rows := []PreparedXactRow{
		{Node: "worker1:5433", Classification: "commit_needed", RecommendedSQL: "COMMIT PREPARED 'g1';", AgeSeconds: 30, Notes: "n1"},
		{Node: "coordinator", Classification: "rollback_needed", RecommendedSQL: "ROLLBACK PREPARED 'g2';", AgeSeconds: 90, Notes: "n2"},
		{Node: "worker1:5433", Classification: "in_flight"}, // excluded
		{Node: "worker2:5434", Classification: "commit_needed", RecommendedSQL: "COMMIT PREPARED 'g3';", AgeSeconds: 15, Notes: "n3"},
	}
	script := buildRecoveryScript(rows)
	for _, want := range []string{
		"coordinator", "worker1:5433", "worker2:5434",
		"COMMIT PREPARED 'g1';", "ROLLBACK PREPARED 'g2';", "COMMIT PREPARED 'g3';",
	} {
		if !contains(script, want) {
			t.Errorf("script missing %q:\n%s", want, script)
		}
	}
	if contains(script, "in_flight") {
		t.Errorf("script should not include in_flight entries")
	}
}

func TestBuildRecoveryScriptNoTransactionBlock(t *testing.T) {
	// COMMIT PREPARED / ROLLBACK PREPARED cannot run inside BEGIN/COMMIT.
	// Regression: the script must be a sequence of standalone top-level
	// statements, never wrapped in a transaction block.
	rows := []PreparedXactRow{
		{Node: "worker1:5433", Classification: "commit_needed", RecommendedSQL: "COMMIT PREPARED 'g1';", AgeSeconds: 30},
		{Node: "worker1:5433", Classification: "rollback_needed", RecommendedSQL: "ROLLBACK PREPARED 'g2';", AgeSeconds: 90},
	}
	script := buildRecoveryScript(rows)
	for _, forbidden := range []string{"BEGIN;", "BEGIN\n", "COMMIT;", "COMMIT\n"} {
		if contains(script, forbidden) {
			t.Errorf("script must not contain %q (PostgreSQL rejects COMMIT/ROLLBACK PREPARED inside a tx block)\nscript:\n%s", forbidden, script)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	// tiny helper to avoid pulling strings in just for tests
	n, m := len(s), len(sub)
	for i := 0; i+m <= n; i++ {
		if s[i:i+m] == sub {
			return i
		}
	}
	return -1
}
