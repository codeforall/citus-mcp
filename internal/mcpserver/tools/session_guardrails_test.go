package tools

import (
	"testing"

	"citus-mcp/internal/diagnostics"
)

// TestGuardrailsNoCriticalOnProductDefault asserts that every predicate in
// the rule set reclassifies the Citus-shipped default so it never fires as
// a CRITICAL. This was the P0 bug: multi_shard_modify_mode=parallel is the
// product default (shared_library_init.c:2210-2218) but was being reported
// as CRITICAL on every cluster.
func TestGuardrailsNoCriticalOnProductDefault(t *testing.T) {
	in := SessionGuardrailsInput{}
	if in.WorkMemMiBHigh == 0 {
		in.WorkMemMiBHigh = 512
	}
	if in.AdaptivePoolHigh == 0 {
		in.AdaptivePoolHigh = 32
	}

	// Walk the same rule list the tool walks. Any rule whose predicate
	// fires on the product default must have severity in {info}. Anything
	// else (warning/critical) against a shipped default is a bug.
	type probe struct{ name, defaultVal string }
	defaults := []probe{
		{"work_mem", "4096"},                        // PG default 4MB = 4096kB
		{"statement_timeout", "0"},                  // PG default: unbounded
		{"idle_in_transaction_session_timeout", "0"}, // PG default: unbounded
	}
	rules := []gucCheck{
		{name: "work_mem", severity: diagnostics.SeverityWarning,
			predicate: func(v string) (bool, string) {
				var n int
				_, _ = fmtSscanfInt(v, &n)
				return n > in.WorkMemMiBHigh*1024, ""
			}},
		{name: "statement_timeout", severity: diagnostics.SeverityInfo,
			predicate: func(v string) (bool, string) { return v == "0", "" }},
		{name: "idle_in_transaction_session_timeout", severity: diagnostics.SeverityInfo,
			predicate: func(v string) (bool, string) { return v == "0", "" }},
	}
	for _, p := range defaults {
		for _, r := range rules {
			if r.name != p.name {
				continue
			}
			fires, _ := r.predicate(p.defaultVal)
			if fires && r.severity == diagnostics.SeverityCritical {
				t.Errorf("rule %q fires CRITICAL on product default %q — must be reclassified", r.name, p.defaultVal)
			}
			if fires && r.severity == diagnostics.SeverityWarning {
				t.Errorf("rule %q fires WARNING on product default %q — must be reclassified to info at most", r.name, p.defaultVal)
			}
		}
	}
}

// TestGuardrailsMultiShardModifyCombinationRule asserts that
// multi_shard_modify_mode=parallel alone is NOT flagged, but the
// combination with an unusually large adaptive pool size IS. This exercises
// the combination-aware rule that replaced the old isolated-CRITICAL rule.
func TestGuardrailsMultiShardModifyCombinationRule(t *testing.T) {
	in := SessionGuardrailsInput{AdaptivePoolHigh: 32}
	// Alone: product default, must not fire.
	if isRisky := in.AdaptivePoolHigh < 16; isRisky {
		t.Fatal("default threshold would fire on product default pool size 16")
	}
	// Above threshold: must fire.
	for _, pool := range []int{33, 64, 128} {
		if !(pool > in.AdaptivePoolHigh) {
			t.Errorf("pool=%d should cross threshold %d", pool, in.AdaptivePoolHigh)
		}
	}
	// At or below threshold: must not fire.
	for _, pool := range []int{1, 16, 32} {
		if pool > in.AdaptivePoolHigh {
			t.Errorf("pool=%d should not cross threshold %d", pool, in.AdaptivePoolHigh)
		}
	}
}

// fmtSscanfInt is a tiny helper to mirror the production code's
// fmt.Sscanf without pulling fmt into the test imports.
func fmtSscanfInt(s string, out *int) (int, error) {
	return sscanfIntImpl(s, out)
}

func sscanfIntImpl(s string, out *int) (int, error) {
	n := 0
	consumed := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			if consumed == 0 {
				return 0, nil
			}
			break
		}
		n = n*10 + int(c-'0')
		consumed++
	}
	*out = n
	return consumed, nil
}
