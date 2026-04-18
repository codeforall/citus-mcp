package tools

import (
	"errors"
	"testing"
)

// P0-#4: tools_skipped must be the single source of truth for "no
// output". A section that returns skipSection must appear in
// ToolsSkipped with a structured reason, not in ToolsRun, and not in
// SectionErrors. A nil return must go to ToolsRun. A real error must
// go to SectionErrors.

func TestSkipSection_IsErrorWithReason(t *testing.T) {
	err := skipSection("pg_stat_statements_not_installed", "install it")
	var se *skipError
	if !errors.As(err, &se) {
		t.Fatalf("skipSection should produce a *skipError recognizable via errors.As")
	}
	if se.Reason != "pg_stat_statements_not_installed" {
		t.Fatalf("Reason lost through errors.As; got %q", se.Reason)
	}
	if se.Detail != "install it" {
		t.Fatalf("Detail lost through errors.As; got %q", se.Detail)
	}
}

// classifyRunResult mirrors the routing logic in the FullReport run()
// wrapper, extracted so we can unit-test the skipError contract
// without spinning up a full aggregator.
func classifyRunResult(err error) (destination string, reason string) {
	if err == nil {
		return "tools_run", ""
	}
	var se *skipError
	if errors.As(err, &se) {
		return "tools_skipped", se.Reason
	}
	return "section_errors", err.Error()
}

func TestClassifyRunResult_RoutesCorrectly(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		wantDst string
		wantR   string
	}{
		{"success goes to tools_run", nil, "tools_run", ""},
		{"skipError routes to tools_skipped with reason", skipSection("no_data", ""), "tools_skipped", "no_data"},
		{"real error routes to section_errors", errors.New("boom"), "section_errors", "boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dst, r := classifyRunResult(tc.err)
			if dst != tc.wantDst {
				t.Fatalf("destination: want %s, got %s", tc.wantDst, dst)
			}
			if r != tc.wantR {
				t.Fatalf("reason/msg: want %q, got %q", tc.wantR, r)
			}
		})
	}
}
