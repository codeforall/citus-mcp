package tools

import (
	"testing"

	"citus-mcp/internal/diagnostics"
)

// P0-#3: the `Severity` field must describe the FINDING, not the class
// of GUC. When a GUC has not drifted there is no finding, so Severity
// must be "". Class, on the other hand, describes the inherent
// criticality of drift on this GUC and is always populated.
func TestBuildGucDriftReport_SeverityEmptyWhenNoDrift(t *testing.T) {
	rule := driftRule{
		Name:     "max_connections",
		Severity: diagnostics.SeverityCritical,
		Note:     "n",
	}
	nodeValues := map[string]map[string]string{
		"coordinator":    {"max_connections": "100"},
		"127.0.0.1:5433": {"max_connections": "100"},
	}
	rep := buildGucDriftReport(rule, nodeValues)
	if rep.Drifted {
		t.Fatalf("expected Drifted=false, got true")
	}
	if rep.Class != "critical" {
		t.Fatalf("Class: want critical, got %q", rep.Class)
	}
	if rep.Severity != "" {
		t.Fatalf("Severity on a non-finding must be empty, got %q", rep.Severity)
	}
	if rep.DistinctValues != 1 {
		t.Fatalf("DistinctValues: want 1, got %d", rep.DistinctValues)
	}
}

func TestBuildGucDriftReport_SeverityEqualsClassWhenDrifted(t *testing.T) {
	rule := driftRule{
		Name:     "max_worker_processes",
		Severity: diagnostics.SeverityCritical,
	}
	nodeValues := map[string]map[string]string{
		"coordinator":    {"max_worker_processes": "8"},
		"127.0.0.1:5433": {"max_worker_processes": "10"},
	}
	rep := buildGucDriftReport(rule, nodeValues)
	if !rep.Drifted {
		t.Fatalf("expected Drifted=true")
	}
	if rep.Class != "critical" || rep.Severity != "critical" {
		t.Fatalf("drifted row must have both Class and Severity = critical; got class=%q severity=%q", rep.Class, rep.Severity)
	}
}

func TestBuildGucDriftReport_MissingValueCountedAsUnknownNotDrift(t *testing.T) {
	rule := driftRule{Name: "x", Severity: diagnostics.SeverityInfo}
	// One node returned no row for this GUC; distinct-values counting
	// must exclude "(not set / unreachable)" so an unreachable node
	// alone does NOT create a spurious drift.
	nodeValues := map[string]map[string]string{
		"coordinator":    {"x": "42"},
		"127.0.0.1:5433": {}, // missing
	}
	rep := buildGucDriftReport(rule, nodeValues)
	if rep.Drifted {
		t.Fatalf("missing row must not count as drift; got Drifted=true")
	}
	if rep.Severity != "" {
		t.Fatalf("non-finding must have empty Severity")
	}
	if rep.Values["127.0.0.1:5433"] != "(not set / unreachable)" {
		t.Fatalf("missing value must be tagged as unreachable")
	}
}
