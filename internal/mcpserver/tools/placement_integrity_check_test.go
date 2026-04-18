// citus-mcp: unit tests for placement_integrity_check pure helpers.

package tools

import (
	"strings"
	"testing"
)

func TestPlacementKeyRoundTrip(t *testing.T) {
	k := placementKey("Worker-01.example.COM", 5433, "Public.Events", 10203)
	if !strings.Contains(k, "worker-01.example.com") {
		t.Errorf("host not lowercased: %q", k)
	}
	if !strings.Contains(k, "public.events") {
		t.Errorf("relation not lowercased: %q", k)
	}
	if !strings.HasSuffix(k, "|10203") {
		t.Errorf("shard id not at end: %q", k)
	}
	host, port, rel, sid := parsePlacementKey(k)
	if host != "worker-01.example.com" || port != 5433 || rel != "public.events" || sid != 10203 {
		t.Errorf("round trip broken: %q %d %q %d", host, port, rel, sid)
	}
}

func TestShardSuffixRE(t *testing.T) {
	cases := []struct {
		name    string
		want    string
		wantSID int64
	}{
		{"events_102089", "102089", 102089},
		{"my_table_name_5", "5", 5},
		// Not a shard table:
		{"events_metadata", "", 0},
		{"mytable", "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := shardSuffixRE.FindStringSubmatch(c.name)
			if c.want == "" {
				if m != nil {
					t.Errorf("expected no match for %q, got %v", c.name, m)
				}
				return
			}
			if m == nil || m[1] != c.want {
				t.Errorf("for %q: want suffix %q, got %v", c.name, c.want, m)
			}
		})
	}
}

func TestCoerceInt64(t *testing.T) {
	cases := map[any]int64{
		float64(123):  123,
		int64(-5):     -5,
		int(7):        7,
		"42":          42,
		"notanumber":  0,
		nil:           0,
		true:          0,
	}
	for in, want := range cases {
		if got := coerceInt64(in); got != want {
			t.Errorf("coerceInt64(%v) = %d, want %d", in, got, want)
		}
	}
}

func TestBuildPlacementPlaybook(t *testing.T) {
	o := &PlacementIntegrityOutput{
		GhostPlacements:  []GhostPlacement{{ShardID: 1}},
		OrphanTables:     []OrphanTable{{ShardIDGuess: 2}},
		InactiveWithData: []InactiveWithData{{ShardID: 3}},
		SizeDrifts:       []SizeDrift{{ShardID: 4}},
		Playbook:         []string{},
		Recommendations:  []string{},
	}
	buildPlacementPlaybook(o)
	joined := strings.Join(o.Playbook, "\n")
	for _, want := range []string{
		"citus_copy_shard_placement",
		"citus_cleanup_orphaned_resources",
		"citus_update_shard_statistics",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("playbook missing %q; got:\n%s", want, joined)
		}
	}

	// Empty case
	empty := &PlacementIntegrityOutput{Playbook: []string{}}
	buildPlacementPlaybook(empty)
	if len(empty.Playbook) != 1 || !strings.Contains(empty.Playbook[0], "No integrity issues") {
		t.Errorf("empty playbook wrong: %v", empty.Playbook)
	}
}

func TestPlacementPlaybookStaleStats(t *testing.T) {
	// Regression: stale_stats alone must trigger its own playbook entry,
	// not fall through to the "no integrity issues" message.
	o := &PlacementIntegrityOutput{
		StaleStats:      []SizeDrift{{ShardID: 1}},
		Playbook:        []string{},
		Recommendations: []string{},
	}
	buildPlacementPlaybook(o)
	joined := strings.Join(o.Playbook, "\n")
	if !strings.Contains(joined, "STALE STATS") {
		t.Errorf("expected STALE STATS playbook section; got:\n%s", joined)
	}
	if strings.Contains(joined, "No integrity issues") {
		t.Errorf("stale_stats-only output must NOT say 'No integrity issues'; got:\n%s", joined)
	}
}

func TestPlacementPlaybookPartialResults(t *testing.T) {
	// Regression: PartialResults must produce its own playbook section and
	// suppress the "no issues" message — even when every other class is empty.
	o := &PlacementIntegrityOutput{
		PartialResults:  true,
		Playbook:        []string{},
		Recommendations: []string{},
	}
	buildPlacementPlaybook(o)
	joined := strings.Join(o.Playbook, "\n")
	if !strings.Contains(joined, "PARTIAL RESULTS") {
		t.Errorf("expected PARTIAL RESULTS section; got:\n%s", joined)
	}
	if strings.Contains(joined, "No integrity issues") {
		t.Errorf("partial_results output must NOT say 'No integrity issues'; got:\n%s", joined)
	}
}
