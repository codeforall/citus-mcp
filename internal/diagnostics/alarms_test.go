// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for the alarms framework.

package diagnostics

import (
	"sync"
	"testing"
	"time"
)

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func TestEmitDeduplicates(t *testing.T) {
	s := NewSink(16)
	a := Alarm{Kind: "k", Source: "tool", Severity: SeverityWarning, Node: "n1", Object: "t"}
	first := s.Emit(a)
	second := s.Emit(a)
	if first.ID != second.ID {
		t.Fatalf("expected stable fingerprint, got %q and %q", first.ID, second.ID)
	}
	if second.SeenCount != 2 {
		t.Fatalf("expected SeenCount=2, got %d", second.SeenCount)
	}
	if len(s.Snapshot()) != 1 {
		t.Fatalf("expected single entry after dedup, got %d", len(s.Snapshot()))
	}
}

func TestEmitReEmitUnacks(t *testing.T) {
	s := NewSink(16)
	a := Alarm{Kind: "k", Source: "tool", Severity: SeverityWarning}
	x := s.Emit(a)
	if !s.Ack(x.ID, "tester") {
		t.Fatal("ack failed")
	}
	y := s.Emit(a)
	if y.Acked {
		t.Fatal("re-emitted alarm should be un-acked")
	}
}

func TestListFiltersAndSorts(t *testing.T) {
	s := NewSink(16)
	now := time.Unix(1_700_000_000, 0)
	s.clock = fixedClock(now)
	s.Emit(Alarm{Kind: "a", Source: "t", Severity: SeverityInfo})
	s.clock = fixedClock(now.Add(1 * time.Second))
	s.Emit(Alarm{Kind: "b", Source: "t", Severity: SeverityCritical})
	s.clock = fixedClock(now.Add(2 * time.Second))
	s.Emit(Alarm{Kind: "c", Source: "t", Severity: SeverityWarning})

	got := s.List(Filter{MinSeverity: SeverityWarning})
	if len(got) != 2 {
		t.Fatalf("expected 2 alarms at >=warning, got %d", len(got))
	}
	if got[0].Severity != SeverityCritical {
		t.Fatalf("expected critical first, got %s", got[0].Severity)
	}

	one := s.List(Filter{Kind: "a", MinSeverity: SeverityInfo})
	if len(one) != 1 || one[0].Kind != "a" {
		t.Fatalf("kind filter failed: %+v", one)
	}

	since := now.Add(1500 * time.Millisecond)
	recent := s.List(Filter{Since: &since, MinSeverity: SeverityInfo})
	if len(recent) != 1 || recent[0].Kind != "c" {
		t.Fatalf("since filter failed: %+v", recent)
	}
}

func TestAckAndAckMatching(t *testing.T) {
	s := NewSink(16)
	x := s.Emit(Alarm{Kind: "a", Source: "t", Severity: SeverityWarning, Node: "n1"})
	y := s.Emit(Alarm{Kind: "b", Source: "t", Severity: SeverityWarning, Node: "n1"})
	_ = s.Emit(Alarm{Kind: "c", Source: "t", Severity: SeverityInfo, Node: "n2"})

	if !s.Ack(x.ID, "u") {
		t.Fatal("ack by id failed")
	}
	if got := s.Ack("bogus", "u"); got {
		t.Fatal("ack of missing id should fail")
	}

	if n := s.AckMatching(Filter{Node: "n1", MinSeverity: SeverityWarning}, "u"); n != 1 {
		t.Fatalf("expected 1 ack (y), got %d", n)
	}
	_ = y
	if open := s.List(Filter{MinSeverity: SeverityInfo}); len(open) != 1 {
		t.Fatalf("expected 1 open alarm after acks, got %d", len(open))
	}
}

func TestEvictionPrefersAcked(t *testing.T) {
	s := NewSink(3)
	a1 := s.Emit(Alarm{Kind: "a", Source: "t", Severity: SeverityInfo})
	_ = s.Emit(Alarm{Kind: "b", Source: "t", Severity: SeverityInfo})
	_ = s.Emit(Alarm{Kind: "c", Source: "t", Severity: SeverityInfo})
	s.Ack(a1.ID, "u")
	_ = s.Emit(Alarm{Kind: "d", Source: "t", Severity: SeverityInfo}) // triggers evict
	for _, al := range s.Snapshot() {
		if al.ID == a1.ID {
			t.Fatal("acked oldest should have been evicted first")
		}
	}
}

func TestConcurrentEmit(t *testing.T) {
	s := NewSink(1024)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			s.Emit(Alarm{Kind: "same", Source: "t", Severity: SeverityInfo})
			s.Emit(Alarm{Kind: "unique", Source: "t", Severity: SeverityInfo, Object: string(rune('a' + i%26))})
		}()
	}
	wg.Wait()
	snap := s.Snapshot()
	// "same" collapses to 1 entry; "unique" expands to up to 26.
	var sameCount int
	for _, a := range snap {
		if a.Kind == "same" {
			sameCount++
			if a.SeenCount < 1 {
				t.Fatal("seen count should be >= 1")
			}
		}
	}
	if sameCount != 1 {
		t.Fatalf("expected deduplicated 'same' alarm, got %d", sameCount)
	}
}

func TestStats(t *testing.T) {
	s := NewSink(16)
	s.Emit(Alarm{Kind: "a", Source: "t", Severity: SeverityCritical})
	w := s.Emit(Alarm{Kind: "b", Source: "t", Severity: SeverityWarning})
	s.Ack(w.ID, "u")
	stats := s.Stats()
	if stats["total"] != 2 || stats["critical"] != 1 || stats["acked"] != 1 {
		t.Fatalf("unexpected stats: %v", stats)
	}
}

func TestNilSinkIsSafe(t *testing.T) {
	var s *Sink
	if got := s.Emit(Alarm{Kind: "x"}); got == nil {
		t.Fatal("Emit on nil sink should return the alarm, not nil")
	}
	if s.List(Filter{}) != nil {
		t.Fatal("List on nil sink should return nil")
	}
	if s.Ack("x", "y") {
		t.Fatal("Ack on nil sink should return false")
	}
	if s.AckMatching(Filter{}, "y") != 0 {
		t.Fatal("AckMatching on nil sink should return 0")
	}
	s.Clear()
	if stats := s.Stats(); len(stats) != 0 {
		t.Fatalf("Stats on nil sink should be empty, got %v", stats)
	}
}
