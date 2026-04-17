package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenAndRecord(t *testing.T) {
	dir, _ := os.MkdirTemp("", "citus-snap-")
	defer os.RemoveAll(dir)
	p := filepath.Join(dir, "test.db")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Record(context.Background(), "c1", "memory", "node1", `{"bytes":1024}`); err != nil {
		t.Fatal(err)
	}
	rows, err := s.List(context.Background(), "c1", "memory", time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	st, _ := os.Stat(p)
	if m := st.Mode().Perm(); m != 0o600 {
		t.Fatalf("want 0600, got %o", m)
	}
}

func TestPrune(t *testing.T) {
	dir, _ := os.MkdirTemp("", "citus-snap-")
	defer os.RemoveAll(dir)
	s, _ := Open(filepath.Join(dir, "t.db"))
	defer s.Close()
	_, _ = s.Record(context.Background(), "c", "k", "", "{}")
	n, err := s.Prune(context.Background(), -1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1 pruned, got %d", n)
	}
}
