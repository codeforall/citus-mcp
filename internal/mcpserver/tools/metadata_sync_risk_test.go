package tools

import "testing"

// Validates that the NLOCKENTS capacity formula matches PostgreSQL's
// actual sizing (src/backend/storage/lmgr/lock.c). Ground-truth numbers
// below came from a live PG17 instance:
//
//	max_connections=100, max_prepared_transactions=200,
//	max_wal_senders=10, max_worker_processes=8, autovacuum_max_workers=3
//	=> MaxBackends = 100+3+8+10 = 121
//	=> NLOCKENTS  = 64 * (121+200) = 20544
func TestComputeLockCapsMatchesNLOCKENTS(t *testing.T) {
	soft, hard := computeLockCaps(64, 121, 200)
	if hard != 20544 {
		t.Errorf("hard cap: want 20544 (64 × (121+200)), got %d", hard)
	}
	if soft != hard/2 {
		t.Errorf("soft cap should be half of hard; soft=%d hard=%d", soft, hard)
	}
}

func TestComputeLockCapsAppliesFloorOnTinyClusters(t *testing.T) {
	// If max_backends is very small, hard/2 would be below the floor of
	// lpt*16 — the floor must kick in so we don't report implausible caps.
	soft, hard := computeLockCaps(64, 4, 0)
	if hard != 256 {
		t.Errorf("hard cap: want 256 (64×4), got %d", hard)
	}
	// hard/2 = 128, floor = 64*16 = 1024 > 128, so soft should be 1024.
	if soft != 1024 {
		t.Errorf("soft cap: expected floor 1024 (lpt*16), got %d", soft)
	}
}

func TestComputeLockCapsScalesWithLpt(t *testing.T) {
	// Doubling max_locks_per_transaction must double NLOCKENTS.
	_, h1 := computeLockCaps(64, 121, 200)
	_, h2 := computeLockCaps(128, 121, 200)
	if h2 != 2*h1 {
		t.Errorf("hard cap should scale linearly with lpt; h1=%d h2=%d", h1, h2)
	}
}
