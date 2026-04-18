package guc

import "testing"

// P0-#5: config advisor "magic number" rules. Every threshold in the
// replication-slot / wal-sender / worker-processes rules must come from
// a cluster fact (a measurable GUC or pg_replication_slots), not a
// hardcoded constant. These tests lock in that the formulas:
//   * scale with the GUCs the rules claim to, and
//   * reflect the existing replication-slot usage when the coordinator
//     already has slots allocated (so we advise ON TOP of what is there,
//     not over it).

func mkCtx(gucs map[string]string, existingSlots int64, workers int) *AnalysisContext {
	all := map[string]GUCValue{}
	for k, v := range gucs {
		all[k] = GUCValue{Name: k, Setting: v}
	}
	return &AnalysisContext{
		AllGUCs:                  all,
		PostgresGUCs:             all,
		CitusGUCs:                all,
		WorkerCount:              workers,
		ExistingReplicationSlots: existingSlots,
	}
}

func TestRuleMaxReplicationSlots_FloorScalesWithBgExecAndExistingSlots(t *testing.T) {
	// bg_executors=4, existing=5 -> min = 4+5+2 = 11.
	// max_replication_slots=10 is below; rule must fire.
	ctx := mkCtx(map[string]string{
		"max_replication_slots":                        "10",
		"citus.max_background_task_executors_per_node": "4",
	}, 5, 0)
	findings := (&RuleMaxReplicationSlots{}).Evaluate(ctx)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	ev := findings[0].Evidence
	if ev["min_required"].(int64) != 11 {
		t.Fatalf("min_required: want 11, got %v", ev["min_required"])
	}
	if ev["existing_active_slots"].(int64) != 5 {
		t.Fatalf("existing slots must be surfaced; got %v", ev["existing_active_slots"])
	}
}

func TestRuleMaxReplicationSlots_NoFalsePositiveWhenSufficient(t *testing.T) {
	// 64 slots, bg=4, existing=0 -> min=6; 64>6: no finding.
	ctx := mkCtx(map[string]string{
		"max_replication_slots":                        "64",
		"citus.max_background_task_executors_per_node": "4",
	}, 0, 0)
	if f := (&RuleMaxReplicationSlots{}).Evaluate(ctx); len(f) != 0 {
		t.Fatalf("expected no findings at adequate budget; got %d", len(f))
	}
}

func TestRuleMaxWalSenders_MatchesSlotFormula(t *testing.T) {
	// bg=2, existing=3 -> min=7. senders=5 < 7 -> finding.
	ctx := mkCtx(map[string]string{
		"max_wal_senders":                              "5",
		"citus.max_background_task_executors_per_node": "2",
	}, 3, 0)
	findings := (&RuleMaxWalSenders{}).Evaluate(ctx)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Evidence["min_required"].(int64) != 7 {
		t.Fatalf("min_required: want 7, got %v", findings[0].Evidence["min_required"])
	}
}

func TestRuleMaxWorkerProcesses_SumsSourceGrouped(t *testing.T) {
	// autovac=3 + parallel=8 + logrep=4 + citus_bg=1 + citus_maint=1 + headroom=2 = 19.
	// max_worker_processes=10 < 19 -> finding.
	ctx := mkCtx(map[string]string{
		"max_worker_processes":                          "10",
		"autovacuum_max_workers":                        "3",
		"max_parallel_workers":                          "8",
		"max_logical_replication_workers":               "4",
		"citus.max_background_task_executors_per_node":  "1",
	}, 0, 0)
	findings := (&RuleMaxWorkerProcesses{}).Evaluate(ctx)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Evidence["min_required"].(int64) != 19 {
		t.Fatalf("min_required: want 19, got %v", findings[0].Evidence["min_required"])
	}
	// bumping citus.max_background_task_executors_per_node to 8 -> 26; rule must track.
	ctx = mkCtx(map[string]string{
		"max_worker_processes":                          "20",
		"autovacuum_max_workers":                        "3",
		"max_parallel_workers":                          "8",
		"max_logical_replication_workers":               "4",
		"citus.max_background_task_executors_per_node":  "8",
	}, 0, 0)
	findings = (&RuleMaxWorkerProcesses{}).Evaluate(ctx)
	if len(findings) != 1 || findings[0].Evidence["min_required"].(int64) != 26 {
		t.Fatalf("formula must scale with bg_executors; got %+v", findings)
	}
}
