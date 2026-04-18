package tools

import "testing"

func TestBuildColocationSkewFlagsHotShard(t *testing.T) {
	shards := []shardPlacementRow{
		// colocation 11: events + orders on 4 shards each (colocated), one shard HUGE on both
		{ShardID: 101, Host: "w1", Port: 5433, TableName: "public.events", ColocationID: 11},
		{ShardID: 102, Host: "w1", Port: 5433, TableName: "public.events", ColocationID: 11},
		{ShardID: 103, Host: "w1", Port: 5433, TableName: "public.events", ColocationID: 11},
		{ShardID: 104, Host: "w1", Port: 5433, TableName: "public.events", ColocationID: 11},
		{ShardID: 201, Host: "w1", Port: 5433, TableName: "public.orders", ColocationID: 11},
		{ShardID: 202, Host: "w1", Port: 5433, TableName: "public.orders", ColocationID: 11},
		// colocation 20: balanced
		{ShardID: 301, Host: "w1", Port: 5433, TableName: "public.users", ColocationID: 20},
		{ShardID: 302, Host: "w1", Port: 5433, TableName: "public.users", ColocationID: 20},
		// ref table — must be skipped entirely
		{ShardID: 401, Host: "c", Port: 5432, TableName: "public.countries", ColocationID: 0, IsReference: true},
	}
	bytes := map[int64]int64{
		101: 26 * 1024 * 1024, // 26 MiB — the hot shard
		102: 200 * 1024,
		103: 250 * 1024,
		104: 150 * 1024,
		201: 50 * 1024,
		202: 60 * 1024,
		301: 100 * 1024,
		302: 100 * 1024,
		401: 16 * 1024,
	}

	cs := buildColocationSkew(shards, bytes)
	if len(cs) != 2 {
		t.Fatalf("expected 2 colocation groups (ref must be skipped), got %d", len(cs))
	}
	// First entry is the hottest.
	if cs[0].ColocationID != 11 {
		t.Errorf("expected colocation 11 first (hottest), got %d", cs[0].ColocationID)
	}
	if cs[0].Verdict != "critical" {
		t.Errorf("expected critical verdict for colocation 11, got %q (ratio=%.2f)", cs[0].Verdict, cs[0].MaxOverAvg)
	}
	if cs[0].HotShardID != 101 {
		t.Errorf("expected hot shard id 101, got %d", cs[0].HotShardID)
	}
	if cs[1].ColocationID != 20 || cs[1].Verdict != "ok" {
		t.Errorf("expected colocation 20 ok, got id=%d verdict=%q", cs[1].ColocationID, cs[1].Verdict)
	}
}

func TestBuildHotShardsEmitsRemediation(t *testing.T) {
	shards := []shardPlacementRow{
		{ShardID: 1, TableName: "public.events", ColocationID: 11},
		{ShardID: 2, TableName: "public.events", ColocationID: 11},
		{ShardID: 3, TableName: "public.events", ColocationID: 11},
		{ShardID: 4, TableName: "public.events", ColocationID: 11},
	}
	bytes := map[int64]int64{1: 100 * 1024 * 1024, 2: 1024, 3: 1024, 4: 1024}
	hot := buildHotShards(shards, bytes, 10)
	if len(hot) != 1 {
		t.Fatalf("expected 1 hot shard, got %d", len(hot))
	}
	if hot[0].ShardID != 1 {
		t.Errorf("hot shard should be id=1, got %d", hot[0].ShardID)
	}
	if hot[0].Remediation == "" {
		t.Error("expected a non-empty remediation string with isolate_tenant_to_new_shard")
	}
}

func TestBuildHotShardsSkipsSingleShardTable(t *testing.T) {
	shards := []shardPlacementRow{{ShardID: 1, TableName: "public.lookup"}}
	bytes := map[int64]int64{1: 1 << 30}
	if got := buildHotShards(shards, bytes, 10); len(got) != 0 {
		t.Errorf("single-shard tables cannot be skewed by definition; got %d hot shards", len(got))
	}
}

// TestClusterSkewDecisionSingleDataNode covers the P0 fix where a
// coordinator with shouldhaveshards=false must not count as a data-holding
// node for cluster-wide skew. With one real worker the metric is undefined.
func TestClusterSkewDecisionSingleDataNode(t *testing.T) {
	s, warn := clusterSkewDecision([]float64{41222144}, "bytes", 1)
	if s.Warning != "single_data_node" {
		t.Errorf("want warning 'single_data_node', got %q", s.Warning)
	}
	if s.Max != 0 || s.Min != 0 {
		t.Errorf("single-data-node score must not report numeric min/max; got min=%v max=%v", s.Min, s.Max)
	}
	if warn == "" {
		t.Error("expected a human-readable warning explaining per_colocation fallback")
	}
}

func TestClusterSkewDecisionNoDataNodes(t *testing.T) {
	s, warn := clusterSkewDecision(nil, "bytes", 0)
	if s.Warning != "single_data_node" {
		t.Errorf("got %q", s.Warning)
	}
	if warn == "" || warn[:2] != "no" {
		t.Errorf("want 'no node has ...' message, got %q", warn)
	}
}

func TestClusterSkewDecisionMultiNodeComputes(t *testing.T) {
	// 3 data-holding nodes, different byte totals — metric must be computed,
	// no degenerate warning.
	s, warn := clusterSkewDecision([]float64{100, 400, 200}, "bytes", 3)
	if warn != "" {
		t.Errorf("unexpected degenerate warning: %q", warn)
	}
	if s.Max != 400 || s.Min != 100 {
		t.Errorf("max/min not populated; got %+v", s)
	}
	if s.Warning == "single_data_node" {
		t.Error("multi-node decision must not tag single_data_node")
	}
}
