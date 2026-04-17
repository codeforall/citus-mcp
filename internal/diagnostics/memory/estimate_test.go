// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for the memory estimator.

package memory

import "testing"

// Calibration regression test: an empty cluster should yield only the base
// Citus overhead.
func TestEmptyCluster(t *testing.T) {
	got := EstimateCitusMetadata(nil)
	if got.Bytes != CitusMetadataBaseBytes {
		t.Fatalf("empty cluster Citus bytes=%d want %d", got.Bytes, CitusMetadataBaseBytes)
	}
	if got.NShards != 0 || got.NTableEntries != 0 {
		t.Fatalf("empty cluster counts non-zero: %+v", got)
	}
}

// Calibration regression test: the 7-table / 193-shard layout measured on
// the live cluster yielded ~52 KB used in MetadataCacheMemoryContext. Our
// estimator should land within its declared uncertainty band around that.
func TestCalibration193Shards(t *testing.T) {
	shapes := []TableShape{
		// reference table: 1 shard × rf=2 (number of nodes)
		{Name: "countries", IsReference: true, NShards: 1, ReplicationFactor: 2, NAttributes: 2, NIndexes: 1},
		// plain distributed: 32 shards, rf=1
		{Name: "events", NShards: 32, ReplicationFactor: 1, NAttributes: 4, NIndexes: 1},
		// partitioned parent: 32 shards × rf=1, parent of 4
		{Name: "orders", IsPartitionParent: true, NShards: 32, ReplicationFactor: 1, NAttributes: 4, NIndexes: 1, NPartitionChildren: 4},
		// 4 partition children, each 32 shards
		{Name: "orders_2026_01", NShards: 32, ReplicationFactor: 1, NAttributes: 4},
		{Name: "orders_2026_02", NShards: 32, ReplicationFactor: 1, NAttributes: 4},
		{Name: "orders_2026_03", NShards: 32, ReplicationFactor: 1, NAttributes: 4},
		{Name: "orders_2026_04", NShards: 32, ReplicationFactor: 1, NAttributes: 4},
	}
	est := EstimateCitusMetadata(shapes)
	const measuredUsed = 52496 // bytes used, from live cluster calibration
	if est.Bytes < int64(float64(measuredUsed)*0.5) || est.Bytes > int64(float64(measuredUsed)*2.0) {
		t.Fatalf("estimator drifted far from calibration: est=%d measured=%d", est.Bytes, measuredUsed)
	}
	if est.NTableEntries != 7 {
		t.Fatalf("expected 7 table entries, got %d", est.NTableEntries)
	}
	if est.NShards != 1+32+32+32*4 {
		t.Fatalf("expected 161 shards (1+32+32+128), got %d", est.NShards)
	}
}

// Partition-doubling simulator invariant: doubling partition children should
// ~double the Citus metadata cost attributable to that table family.
func TestPartitionDoublingIsLinear(t *testing.T) {
	mk := func(nChildren int) []TableShape {
		s := []TableShape{{Name: "p", IsPartitionParent: true, NShards: 32, ReplicationFactor: 1, NPartitionChildren: nChildren}}
		for i := 0; i < nChildren; i++ {
			s = append(s, TableShape{Name: "c", NShards: 32, ReplicationFactor: 1})
		}
		return s
	}
	a := EstimateCitusMetadata(mk(4))
	b := EstimateCitusMetadata(mk(8))
	// "per-family" bytes excluding the fixed base
	deltaA := a.Bytes - CitusMetadataBaseBytes
	deltaB := b.Bytes - CitusMetadataBaseBytes
	if deltaB <= deltaA {
		t.Fatalf("doubling partitions must grow metadata: a=%d b=%d", deltaA, deltaB)
	}
	// Expect roughly 1.5×-2.0× growth (parent overhead stays constant, children double)
	ratio := float64(deltaB) / float64(deltaA)
	if ratio < 1.5 || ratio > 2.2 {
		t.Fatalf("partition doubling ratio out of expected range: %.2f", ratio)
	}
}

func TestPgCacheIncludesShardRelsOnWorker(t *testing.T) {
	shapes := []TableShape{{NShards: 32, ReplicationFactor: 1, NAttributes: 5, NIndexes: 2}}
	coord := EstimatePgCache(shapes, false)
	worker := EstimatePgCache(shapes, true)
	if worker.Bytes <= coord.Bytes {
		t.Fatalf("worker (with shard rels) should cost more than coordinator: coord=%d worker=%d", coord.Bytes, worker.Bytes)
	}
	if worker.NRelations < coord.NRelations+32 {
		t.Fatalf("worker should count shard rels in NRelations, got coord=%d worker=%d", coord.NRelations, worker.NRelations)
	}
}

func TestEstimateMonotonicInShards(t *testing.T) {
	a := EstimateCitusMetadata([]TableShape{{NShards: 32, ReplicationFactor: 1}})
	b := EstimateCitusMetadata([]TableShape{{NShards: 64, ReplicationFactor: 1}})
	if b.Bytes <= a.Bytes {
		t.Fatalf("more shards must cost more: a=%d b=%d", a.Bytes, b.Bytes)
	}
}
