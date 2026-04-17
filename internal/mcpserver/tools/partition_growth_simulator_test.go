// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT

package tools

import (
	"citus-mcp/internal/diagnostics/memory"
	"testing"
)

func TestNormalizeTable(t *testing.T) {
	cases := map[string]string{
		"orders":         "public.orders",
		"public.orders":  "public.orders",
		"myapp.events":   "myapp.events",
		"  foo  ":        "public.foo",
	}
	for in, want := range cases {
		if got := normalizeTable(in); got != want {
			t.Errorf("normalizeTable(%q)=%q want %q", in, got, want)
		}
	}
}

// Verify snapshot is additive / monotonic in partition count: doubling
// partitions on a parent table must monotonically increase per-backend
// bytes (non-decreasing), and the increase should scale roughly linearly
// with the added children.
func TestSnapshotMonotonicInPartitions(t *testing.T) {
	parent := memory.TableShape{
		Oid: 1, Name: "orders", Schema: "public",
		IsPartitionParent: true, NShards: 32, ReplicationFactor: 1,
		NAttributes: 5, NIndexes: 1, NPartitionChildren: 4,
	}
	child := func(oid int64, name string) memory.TableShape {
		return memory.TableShape{
			Oid: oid, Name: name, Schema: "public", ParentOid: 1,
			NShards: 32, ReplicationFactor: 1, NAttributes: 5, NIndexes: 1,
		}
	}
	shapes4 := []memory.TableShape{parent,
		child(2, "c1"), child(3, "c2"), child(4, "c3"), child(5, "c4"),
	}
	shapes8 := append([]memory.TableShape{}, shapes4...)
	for i := 0; i < 4; i++ {
		shapes8 = append(shapes8, child(int64(6+i), "cN"))
	}
	// also bump parent's NPartitionChildren to 8 for PG cache modelling
	shapes8[0].NPartitionChildren = 8

	s4 := memSnapshot(shapes4, false)
	s8 := memSnapshot(shapes8, false)
	if s8.PerBackendBytes <= s4.PerBackendBytes {
		t.Fatalf("doubling partitions must grow footprint: 4-part=%d 8-part=%d", s4.PerBackendBytes, s8.PerBackendBytes)
	}
	// Citus side: 4 extra children, each with 32 shards -> must add per_shard * 128 bytes at minimum.
	deltaCitus := s8.Citus.Bytes - s4.Citus.Bytes
	if deltaCitus < 4*memory.CitusPerTableBytes {
		t.Fatalf("Citus delta %d smaller than floor 4*per_table=%d", deltaCitus, 4*memory.CitusPerTableBytes)
	}
}
