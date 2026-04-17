// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Estimators for per-backend memory pressure. See package docs in doc.go.

package memory

// --- Citus MetadataCacheMemoryContext coefficients -------------------------
//
// Measured on a backend after touching 7 distributed tables spanning 193
// shards (1 reference, 1 plain distributed, 1 partitioned parent + 4
// children): MetadataCacheMemoryContext grew from 288 B used to 52,496 B
// used. Regressing that single data point (with per_placement/fk held
// constant) gives ~220 B/shard + ~400 B per table-entry overhead. We carry
// a wide uncertainty band (0.6× – 1.8×) so the "risk" output is honest.
//
// NOTE: the per-shard coefficient also implicitly absorbs a small
// contribution from per_placement (rf=1 in the measurement). The
// per_placement term separately adds cost for replicated/reference tables
// where rf>1.

const (
	CitusMetadataBaseBytes = 8192
	CitusPerTableBytes     = 400
	CitusPerShardBytes     = 220
	CitusPerPlacementBytes = 80
	CitusPerFKEdgeBytes    = 32
	CitusEstimateLowMul    = 0.60
	CitusEstimateHighMul   = 1.80
)

// --- PostgreSQL CacheMemoryContext coefficients ---------------------------

const (
	PgCacheBaseBytes               = 500_000
	PgRelCachePerRelationBytes     = 1800
	PgRelCachePerIndexBytes        = 1200
	PgCatCachePerAttrBytes         = 90
	PgPartitionParentOverheadBytes = 4000
	PgPartitionPerChildBytes       = 350
	PgCacheEstimateLowMul          = 0.60
	PgCacheEstimateHighMul         = 2.00
)

// TableShape captures per-table inputs to the estimator.
type TableShape struct {
	Oid                int64
	Name               string
	Schema             string
	IsReference        bool
	IsPartitionParent  bool
	ParentOid          int64
	NShards            int
	ReplicationFactor  int
	NAttributes        int
	NIndexes           int
	NFKOut             int
	NFKIn              int
	NPartitionChildren int
}

type CitusEstimate struct {
	Bytes         int64                 `json:"bytes"`
	LowBytes      int64                 `json:"low_bytes"`
	HighBytes     int64                 `json:"high_bytes"`
	Breakdown     []CitusEstimateBucket `json:"breakdown"`
	NTableEntries int                   `json:"n_table_entries"`
	NShards       int                   `json:"n_shards"`
	NPlacements   int                   `json:"n_placements"`
	NFKEdges      int                   `json:"n_fk_edges"`
}

type CitusEstimateBucket struct {
	Kind  string `json:"kind"`
	Bytes int64  `json:"bytes"`
	Note  string `json:"note,omitempty"`
}

type PgCacheEstimate struct {
	Bytes      int64           `json:"bytes"`
	LowBytes   int64           `json:"low_bytes"`
	HighBytes  int64           `json:"high_bytes"`
	Breakdown  []PgCacheBucket `json:"breakdown"`
	NRelations int             `json:"n_relations"`
	NIndexes   int             `json:"n_indexes"`
	NAttrs     int             `json:"n_attrs"`
}

type PgCacheBucket struct {
	Kind  string `json:"kind"`
	Bytes int64  `json:"bytes"`
	Note  string `json:"note,omitempty"`
}

// EstimateCitusMetadataScaled models a backend that has only touched
// `tablesTouched` out of the full `tables` set. Unlike EstimateCitusMetadata
// this returns a proportionally-scaled estimate: `tablesTouched = len(tables)`
// degenerates to the full worst-case estimate; smaller values linearly scale
// the per-table / per-shard / per-placement / fk buckets.
//
// The scaling is proportional (not top-K by size) because the selection of
// tables a real OLTP backend touches is workload-dependent, and proportional
// scaling yields a calibrated "average subset" rather than an optimistic
// best-case or pessimistic worst-case sub-selection.
func EstimateCitusMetadataScaled(tables []TableShape, tablesTouched int) CitusEstimate {
	n := len(tables)
	if n == 0 || tablesTouched <= 0 {
		base := int64(CitusMetadataBaseBytes)
		return CitusEstimate{
			Bytes:     base,
			LowBytes:  int64(float64(base) * CitusEstimateLowMul),
			HighBytes: int64(float64(base) * CitusEstimateHighMul),
			Breakdown: []CitusEstimateBucket{{Kind: "base", Bytes: base, Note: "MetadataCacheMemoryContext fixed overhead"}},
		}
	}
	if tablesTouched >= n {
		return EstimateCitusMetadata(tables)
	}
	full := EstimateCitusMetadata(tables)
	ratio := float64(tablesTouched) / float64(n)
	scaled := CitusEstimate{
		NTableEntries: tablesTouched,
		NShards:       int(float64(full.NShards) * ratio),
		NPlacements:   int(float64(full.NPlacements) * ratio),
		NFKEdges:      int(float64(full.NFKEdges) * ratio),
	}
	var total int64 = CitusMetadataBaseBytes
	scaled.Breakdown = append(scaled.Breakdown, CitusEstimateBucket{Kind: "base", Bytes: CitusMetadataBaseBytes, Note: "MetadataCacheMemoryContext fixed overhead"})
	for _, b := range full.Breakdown {
		if b.Kind == "base" {
			continue
		}
		s := int64(float64(b.Bytes) * ratio)
		total += s
		scaled.Breakdown = append(scaled.Breakdown, CitusEstimateBucket{Kind: b.Kind, Bytes: s, Note: b.Note + " (scaled)"})
	}
	scaled.Bytes = total
	scaled.LowBytes = int64(float64(total) * CitusEstimateLowMul)
	scaled.HighBytes = int64(float64(total) * CitusEstimateHighMul)
	return scaled
}

// EstimateCitusMetadata models the per-backend MetadataCacheMemoryContext
// size from a set of table shapes. Order-invariant, pure.
func EstimateCitusMetadata(tables []TableShape) CitusEstimate {
	e := CitusEstimate{Bytes: CitusMetadataBaseBytes}
	e.Breakdown = append(e.Breakdown, CitusEstimateBucket{
		Kind: "base", Bytes: CitusMetadataBaseBytes,
		Note: "MetadataCacheMemoryContext fixed overhead",
	})
	var perTable, perShard, perPlacement, fkBytes int64
	var shards, placements, fkEdges int
	for _, t := range tables {
		rf := t.ReplicationFactor
		if rf < 1 {
			rf = 1
		}
		perTable += CitusPerTableBytes
		perShard += int64(t.NShards) * CitusPerShardBytes
		perPlacement += int64(t.NShards) * int64(rf) * CitusPerPlacementBytes
		fkBytes += int64(t.NFKIn+t.NFKOut) * CitusPerFKEdgeBytes
		shards += t.NShards
		placements += t.NShards * rf
		fkEdges += t.NFKIn + t.NFKOut
	}
	e.NTableEntries = len(tables)
	e.NShards = shards
	e.NPlacements = placements
	e.NFKEdges = fkEdges
	e.Bytes += perTable + perShard + perPlacement + fkBytes
	e.LowBytes = int64(float64(e.Bytes) * CitusEstimateLowMul)
	e.HighBytes = int64(float64(e.Bytes) * CitusEstimateHighMul)
	e.Breakdown = append(e.Breakdown,
		CitusEstimateBucket{Kind: "per_table", Bytes: perTable, Note: "CitusTableCacheEntry per distributed table (incl. every partition child)"},
		CitusEstimateBucket{Kind: "per_shard", Bytes: perShard, Note: "ShardInterval + pointer per shard"},
		CitusEstimateBucket{Kind: "per_placement", Bytes: perPlacement, Note: "GroupShardPlacement per (shard × replication_factor)"},
		CitusEstimateBucket{Kind: "fk_closure", Bytes: fkBytes, Note: "referenced/referencing FK edges"},
	)
	return e
}

// EstimatePgCache models the PG CacheMemoryContext footprint per backend.
// `includeShardRels` should be true for worker backends in MX mode where
// shard tables are real local relations on that worker.
func EstimatePgCache(tables []TableShape, includeShardRels bool) PgCacheEstimate {
	e := PgCacheEstimate{Bytes: PgCacheBaseBytes}
	e.Breakdown = append(e.Breakdown, PgCacheBucket{
		Kind: "base", Bytes: PgCacheBaseBytes,
		Note: "PG CacheMemoryContext baseline after extension load",
	})
	var relBytes, indexBytes, attrBytes, partBytes int64
	var nRels, nIndexes, nAttrs int
	for _, t := range tables {
		relBytes += PgRelCachePerRelationBytes
		indexBytes += int64(t.NIndexes) * PgRelCachePerIndexBytes
		attrBytes += int64(t.NAttributes) * PgCatCachePerAttrBytes
		nRels++
		nIndexes += t.NIndexes
		nAttrs += t.NAttributes
		if t.IsPartitionParent {
			partBytes += PgPartitionParentOverheadBytes
			partBytes += int64(t.NPartitionChildren) * PgPartitionPerChildBytes
		}
		if includeShardRels {
			rf := t.ReplicationFactor
			if rf < 1 {
				rf = 1
			}
			shardRels := int64(t.NShards) * int64(rf)
			relBytes += shardRels * PgRelCachePerRelationBytes
			indexBytes += shardRels * int64(t.NIndexes) * PgRelCachePerIndexBytes
			attrBytes += shardRels * int64(t.NAttributes) * PgCatCachePerAttrBytes
			nRels += int(shardRels)
			nIndexes += int(shardRels) * t.NIndexes
			nAttrs += int(shardRels) * t.NAttributes
		}
	}
	e.Bytes += relBytes + indexBytes + attrBytes + partBytes
	e.LowBytes = int64(float64(e.Bytes) * PgCacheEstimateLowMul)
	e.HighBytes = int64(float64(e.Bytes) * PgCacheEstimateHighMul)
	e.NRelations = nRels
	e.NIndexes = nIndexes
	e.NAttrs = nAttrs
	e.Breakdown = append(e.Breakdown,
		PgCacheBucket{Kind: "relcache", Bytes: relBytes, Note: "RelationData per relation"},
		PgCacheBucket{Kind: "index_info", Bytes: indexBytes, Note: "Per-index relcache"},
		PgCacheBucket{Kind: "catcache_attrs", Bytes: attrBytes, Note: "CatCache for pg_attribute/pg_type"},
		PgCacheBucket{Kind: "partition_descriptors", Bytes: partBytes, Note: "PartitionDesc + plancache fan-out"},
	)
	return e
}
