// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_shard_skew_report tool for data distribution analysis.

package tools

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"citus-mcp/internal/db"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

// ShardSkewInput for citus.shard_skew_report.
type ShardSkewInput struct {
	Table            string `json:"table,omitempty"`  // schema.name
	Metric           string `json:"metric,omitempty"` // shard_count | bytes
	IncludeTopShards bool   `json:"include_top_shards,omitempty"`
}

// ShardSkewOutput result.
type ShardSkewOutput struct {
	PerNode       []NodeSkewSummary       `json:"per_node"`
	Skew          SkewScore               `json:"skew"`
	PerColocation []ColocationSkewSummary `json:"per_colocation,omitempty"`
	HotShards     []HotShard              `json:"hot_shards,omitempty"`
	TopShards     []TopShard              `json:"top_shards,omitempty"`
	// DataHoldingNodeCount is the number of pg_dist_node rows with
	// shouldhaveshards=true AND isactive=true AND noderole='primary'.
	// Cluster-wide skew is only meaningful when this is >= 2.
	DataHoldingNodeCount int      `json:"data_holding_node_count"`
	Warnings             []string `json:"warnings,omitempty"`
}

type NodeSkewSummary struct {
	Host             string  `json:"host"`
	Port             int32   `json:"port"`
	ShardCount       int64   `json:"shard_count"`
	BytesTotal       int64   `json:"bytes_total"`
	BytesAvgPerShard float64 `json:"bytes_avg_per_shard"`
	// ShouldHaveShards mirrors pg_dist_node.shouldhaveshards. This is the
	// authoritative signal the Citus rebalancer itself uses to decide which
	// nodes are "data-holding" (see src/backend/distributed/operations/
	// shard_rebalancer.c:567 and :651). Only shouldhaveshards=true nodes are
	// part of the cluster-wide skew metric; coordinators and drained nodes
	// are reported per-node but excluded from the cluster aggregate.
	ShouldHaveShards bool `json:"should_have_shards"`
	// NodeRole mirrors pg_dist_node.noderole ('primary' | 'secondary').
	NodeRole string `json:"node_role,omitempty"`
	// OnlyReferenceTables is true when every placement on this node belongs
	// to a reference table. Kept as a secondary signal for UI / explanation
	// (typical for coordinators with should_have_shards=false that still
	// carry ref-table copies).
	OnlyReferenceTables bool `json:"only_reference_tables"`
}

// ColocationSkewSummary reports bytes skew within a single colocation group.
// This is the signal that matters most for hot-tenant detection: uneven hash
// distribution within a colocation group caused by a skewed distribution key
// manifests as max/avg >> 1 on THIS table, not at the node level.
type ColocationSkewSummary struct {
	ColocationID  int64    `json:"colocation_id"`
	Tables        []string `json:"tables"`
	Shards        int      `json:"shards"`
	BytesTotal    int64    `json:"bytes_total"`
	BytesMax      int64    `json:"bytes_max"`
	BytesAvg      float64  `json:"bytes_avg"`
	MaxOverAvg    float64  `json:"max_over_avg"`
	Verdict       string   `json:"verdict"` // ok | warn | critical
	HotShardID    int64    `json:"hot_shard_id,omitempty"`
}

// HotShard is a single shard whose size is a large multiple of its peers
// within the same table. Typically indicates a "hot tenant" — i.e. the
// distribution key concentrates traffic on one value. Remediation is
// isolate_tenant_to_new_shard(...).
type HotShard struct {
	TableName   string  `json:"table_name"`
	ShardID     int64   `json:"shard_id"`
	Bytes       int64   `json:"bytes"`
	TableAvg    float64 `json:"table_avg_bytes"`
	Ratio       float64 `json:"max_over_avg"`
	Remediation string  `json:"remediation"`
}

type SkewScore struct {
	Metric  string  `json:"metric"`
	Max     float64 `json:"max"`
	Min     float64 `json:"min"`
	Stddev  float64 `json:"stddev"`
	Warning string  `json:"warning_level"`
}

type TopShard struct {
	ShardID    int64            `json:"shard_id"`
	Bytes      int64            `json:"bytes"`
	Placements []ShardPlacement `json:"placements"`
}

type ShardPlacement struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
}

func shardSkewReportTool(ctx context.Context, deps Dependencies, input ShardSkewInput) (*mcp.CallToolResult, ShardSkewOutput, error) {
	metric := input.Metric
	if metric == "" {
		metric = "bytes"
	}
	metricBytes := metric == "bytes"
	// ensure read-only
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ShardSkewOutput{PerNode: []NodeSkewSummary{}}, nil
	}

	// validate citus
	ext, err := db.GetExtensionInfo(ctx, deps.Pool)
	if err != nil || ext == nil {
		return callError(serr.CodeNotCitus, "citus extension not found", "enable citus extension"), ShardSkewOutput{PerNode: []NodeSkewSummary{}}, nil
	}

	var schema, table string
	if input.Table != "" {
		schema, table, err = parseSchemaTable(input.Table)
		if err != nil {
			return callError(serr.CodeInvalidInput, "invalid table format", "use schema.table"), ShardSkewOutput{PerNode: []NodeSkewSummary{}}, nil
		}
	}

	// fetch worker topology
	infos, err := deps.WorkerManager.Topology(ctx)
	warnings := []string{}
	if err != nil {
		deps.Logger.Warn("topology fetch failed", zap.Error(err))
		warnings = append(warnings, "failed to fetch worker topology")
	}
	if len(infos) == 0 {
		warnings = append(warnings, "no workers")
	}

	// detect bytes capability via pg_catalog.citus_shards view first, fallback to citus_shard_sizes()
	hasBytes := false
	var useCitusShardsView bool
	if metricBytes {
		// check view existence
		var ok bool
		if err := deps.Pool.QueryRow(ctx, "SELECT to_regclass('pg_catalog.citus_shards') IS NOT NULL").Scan(&ok); err == nil && ok {
			hasBytes = true
			useCitusShardsView = true
		} else {
			if err := deps.Pool.QueryRow(ctx, "SELECT to_regproc('citus_shard_sizes') IS NOT NULL").Scan(&ok); err == nil && ok {
				hasBytes = true
			}
		}
	}

	// fetch shards and placements (with table metadata)
	shards, err := fetchShardPlacements(ctx, deps, schema, table)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "fetch shard placements"), ShardSkewOutput{PerNode: []NodeSkewSummary{}}, nil
	}
	if len(shards) == 0 {
		warnings = append(warnings, "no shards found")
	}

	shardBytes := map[int64]int64{}
	if hasBytes {
		var sz map[int64]int64
		var err error
		if useCitusShardsView {
			sz, err = fetchShardSizesFromView(ctx, deps, schema, table)
			if err != nil {
				// fallback to function if view failed
				deps.Logger.Warn("citus_shards view failed, falling back to citus_shard_sizes()", zap.Error(err))
				sz, err = fetchShardSizes(ctx, deps)
			}
		} else {
			sz, err = fetchShardSizes(ctx, deps)
		}
		if err != nil {
			warnings = append(warnings, "failed to fetch shard sizes; falling back to shard_count")
		} else {
			shardBytes = sz
		}
	}
	if metricBytes && !hasBytes {
		warnings = append(warnings, "citus_shard_sizes not available; using shard_count")
		metricBytes = false
		metric = "shard_count"
	}

	// Fetch pg_dist_node to know which nodes are "data-holding" from Citus's
	// own perspective. The rebalancer source uses shouldhaveshards as the
	// authoritative filter (src/backend/distributed/operations/
	// shard_rebalancer.c:567,651) so we adopt it here to avoid the
	// coord-masquerading-as-a-skewed-worker false positive.
	nodeMeta, err := fetchDistNodeMeta(ctx, deps)
	if err != nil {
		deps.Logger.Warn("pg_dist_node fetch failed", zap.Error(err))
		warnings = append(warnings, "pg_dist_node fetch failed; falling back to placement-derived node set")
		nodeMeta = map[string]distNodeMeta{}
	}

	// Per-node: count shards and bytes; remember whether a node holds ONLY
	// reference-table placements (secondary signal for UI).
	type nodeAcc struct {
		*NodeSkewSummary
		sawDistributed bool
	}
	perNode := map[string]*nodeAcc{}
	// Seed with every pg_dist_node row so we surface data-holding workers
	// that happen to have zero placements (e.g. brand-new node waiting for
	// rebalance). Without this such a node would be invisible in the report.
	for key, m := range nodeMeta {
		perNode[key] = &nodeAcc{NodeSkewSummary: &NodeSkewSummary{
			Host:                m.Host,
			Port:                m.Port,
			ShouldHaveShards:    m.ShouldHaveShards,
			NodeRole:            m.NodeRole,
			OnlyReferenceTables: true,
		}}
		_ = key
	}
	for _, shp := range shards {
		key := shp.Host + ":" + strconv.Itoa(int(shp.Port))
		ns, ok := perNode[key]
		if !ok {
			// Placement exists on a node that is not in pg_dist_node or is
			// not primary. Preserve it in per_node output but default
			// shouldhaveshards=false so it does NOT poison the cluster metric.
			ns = &nodeAcc{NodeSkewSummary: &NodeSkewSummary{
				Host:                shp.Host,
				Port:                shp.Port,
				OnlyReferenceTables: true,
			}}
			perNode[key] = ns
		}
		ns.ShardCount++
		if metricBytes && shardBytes[shp.ShardID] > 0 {
			ns.BytesTotal += shardBytes[shp.ShardID]
		}
		if !shp.IsReference {
			ns.sawDistributed = true
			ns.OnlyReferenceTables = false
		}
	}

	nodeSummaries := make([]NodeSkewSummary, 0, len(perNode))
	metricVals := []float64{}
	dataHolding := 0
	for _, ns := range perNode {
		if ns.ShardCount > 0 {
			ns.BytesAvgPerShard = float64(ns.BytesTotal) / float64(ns.ShardCount)
		}
		nodeSummaries = append(nodeSummaries, *ns.NodeSkewSummary)
		// Cluster-wide skew is defined only across data-holding primary
		// nodes (shouldhaveshards=true). Drained nodes, coordinators with
		// should_have_shards=false, and secondaries are excluded.
		if !ns.ShouldHaveShards {
			continue
		}
		dataHolding++
		if metricBytes {
			metricVals = append(metricVals, float64(ns.BytesTotal))
		} else {
			metricVals = append(metricVals, float64(ns.ShardCount))
		}
	}
	sort.Slice(nodeSummaries, func(i, j int) bool {
		if nodeSummaries[i].Host == nodeSummaries[j].Host {
			return nodeSummaries[i].Port < nodeSummaries[j].Port
		}
		return nodeSummaries[i].Host < nodeSummaries[j].Host
	})

	// When only 0 or 1 data-holding nodes exist the cluster-wide skew
	// metric is structurally meaningless (max==min, stddev==0 always).
	// Surface this explicitly instead of reporting a bogus "low skew" ok.
	skew, degenWarn := clusterSkewDecision(metricVals, metric, dataHolding)
	if degenWarn != "" {
		warnings = append(warnings, degenWarn)
	}

	// Per-colocation skew: the primary signal for a skewed distribution key.
	// We compute max/avg bytes within each colocation group, flag CRITICAL
	// at ratio >= 5.0, WARN at >= 2.0.
	var perColocation []ColocationSkewSummary
	var hotShards []HotShard
	if metricBytes && len(shardBytes) > 0 && len(shards) > 0 {
		perColocation = buildColocationSkew(shards, shardBytes)
		hotShards = buildHotShards(shards, shardBytes, 10)
	}

	var topShards []TopShard
	if input.IncludeTopShards && len(shardBytes) > 0 {
		topShards = buildTopShards(shardBytes, shards, 10)
	}

	// ensure non-nil slices for JSON (avoid null)
	if nodeSummaries == nil {
		nodeSummaries = []NodeSkewSummary{}
	}
	if topShards == nil && input.IncludeTopShards {
		topShards = []TopShard{}
	}
	if warnings == nil {
		warnings = []string{}
	}
	return nil, ShardSkewOutput{
		PerNode:              nodeSummaries,
		Skew:                 skew,
		PerColocation:        perColocation,
		HotShards:            hotShards,
		TopShards:            topShards,
		DataHoldingNodeCount: dataHolding,
		Warnings:             warnings,
	}, nil
}

// distNodeMeta is the subset of pg_dist_node we need for skew decisions.
type distNodeMeta struct {
	Host             string
	Port             int32
	NodeRole         string
	ShouldHaveShards bool
	IsActive         bool
}

// clusterSkewDecision centralizes the "is the cluster-wide skew metric even
// meaningful?" rule: if 0 or 1 pg_dist_node rows have shouldhaveshards=true,
// max==min by definition and a "low skew / ok" verdict would be misleading.
// In those cases we return a single_data_node SkewScore plus a warning
// pointing the operator at per_colocation instead. Otherwise we compute the
// normal metric.
func clusterSkewDecision(metricVals []float64, metric string, dataHoldingCount int) (SkewScore, string) {
	if dataHoldingCount < 2 {
		warn := "only one data-holding node; cluster-wide skew metric is N/A. Use per_colocation to detect key-level skew."
		if dataHoldingCount == 0 {
			warn = "no node has pg_dist_node.shouldhaveshards=true; cluster-wide skew metric is N/A. Use per_colocation instead."
		}
		return SkewScore{Metric: metric, Warning: "single_data_node"}, warn
	}
	return computeSkew(metricVals, metric), ""
}

// fetchDistNodeMeta returns one entry per primary active node keyed by
// "host:port". Citus's own rebalancer uses shouldhaveshards as the filter for
// shard-holding nodes (shard_rebalancer.c:567,651), so we adopt the same
// signal here.
func fetchDistNodeMeta(ctx context.Context, deps Dependencies) (map[string]distNodeMeta, error) {
	q := `SELECT nodename, nodeport, noderole::text, shouldhaveshards, isactive
	      FROM pg_catalog.pg_dist_node
	      WHERE isactive = true AND noderole = 'primary'`
	rows, err := deps.Pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]distNodeMeta{}
	for rows.Next() {
		var m distNodeMeta
		if err := rows.Scan(&m.Host, &m.Port, &m.NodeRole, &m.ShouldHaveShards, &m.IsActive); err != nil {
			return nil, err
		}
		out[m.Host+":"+strconv.Itoa(int(m.Port))] = m
	}
	return out, rows.Err()
}

// ShardSkewReport is exported for resources.
func ShardSkewReport(ctx context.Context, deps Dependencies, input ShardSkewInput) (*mcp.CallToolResult, ShardSkewOutput, error) {
	return shardSkewReportTool(ctx, deps, input)
}

type shardPlacementRow struct {
	ShardID      int64
	Host         string
	Port         int32
	TableName    string
	ColocationID int64
	IsReference  bool
}

func fetchShardPlacements(ctx context.Context, deps Dependencies, schema, table string) ([]shardPlacementRow, error) {
	// Join pg_dist_partition to pull colocation + partmethod for each shard.
	// partmethod = 'n' means reference table (see citus_tables.c).
	q := `
SELECT s.shardid,
       p.nodename,
       p.nodeport,
       ns.nspname || '.' || c.relname AS table_name,
       COALESCE(dp.colocationid, 0)  AS colocationid,
       COALESCE(dp.partmethod::text, '')   AS partmethod
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p USING (shardid)
JOIN pg_class c ON c.oid = s.logicalrelid
JOIN pg_namespace ns ON ns.oid = c.relnamespace
LEFT JOIN pg_dist_partition dp ON dp.logicalrelid = s.logicalrelid
WHERE ($1 = '' OR (ns.nspname = $1::name AND c.relname = $2::name))
`
	rows, err := deps.Pool.Query(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []shardPlacementRow
	for rows.Next() {
		var r shardPlacementRow
		var partmethod string
		if err := rows.Scan(&r.ShardID, &r.Host, &r.Port, &r.TableName, &r.ColocationID, &partmethod); err != nil {
			return nil, err
		}
		r.IsReference = (partmethod == "n")
		out = append(out, r)
	}
	return out, rows.Err()
}

func fetchShardSizes(ctx context.Context, deps Dependencies) (map[int64]int64, error) {
	q := `SELECT shard_id, size FROM citus_shard_sizes()`
	rows, err := deps.Pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sizes := map[int64]int64{}
	for rows.Next() {
		var shardid int64
		var size int64
		if err := rows.Scan(&shardid, &size); err != nil {
			return nil, err
		}
		sizes[shardid] = size
	}
	return sizes, rows.Err()
}

func fetchShardSizesFromView(ctx context.Context, deps Dependencies, schema, table string) (map[int64]int64, error) {
	q := `SELECT shardid, shard_size FROM pg_catalog.citus_shards WHERE ($1='' OR table_name = $1||'.'||$2)`
	rows, err := deps.Pool.Query(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sizes := map[int64]int64{}
	for rows.Next() {
		var shardid int64
		var size int64
		if err := rows.Scan(&shardid, &size); err != nil {
			return nil, err
		}
		sizes[shardid] = size
	}
	return sizes, rows.Err()
}

func computeSkew(vals []float64, metric string) SkewScore {
	if len(vals) == 0 {
		return SkewScore{Metric: metric}
	}
	maxv, minv := vals[0], vals[0]
	var sum float64
	for _, v := range vals {
		if v > maxv {
			maxv = v
		}
		if v < minv {
			minv = v
		}
		sum += v
	}
	mean := sum / float64(len(vals))
	var variance float64
	for _, v := range vals {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(len(vals))
	stddev := math.Sqrt(variance)

	warning := "low"
	ratio := 1.0
	if minv > 0 {
		ratio = maxv / minv
	} else if maxv > 0 {
		ratio = math.Inf(1)
	}
	if ratio >= 2.0 || stddev > mean {
		warning = "high"
	} else if ratio >= 1.5 || stddev > mean*0.5 {
		warning = "medium"
	}

	return SkewScore{Metric: metric, Max: maxv, Min: minv, Stddev: stddev, Warning: warning}
}

func buildTopShards(shardBytes map[int64]int64, placements []shardPlacementRow, limit int) []TopShard {
	type pair struct {
		id int64
		b  int64
	}
	pairs := make([]pair, 0, len(shardBytes))
	for id, b := range shardBytes {
		pairs = append(pairs, pair{id: id, b: b})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].b > pairs[j].b })
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}
	// map placements
	placeByShard := map[int64][]ShardPlacement{}
	for _, p := range placements {
		placeByShard[p.ShardID] = append(placeByShard[p.ShardID], ShardPlacement{Host: p.Host, Port: p.Port})
	}

	out := make([]TopShard, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, TopShard{ShardID: p.id, Bytes: p.b, Placements: placeByShard[p.id]})
	}
	return out
}

// buildColocationSkew computes max/avg shard-bytes within each colocation
// group. A ratio >> 1 is the primary signal of a skewed distribution key:
// hash-distributed shards should be roughly equal within a colocation group,
// so one shard being much larger than its peers usually means one hash value
// (one tenant) absorbs a disproportionate share of traffic. Reference tables
// (colocation_id=0 on our unified query path, or partmethod='n') are skipped
// because they have only 1 shard per relation.
func buildColocationSkew(shards []shardPlacementRow, shardBytes map[int64]int64) []ColocationSkewSummary {
	type agg struct {
		tables map[string]struct{}
		bytes  []int64
		total  int64
		max    int64
		maxSh  int64
	}
	byColo := map[int64]*agg{}
	seen := map[int64]struct{}{}
	for _, shp := range shards {
		if shp.IsReference || shp.ColocationID == 0 {
			continue
		}
		// Avoid double-counting replicas: only consider each shardid once.
		if _, ok := seen[shp.ShardID]; ok {
			continue
		}
		seen[shp.ShardID] = struct{}{}
		a, ok := byColo[shp.ColocationID]
		if !ok {
			a = &agg{tables: map[string]struct{}{}}
			byColo[shp.ColocationID] = a
		}
		a.tables[shp.TableName] = struct{}{}
		b := shardBytes[shp.ShardID]
		a.bytes = append(a.bytes, b)
		a.total += b
		if b > a.max {
			a.max = b
			a.maxSh = shp.ShardID
		}
	}
	out := make([]ColocationSkewSummary, 0, len(byColo))
	for colo, a := range byColo {
		if len(a.bytes) == 0 {
			continue
		}
		avg := float64(a.total) / float64(len(a.bytes))
		ratio := 0.0
		if avg > 0 {
			ratio = float64(a.max) / avg
		}
		verdict := "ok"
		switch {
		case ratio >= 5.0:
			verdict = "critical"
		case ratio >= 2.0:
			verdict = "warn"
		}
		tables := make([]string, 0, len(a.tables))
		for t := range a.tables {
			tables = append(tables, t)
		}
		sort.Strings(tables)
		s := ColocationSkewSummary{
			ColocationID: colo, Tables: tables,
			Shards: len(a.bytes), BytesTotal: a.total,
			BytesMax: a.max, BytesAvg: avg, MaxOverAvg: ratio,
			Verdict: verdict,
		}
		if verdict != "ok" {
			s.HotShardID = a.maxSh
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MaxOverAvg > out[j].MaxOverAvg })
	return out
}

// buildHotShards surfaces per-table hot shards: shards whose bytes are
// >= 2× the table's average. Emits a ready-to-run isolate_tenant_to_new_shard
// suggestion. Reference tables are skipped.
func buildHotShards(shards []shardPlacementRow, shardBytes map[int64]int64, limit int) []HotShard {
	type perTable struct {
		name   string
		shards []int64
		total  int64
	}
	byTable := map[string]*perTable{}
	seen := map[int64]struct{}{}
	for _, shp := range shards {
		if shp.IsReference {
			continue
		}
		if _, ok := seen[shp.ShardID]; ok {
			continue
		}
		seen[shp.ShardID] = struct{}{}
		pt, ok := byTable[shp.TableName]
		if !ok {
			pt = &perTable{name: shp.TableName}
			byTable[shp.TableName] = pt
		}
		b := shardBytes[shp.ShardID]
		pt.shards = append(pt.shards, shp.ShardID)
		pt.total += b
	}
	var out []HotShard
	for _, pt := range byTable {
		if len(pt.shards) < 2 {
			continue // single-shard tables can't be "skewed"
		}
		avg := float64(pt.total) / float64(len(pt.shards))
		if avg == 0 {
			continue
		}
		for _, sh := range pt.shards {
			b := shardBytes[sh]
			ratio := float64(b) / avg
			if ratio < 2.0 {
				continue
			}
			out = append(out, HotShard{
				TableName: pt.name, ShardID: sh, Bytes: b,
				TableAvg: avg, Ratio: ratio,
				Remediation: fmt.Sprintf(
					"SELECT isolate_tenant_to_new_shard('%s', <tenant_value>, 'CASCADE');",
					pt.name),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ratio > out[j].Ratio })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func parseSchemaTable(s string) (string, string, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid table: %s", s)
	}
	return parts[0], parts[1], nil
}
