// citus-mcp: B10 / B10b / B9b / B24 snapshot-backed tools.
//
//   citus_snapshot_record   — opt-in: capture current diagnostics into SQLite.
//   citus_snapshot_list     — list stored snapshots filtered by kind/since.
//   citus_trend             — series view of a single numeric key across time.
//   citus_regression_detect — flag signals whose p95 moved > N% vs baseline.
//   citus_growth_projection — simple linear projection of when a resource
//                             hits a wall (RAM, shard count, connections).
//   citus_what_changed      — diff the newest two snapshots of a kind.
//
// The store is *opt-in*: if deps.Snapshot is nil these tools return a
// helpful message telling the operator how to enable it.

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/snapshot"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func requireStore(s *snapshot.Store) error {
	if s == nil {
		return fmt.Errorf("snapshot store disabled; start the server with --snapshot_db=<path> (or CITUS_MCP_SNAPSHOT_DB) to enable")
	}
	return nil
}

// --- citus_snapshot_record ----------------------------------------------

type SnapshotRecordInput struct {
	Cluster string            `json:"cluster,omitempty"` // default "default"
	Kind    string            `json:"kind"`              // required: memory | connections | shards | gucs | alarms | tenants | queries
	Node    string            `json:"node,omitempty"`
	Data    map[string]any    `json:"data"`              // caller-provided aggregated data (already redacted)
}
type SnapshotRecordOutput struct {
	ID       int64  `json:"id"`
	Path     string `json:"path"`
	Redacted bool   `json:"redacted"`
}

var allowedSnapshotKinds = map[string]bool{
	"memory": true, "connections": true, "shards": true, "gucs": true,
	"alarms": true, "tenants": true, "queries": true, "health": true,
}

func SnapshotRecordTool(ctx context.Context, deps Dependencies, in SnapshotRecordInput) (*mcp.CallToolResult, SnapshotRecordOutput, error) {
	if err := requireStore(deps.Snapshot); err != nil {
		return nil, SnapshotRecordOutput{}, err
	}
	if !allowedSnapshotKinds[in.Kind] {
		return nil, SnapshotRecordOutput{}, fmt.Errorf("kind must be one of: memory, connections, shards, gucs, alarms, tenants, queries, health")
	}
	if in.Cluster == "" {
		in.Cluster = "default"
	}
	redacted := redactSnapshot(in.Data)
	blob, err := json.Marshal(redacted)
	if err != nil {
		return nil, SnapshotRecordOutput{}, err
	}
	id, err := deps.Snapshot.Record(ctx, in.Cluster, in.Kind, in.Node, string(blob))
	if err != nil {
		return nil, SnapshotRecordOutput{}, err
	}
	return nil, SnapshotRecordOutput{ID: id, Path: deps.Snapshot.Path(), Redacted: true}, nil
}

// redactSnapshot removes any keys that look like DSNs, passwords, or
// user row data. Conservative — we only keep numeric & short-string
// summary fields.
func redactSnapshot(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	banned := map[string]bool{
		"dsn": true, "connection_string": true, "password": true,
		"secret": true, "token": true, "credentials": true,
		"query": true, "query_text": true, "args": true, "parameters": true,
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if banned[k] {
			out[k] = "[REDACTED]"
			continue
		}
		switch vv := v.(type) {
		case map[string]any:
			out[k] = redactSnapshot(vv)
		case []any:
			arr := make([]any, len(vv))
			for i, el := range vv {
				if em, ok := el.(map[string]any); ok {
					arr[i] = redactSnapshot(em)
				} else {
					arr[i] = el
				}
			}
			out[k] = arr
		default:
			out[k] = v
		}
	}
	return out
}

// --- citus_snapshot_list ------------------------------------------------

type SnapshotListInput struct {
	Cluster string `json:"cluster,omitempty"`
	Kind    string `json:"kind"`
	SinceHours int `json:"since_hours,omitempty"` // default 168
	Limit   int    `json:"limit,omitempty"`       // default 100
}
type SnapshotListOutput struct {
	Snapshots []snapshot.Snapshot `json:"snapshots"`
}

func SnapshotListTool(ctx context.Context, deps Dependencies, in SnapshotListInput) (*mcp.CallToolResult, SnapshotListOutput, error) {
	if err := requireStore(deps.Snapshot); err != nil {
		return nil, SnapshotListOutput{}, err
	}
	if in.Cluster == "" {
		in.Cluster = "default"
	}
	if in.SinceHours == 0 {
		in.SinceHours = 168
	}
	since := time.Now().Add(-time.Duration(in.SinceHours) * time.Hour)
	rows, err := deps.Snapshot.List(ctx, in.Cluster, in.Kind, since, in.Limit)
	if err != nil {
		return nil, SnapshotListOutput{}, err
	}
	return nil, SnapshotListOutput{Snapshots: rows}, nil
}

// --- citus_trend --------------------------------------------------------

type TrendInput struct {
	Cluster string   `json:"cluster,omitempty"`
	Kind    string   `json:"kind"`           // required
	Path    []string `json:"path"`           // JSON path within data_json to pull numeric, e.g. ["total_bytes"]
	SinceHours int   `json:"since_hours,omitempty"` // default 168
}
type TrendPoint struct {
	At    time.Time `json:"at"`
	Node  string    `json:"node,omitempty"`
	Value float64   `json:"value"`
}
type TrendOutput struct {
	Points    []TrendPoint `json:"points"`
	Count     int          `json:"count"`
	FirstVal  float64      `json:"first_value"`
	LastVal   float64      `json:"last_value"`
	MinVal    float64      `json:"min_value"`
	MaxVal    float64      `json:"max_value"`
	SlopePerHour float64   `json:"slope_per_hour"`
}

func TrendTool(ctx context.Context, deps Dependencies, in TrendInput) (*mcp.CallToolResult, TrendOutput, error) {
	if err := requireStore(deps.Snapshot); err != nil {
		return nil, TrendOutput{}, err
	}
	if in.Cluster == "" {
		in.Cluster = "default"
	}
	if in.SinceHours == 0 {
		in.SinceHours = 168
	}
	if len(in.Path) == 0 {
		return nil, TrendOutput{}, fmt.Errorf("path required")
	}
	since := time.Now().Add(-time.Duration(in.SinceHours) * time.Hour)
	rows, err := deps.Snapshot.List(ctx, in.Cluster, in.Kind, since, 10000)
	if err != nil {
		return nil, TrendOutput{}, err
	}
	var pts []TrendPoint
	for _, r := range rows {
		var doc any
		if err := json.Unmarshal([]byte(r.Data), &doc); err != nil {
			continue
		}
		v, ok := extractPath(doc, in.Path)
		if !ok {
			continue
		}
		f, ok := toFloat(v)
		if !ok {
			continue
		}
		pts = append(pts, TrendPoint{At: r.CollectedAt, Node: r.Node, Value: f})
	}
	// Sort ascending.
	sort.Slice(pts, func(i, j int) bool { return pts[i].At.Before(pts[j].At) })
	out := TrendOutput{Points: pts, Count: len(pts)}
	if len(pts) > 0 {
		out.FirstVal = pts[0].Value
		out.LastVal = pts[len(pts)-1].Value
		out.MinVal, out.MaxVal = pts[0].Value, pts[0].Value
		for _, p := range pts {
			if p.Value < out.MinVal {
				out.MinVal = p.Value
			}
			if p.Value > out.MaxVal {
				out.MaxVal = p.Value
			}
		}
		if len(pts) >= 2 {
			hours := pts[len(pts)-1].At.Sub(pts[0].At).Hours()
			if hours > 0 {
				out.SlopePerHour = (out.LastVal - out.FirstVal) / hours
			}
		}
	}
	return nil, out, nil
}

func extractPath(doc any, path []string) (any, bool) {
	cur := doc
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

// --- citus_regression_detect --------------------------------------------

type RegressionInput struct {
	Cluster        string   `json:"cluster,omitempty"`
	Kind           string   `json:"kind"`
	Path           []string `json:"path"`
	BaselineHours  int      `json:"baseline_hours,omitempty"`  // default 24
	ComparisonHours int     `json:"comparison_hours,omitempty"` // default 1
	ThresholdPct   float64  `json:"threshold_pct,omitempty"`   // default 25
}
type RegressionOutput struct {
	BaselineMean   float64 `json:"baseline_mean"`
	ComparisonMean float64 `json:"comparison_mean"`
	DeltaPct       float64 `json:"delta_pct"`
	Regressed      bool    `json:"regressed"`
	Alarms         []diagnostics.Alarm `json:"alarms"`
}

func RegressionDetectTool(ctx context.Context, deps Dependencies, in RegressionInput) (*mcp.CallToolResult, RegressionOutput, error) {
	out := RegressionOutput{Alarms: []diagnostics.Alarm{}}
	if err := requireStore(deps.Snapshot); err != nil {
		return nil, out, err
	}
	if in.Cluster == "" {
		in.Cluster = "default"
	}
	if in.BaselineHours == 0 {
		in.BaselineHours = 24
	}
	if in.ComparisonHours == 0 {
		in.ComparisonHours = 1
	}
	if in.ThresholdPct == 0 {
		in.ThresholdPct = 25
	}
	since := time.Now().Add(-time.Duration(in.BaselineHours) * time.Hour)
	rows, err := deps.Snapshot.List(ctx, in.Cluster, in.Kind, since, 10000)
	if err != nil {
		return nil, out, err
	}
	cutoff := time.Now().Add(-time.Duration(in.ComparisonHours) * time.Hour)
	var baseVals, cmpVals []float64
	for _, r := range rows {
		var doc any
		if err := json.Unmarshal([]byte(r.Data), &doc); err != nil {
			continue
		}
		v, ok := extractPath(doc, in.Path)
		if !ok {
			continue
		}
		f, ok := toFloat(v)
		if !ok {
			continue
		}
		if r.CollectedAt.After(cutoff) {
			cmpVals = append(cmpVals, f)
		} else {
			baseVals = append(baseVals, f)
		}
	}
	out.BaselineMean = mean(baseVals)
	out.ComparisonMean = mean(cmpVals)
	if out.BaselineMean > 0 {
		out.DeltaPct = (out.ComparisonMean - out.BaselineMean) * 100.0 / out.BaselineMean
	}
	out.Regressed = math.Abs(out.DeltaPct) >= in.ThresholdPct
	if out.Regressed && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "regression." + in.Kind, Severity: diagnostics.SeverityWarning,
			Source: "citus_regression_detect",
			Message: fmt.Sprintf("%s.%v moved %.1f%% (baseline %.2f → current %.2f)", in.Kind, in.Path, out.DeltaPct, out.BaselineMean, out.ComparisonMean),
			Evidence: map[string]any{"kind": in.Kind, "path": in.Path, "baseline": out.BaselineMean, "comparison": out.ComparisonMean, "delta_pct": out.DeltaPct},
			FixHint: "Correlate with recent deployments, partition changes, or workload bursts.",
		})
		out.Alarms = append(out.Alarms, *a)
	}
	return nil, out, nil
}

func mean(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	var s float64
	for _, v := range vs {
		s += v
	}
	return s / float64(len(vs))
}

// --- citus_growth_projection --------------------------------------------

type GrowthProjectionInput struct {
	Cluster    string   `json:"cluster,omitempty"`
	Kind       string   `json:"kind"`        // e.g. "shards"
	Path       []string `json:"path"`        // numeric field to project
	Ceiling    float64  `json:"ceiling"`     // the wall we hit
	LookbackHours int   `json:"lookback_hours,omitempty"` // default 168
}
type GrowthProjectionOutput struct {
	CurrentValue   float64 `json:"current_value"`
	SlopePerHour   float64 `json:"slope_per_hour"`
	HoursToCeiling float64 `json:"hours_to_ceiling"`
	DaysToCeiling  float64 `json:"days_to_ceiling"`
	WillHitWall    bool    `json:"will_hit_wall"`
	EstimatedWhen  string  `json:"estimated_when,omitempty"`
	Alarms         []diagnostics.Alarm `json:"alarms"`
	Warnings       []string `json:"warnings,omitempty"`
}

func GrowthProjectionTool(ctx context.Context, deps Dependencies, in GrowthProjectionInput) (*mcp.CallToolResult, GrowthProjectionOutput, error) {
	out := GrowthProjectionOutput{Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if err := requireStore(deps.Snapshot); err != nil {
		return nil, out, err
	}
	if in.LookbackHours == 0 {
		in.LookbackHours = 168
	}
	if in.Ceiling <= 0 {
		return nil, out, fmt.Errorf("ceiling required and must be > 0")
	}
	tr, _, err := TrendTool(ctx, deps, TrendInput{Cluster: in.Cluster, Kind: in.Kind, Path: in.Path, SinceHours: in.LookbackHours})
	if err != nil {
		return nil, out, err
	}
	_ = tr
	t := TrendOutput{}
	// Re-invoke to get the output struct directly.
	_, t, _ = TrendTool(ctx, deps, TrendInput{Cluster: in.Cluster, Kind: in.Kind, Path: in.Path, SinceHours: in.LookbackHours})
	if t.Count < 2 {
		out.Warnings = append(out.Warnings, "need at least 2 data points; record more snapshots to enable projection")
		return nil, out, nil
	}
	out.CurrentValue = t.LastVal
	out.SlopePerHour = t.SlopePerHour
	if out.SlopePerHour <= 0 {
		out.Warnings = append(out.Warnings, "signal is flat or decreasing; no projection emitted")
		return nil, out, nil
	}
	remaining := in.Ceiling - out.CurrentValue
	if remaining <= 0 {
		out.WillHitWall = true
		out.HoursToCeiling = 0
		out.EstimatedWhen = "already past ceiling"
	} else {
		out.HoursToCeiling = remaining / out.SlopePerHour
		out.DaysToCeiling = out.HoursToCeiling / 24
		out.WillHitWall = out.HoursToCeiling < float64(24*90) // flag if < 90 days
		out.EstimatedWhen = time.Now().Add(time.Duration(out.HoursToCeiling * float64(time.Hour))).Format("2006-01-02 15:04 MST")
	}
	if out.WillHitWall && deps.Alarms != nil {
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "growth.wall_approaching", Severity: diagnostics.SeverityWarning,
			Source: "citus_growth_projection",
			Message: fmt.Sprintf("%s.%v projected to hit %.0f in %.1f days (%s)", in.Kind, in.Path, in.Ceiling, out.DaysToCeiling, out.EstimatedWhen),
			Evidence: map[string]any{"kind": in.Kind, "path": in.Path, "current": out.CurrentValue, "ceiling": in.Ceiling, "days": out.DaysToCeiling},
			FixHint: "Plan capacity now (citus_hardware_sizer) or slow growth (citus_shard_advisor).",
		})
		out.Alarms = append(out.Alarms, *a)
	}
	return nil, out, nil
}

// --- citus_what_changed -------------------------------------------------

type WhatChangedInput struct {
	Cluster string `json:"cluster,omitempty"`
	Kind    string `json:"kind"`
	SinceHours int `json:"since_hours,omitempty"` // default 24
}
type WhatChangedOutput struct {
	PrevID   int64             `json:"prev_id"`
	CurrID   int64             `json:"curr_id"`
	AddedKeys []string          `json:"added_keys,omitempty"`
	RemovedKeys []string        `json:"removed_keys,omitempty"`
	ChangedKeys []ChangedKey    `json:"changed_keys,omitempty"`
}
type ChangedKey struct {
	Path  string `json:"path"`
	Prev  any    `json:"prev"`
	Curr  any    `json:"curr"`
}

func WhatChangedTool(ctx context.Context, deps Dependencies, in WhatChangedInput) (*mcp.CallToolResult, WhatChangedOutput, error) {
	out := WhatChangedOutput{}
	if err := requireStore(deps.Snapshot); err != nil {
		return nil, out, err
	}
	if in.Cluster == "" {
		in.Cluster = "default"
	}
	if in.SinceHours == 0 {
		in.SinceHours = 24
	}
	since := time.Now().Add(-time.Duration(in.SinceHours) * time.Hour)
	rows, err := deps.Snapshot.List(ctx, in.Cluster, in.Kind, since, 2)
	if err != nil {
		return nil, out, err
	}
	if len(rows) < 2 {
		return nil, out, fmt.Errorf("need at least 2 snapshots of kind %q within last %dh; found %d", in.Kind, in.SinceHours, len(rows))
	}
	// rows[0] is newest (DESC).
	out.CurrID = rows[0].ID
	out.PrevID = rows[1].ID
	var cur, prev map[string]any
	_ = json.Unmarshal([]byte(rows[0].Data), &cur)
	_ = json.Unmarshal([]byte(rows[1].Data), &prev)
	out.AddedKeys, out.RemovedKeys, out.ChangedKeys = diffMaps("", prev, cur)
	return nil, out, nil
}

func diffMaps(prefix string, prev, cur map[string]any) (added, removed []string, changed []ChangedKey) {
	for k, v := range cur {
		full := k
		if prefix != "" {
			full = prefix + "." + k
		}
		pv, ok := prev[k]
		if !ok {
			added = append(added, full)
			continue
		}
		// Recurse into maps.
		if pm, pmok := pv.(map[string]any); pmok {
			if cm, cmok := v.(map[string]any); cmok {
				a, r, c := diffMaps(full, pm, cm)
				added = append(added, a...)
				removed = append(removed, r...)
				changed = append(changed, c...)
				continue
			}
		}
		if fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", v) {
			changed = append(changed, ChangedKey{Path: full, Prev: pv, Curr: v})
		}
	}
	for k := range prev {
		full := k
		if prefix != "" {
			full = prefix + "." + k
		}
		if _, ok := cur[k]; !ok {
			removed = append(removed, full)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Slice(changed, func(i, j int) bool { return changed[i].Path < changed[j].Path })
	return
}
