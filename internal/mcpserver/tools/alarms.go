// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// MCP tools backing the alarms framework (B11):
//   - citus_alarms_list  : query open / acked alarms with filtering
//   - citus_alarms_ack   : acknowledge an alarm by id (or all matching a filter)
//   - citus_alarms_clear : drop all alarms (read-only sink reset; no DB effect)
//
// All diagnostic tools emit findings through deps.Alarms; this file is the
// user-facing surface over that sink.

package tools

import (
	"context"
	"time"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// AlarmsListInput controls filtering of the alarms list.
type AlarmsListInput struct {
	MinSeverity  string `json:"min_severity,omitempty"` // info|warning|critical
	Kind         string `json:"kind,omitempty"`
	Source       string `json:"source,omitempty"`
	Node         string `json:"node,omitempty"`
	Object       string `json:"object,omitempty"`
	SinceSecs    int    `json:"since_secs,omitempty"` // only alarms updated within the last N seconds
	IncludeAcked bool   `json:"include_acked,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

// AlarmsListOutput is the list + summary.
type AlarmsListOutput struct {
	Stats  map[string]int      `json:"stats"`
	Alarms []diagnostics.Alarm `json:"alarms"`
}

// AlarmsList returns currently-known alarms. Read-only.
func AlarmsList(ctx context.Context, deps Dependencies, in AlarmsListInput) (*mcp.CallToolResult, AlarmsListOutput, error) {
	f := diagnostics.Filter{
		MinSeverity:  diagnostics.Severity(in.MinSeverity),
		Kind:         in.Kind,
		Source:       in.Source,
		Node:         in.Node,
		Object:       in.Object,
		IncludeAcked: in.IncludeAcked,
		Limit:        in.Limit,
	}
	if in.SinceSecs > 0 {
		t := time.Now().Add(-time.Duration(in.SinceSecs) * time.Second)
		f.Since = &t
	}
	out := AlarmsListOutput{
		Stats:  deps.Alarms.Stats(),
		Alarms: deps.Alarms.List(f),
	}
	if out.Alarms == nil {
		out.Alarms = []diagnostics.Alarm{}
	}
	return nil, out, nil
}

// AlarmsAckInput specifies which alarm(s) to acknowledge.
type AlarmsAckInput struct {
	ID          string `json:"id,omitempty"`           // exact alarm id
	MatchFilter bool   `json:"match_filter,omitempty"` // if true, ack all matching the filter fields
	MinSeverity string `json:"min_severity,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Source      string `json:"source,omitempty"`
	Node        string `json:"node,omitempty"`
	Object      string `json:"object,omitempty"`
	By          string `json:"by,omitempty"` // free-form label (user / agent id)
}

// AlarmsAckOutput reports the result.
type AlarmsAckOutput struct {
	Acked   int    `json:"acked"`
	Message string `json:"message"`
}

// AlarmsAck marks alarms as acknowledged. Read-only w.r.t. the cluster.
func AlarmsAck(ctx context.Context, deps Dependencies, in AlarmsAckInput) (*mcp.CallToolResult, AlarmsAckOutput, error) {
	if in.ID != "" {
		if deps.Alarms.Ack(in.ID, in.By) {
			return nil, AlarmsAckOutput{Acked: 1, Message: "acknowledged"}, nil
		}
		return nil, AlarmsAckOutput{Acked: 0, Message: "alarm id not found"}, nil
	}
	if !in.MatchFilter {
		return nil, AlarmsAckOutput{Acked: 0, Message: "must supply id or set match_filter=true"}, nil
	}
	n := deps.Alarms.AckMatching(diagnostics.Filter{
		MinSeverity: diagnostics.Severity(in.MinSeverity),
		Kind:        in.Kind,
		Source:      in.Source,
		Node:        in.Node,
		Object:      in.Object,
	}, in.By)
	return nil, AlarmsAckOutput{Acked: n, Message: "acknowledged matching alarms"}, nil
}

// AlarmsClearInput is intentionally empty; no cluster side-effects.
type AlarmsClearInput struct {
	Confirm bool `json:"confirm"`
}

// AlarmsClearOutput confirms the reset.
type AlarmsClearOutput struct {
	Cleared bool   `json:"cleared"`
	Message string `json:"message"`
}

// AlarmsClear drops every alarm from the in-memory sink. This does NOT change
// cluster state. Useful after a remediation to start a fresh observation window.
func AlarmsClear(ctx context.Context, deps Dependencies, in AlarmsClearInput) (*mcp.CallToolResult, AlarmsClearOutput, error) {
	if !in.Confirm {
		return nil, AlarmsClearOutput{Cleared: false, Message: "pass confirm=true to clear"}, nil
	}
	deps.Alarms.Clear()
	return nil, AlarmsClearOutput{Cleared: true, Message: "alarms cleared"}, nil
}
