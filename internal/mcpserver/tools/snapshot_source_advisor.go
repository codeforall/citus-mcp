// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_snapshot_source_advisor for node scaling.

package tools

import (
	"context"

	"citus-mcp/internal/citus/snapshotadvisor"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// SnapshotSourceAdvisorInput input schema.
type SnapshotSourceAdvisorInput struct {
	Strategy          string   `json:"strategy,omitempty"`
	MaxCandidates     *int     `json:"max_candidates,omitempty"`
	ExcludeNodes      []string `json:"exclude_nodes,omitempty"`
	RequireReachable  *bool    `json:"require_reachable,omitempty"`
	IncludeSimulation *bool    `json:"include_simulation,omitempty"`
	IncludeNextSteps  *bool    `json:"include_next_steps,omitempty"`
}

// SnapshotSourceAdvisorOutput output alias.
type SnapshotSourceAdvisorOutput = snapshotadvisor.Output

// SnapshotSourceAdvisor tool entry.
func SnapshotSourceAdvisor(ctx context.Context, deps Dependencies, input SnapshotSourceAdvisorInput) (*mcp.CallToolResult, SnapshotSourceAdvisorOutput, error) {
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		me := serr.ToToolError(err)
		return callError(me.Code, me.Message, me.Hint), SnapshotSourceAdvisorOutput{}, nil
	}

	in := snapshotadvisor.Input{}
	switch input.Strategy {
	case string(snapshotadvisor.StrategyByDiskSize):
		in.Strategy = snapshotadvisor.StrategyByDiskSize
	case string(snapshotadvisor.StrategyByShardCount):
		in.Strategy = snapshotadvisor.StrategyByShardCount
	case string(snapshotadvisor.StrategyHybrid):
		fallthrough
	default:
		in.Strategy = snapshotadvisor.StrategyHybrid
	}
	if input.MaxCandidates != nil {
		in.MaxCandidates = *input.MaxCandidates
	}
	if input.RequireReachable != nil {
		in.RequireReachable = *input.RequireReachable
	} else {
		in.RequireReachable = true
	}
	if input.IncludeSimulation != nil {
		in.IncludeSimulation = *input.IncludeSimulation
	} else {
		in.IncludeSimulation = true
	}
	if input.IncludeNextSteps != nil {
		in.IncludeNextSteps = *input.IncludeNextSteps
	} else {
		in.IncludeNextSteps = true
	}
	in.ExcludeNodes = input.ExcludeNodes

	out, err := snapshotadvisor.Run(ctx, snapshotadvisor.Dependencies{
		Pool:          deps.Pool,
		WorkerManager: deps.WorkerManager,
		Fanout:        deps.Fanout,
		Cache:         deps.Cache,
		Config:        deps.Config,
		Capabilities:  deps.Capabilities,
		Logger:        deps.Logger,
		CollectBytes:  deps.Config.SnapshotAdvisorCollectBytes,
	}, in)
	if err != nil {
		if err == snapshotadvisor.ErrNotCitus {
			return callError(serr.CodeNotCitus, "citus extension not found", "install citus"), SnapshotSourceAdvisorOutput{}, nil
		}
		return callError(serr.CodeInternalError, err.Error(), "advisor error"), SnapshotSourceAdvisorOutput{}, nil
	}
	return nil, out, nil
}
