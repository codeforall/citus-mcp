// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_metadata_health tool for corruption detection.

package tools

import (
	"context"

	"citus-mcp/internal/citus/metadata"
	serr "citus-mcp/internal/errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MetadataHealthInput defines input for the citus_metadata_health tool.
type MetadataHealthInput struct {
	// CheckLevel: basic (fast), thorough (all coordinator), deep (cross-node)
	CheckLevel          string `json:"check_level,omitempty"`
	IncludeFixes        bool   `json:"include_fixes,omitempty"`
	IncludeVerification bool   `json:"include_verification,omitempty"`
}

// MetadataHealthOutput is the output from the metadata health tool.
type MetadataHealthOutput struct {
	Summary      metadata.Summary `json:"summary"`
	Checks       []metadata.Check `json:"checks"`
	Issues       []metadata.Issue `json:"issues"`
	Healthy      bool             `json:"healthy"`
	CheckedNodes []string         `json:"checked_nodes"`
	Warnings     []string         `json:"warnings,omitempty"`
}

func metadataHealthTool(ctx context.Context, deps Dependencies, input MetadataHealthInput) (*mcp.CallToolResult, MetadataHealthOutput, error) {
	emptyOutput := MetadataHealthOutput{
		Checks:       []metadata.Check{},
		Issues:       []metadata.Issue{},
		CheckedNodes: []string{},
		Warnings:     []string{},
	}

	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), emptyOutput, nil
	}

	// Map input to internal type
	checkLevel := metadata.CheckLevelBasic
	switch input.CheckLevel {
	case "thorough":
		checkLevel = metadata.CheckLevelThorough
	case "deep":
		checkLevel = metadata.CheckLevelDeep
	}

	detector := metadata.NewDetector(deps.Pool, deps.WorkerManager, deps.Fanout)
	out, err := detector.Run(ctx, metadata.Input{
		CheckLevel:          checkLevel,
		IncludeFixes:        input.IncludeFixes,
		IncludeVerification: input.IncludeVerification,
	})
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), emptyOutput, nil
	}

	// Convert to output type
	output := MetadataHealthOutput{
		Summary:      out.Summary,
		Checks:       out.Checks,
		Issues:       out.Issues,
		Healthy:      out.Healthy,
		CheckedNodes: out.CheckedNodes,
		Warnings:     out.Warnings,
	}

	// Ensure non-nil slices for JSON
	if output.Checks == nil {
		output.Checks = []metadata.Check{}
	}
	if output.Issues == nil {
		output.Issues = []metadata.Issue{}
	}
	if output.CheckedNodes == nil {
		output.CheckedNodes = []string{}
	}
	if output.Warnings == nil {
		output.Warnings = []string{}
	}

	return nil, output, nil
}

// CitusMetadataHealth is the exported function for the tool.
func CitusMetadataHealth(ctx context.Context, deps Dependencies, input MetadataHealthInput) (*mcp.CallToolResult, MetadataHealthOutput, error) {
	return metadataHealthTool(ctx, deps, input)
}
