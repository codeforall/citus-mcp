// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Metadata health detection orchestration.

package metadata

import (
	"context"
	"time"

	"citus-mcp/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Detector orchestrates metadata health checks.
type Detector struct {
	pool          *pgxpool.Pool
	workerManager *db.WorkerManager
	fanout        *db.Fanout
}

// NewDetector creates a new metadata health detector.
func NewDetector(pool *pgxpool.Pool, workerManager *db.WorkerManager, fanout *db.Fanout) *Detector {
	return &Detector{
		pool:          pool,
		workerManager: workerManager,
		fanout:        fanout,
	}
}

// Run executes metadata health checks based on input parameters.
func (d *Detector) Run(ctx context.Context, input Input) (Output, error) {
	output := Output{
		Checks:       []Check{},
		Issues:       []Issue{},
		CheckedNodes: []string{"coordinator"},
		Warnings:     []string{},
	}

	// Normalize input
	if input.CheckLevel == "" {
		input.CheckLevel = CheckLevelBasic
	}

	start := time.Now()

	// Run coordinator-only checks
	coordResults := d.runCoordinatorChecks(ctx, input)
	for _, result := range coordResults {
		output.Checks = append(output.Checks, result.Check)
		if result.Error != nil {
			output.Warnings = append(output.Warnings, "Check "+result.Check.ID+" error: "+result.Error.Error())
		}
		output.Issues = append(output.Issues, result.Issues...)
	}

	// Run cross-node checks if requested
	if input.CheckLevel == CheckLevelDeep {
		crossResults := d.runCrossNodeChecks(ctx, input)
		for _, result := range crossResults {
			output.Checks = append(output.Checks, result.Check)
			if result.Error != nil {
				output.Warnings = append(output.Warnings, "Check "+result.Check.ID+" error: "+result.Error.Error())
			}
			output.Issues = append(output.Issues, result.Issues...)
		}
		output.CheckedNodes = append(output.CheckedNodes, "workers")
	}

	// Calculate summary
	output.Summary = d.calculateSummary(output.Checks, output.Issues)
	output.Healthy = output.Summary.Critical == 0

	_ = time.Since(start)

	return output, nil
}

// runCoordinatorChecks runs all coordinator-only checks.
func (d *Detector) runCoordinatorChecks(ctx context.Context, input Input) []CheckResult {
	results := []CheckResult{}

	// Basic checks (always run)
	results = append(results, checkOrphanedShards(ctx, d.pool, input.IncludeFixes))
	results = append(results, checkOrphanedPlacements(ctx, d.pool, input.IncludeFixes))
	results = append(results, checkMissingRelations(ctx, d.pool, input.IncludeFixes))
	results = append(results, checkInvalidNodeRefs(ctx, d.pool, input.IncludeFixes))

	// Thorough checks
	if input.CheckLevel == CheckLevelThorough || input.CheckLevel == CheckLevelDeep {
		results = append(results, checkStaleCleanupRecords(ctx, d.pool, input.IncludeFixes))
		results = append(results, checkColocationMismatch(ctx, d.pool, input.IncludeFixes))
		results = append(results, checkUnsyncedNodes(ctx, d.pool, input.IncludeFixes))
		results = append(results, checkShardRangeGaps(ctx, d.pool, input.IncludeFixes))
		results = append(results, checkShardRangeOverlaps(ctx, d.pool, input.IncludeFixes))
		results = append(results, checkReferenceTableConsistency(ctx, d.pool, input.IncludeFixes))
	}

	return results
}

// runCrossNodeChecks runs cross-node validation checks.
func (d *Detector) runCrossNodeChecks(ctx context.Context, input Input) []CheckResult {
	results := []CheckResult{}

	if d.workerManager == nil {
		return results
	}

	checker := NewCrossNodeChecker(d.pool, d.workerManager, d.fanout, input.IncludeFixes)

	results = append(results, checker.CheckExtensionVersions(ctx))
	results = append(results, checker.CheckShardExistence(ctx))
	results = append(results, checker.CheckMetadataSync(ctx))

	return results
}

// calculateSummary calculates the summary statistics.
func (d *Detector) calculateSummary(checks []Check, issues []Issue) Summary {
	summary := Summary{
		TotalChecks: len(checks),
	}

	for _, check := range checks {
		switch check.Status {
		case "passed":
			summary.Passed++
		case "failed":
			summary.Failed++
		case "skipped":
			summary.Skipped++
		}
	}

	for _, issue := range issues {
		switch issue.Severity {
		case SeverityCritical:
			summary.Critical++
		case SeverityWarning:
			summary.Warning++
		case SeverityInfo:
			summary.Info++
		}
	}

	return summary
}

// QuickHealthCheck performs a fast health check returning just healthy/unhealthy.
func QuickHealthCheck(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	detector := NewDetector(pool, nil, nil)
	output, err := detector.Run(ctx, Input{CheckLevel: CheckLevelBasic})
	if err != nil {
		return false, err
	}
	return output.Healthy, nil
}
