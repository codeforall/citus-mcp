// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Snapshot source advisor for node scaling recommendations.

package snapshotadvisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"citus-mcp/internal/cache"
	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Dependencies for advisor.
type Dependencies struct {
	Pool          *pgxpool.Pool
	WorkerManager *db.WorkerManager
	Fanout        *db.Fanout
	Cache         *cache.Cache
	Config        config.Config
	Capabilities  *db.Capabilities
	Logger        *zap.Logger
	CollectBytes  bool
}

var ErrNotCitus = errors.New("NOT_CITUS")

// Run executes advisor and returns output with warnings embedded.
func Run(ctx context.Context, deps Dependencies, input Input) (Output, error) {
	in := applyDefaults(input)

	// Validate Citus
	if deps.Capabilities == nil || !deps.Capabilities.HasCitusExtension {
		return Output{}, ErrNotCitus
	}

	warnings := []Warning{}

	// Version warning if < 13.2
	if warn := warnIfVersionLess(deps.Capabilities.CitusVersion, 13, 2); warn != nil {
		warnings = append(warnings, *warn)
	}

	// Collect workers
	workers, collectWarnings, err := collectWorkers(ctx, deps, in)
	warnings = append(warnings, collectWarnings...)
	if err != nil {
		return Output{}, err
	}

	// If no workers, return empty recommendation
	if len(workers) == 0 {
		out := Output{Summary: Summary{AdvisorVersion: "v1", Strategy: in.Strategy, WorkersConsidered: 0, GeneratedAt: time.Now().UTC(), Note: note()}}
		out.Warnings = warnings
		return out, nil
	}

	// Compute metrics before
	before := computeClusterMetrics(workers)

	// Ideal target after adding one worker
	ideal := computeIdealTarget(before)

	// Candidates & scoring
	candidates := []Candidate{}
	if in.IncludeSimulation {
		var scoreWarnings []Warning
		candidates, scoreWarnings = scoreCandidates(workers, before, ideal, in.Strategy)
		warnings = append(warnings, scoreWarnings...)
	}

	// Sort and truncate candidates
	candidates = sortAndTruncate(candidates, in.MaxCandidates)

	var recommendation *Recommendation
	if len(candidates) > 0 {
		rec := Recommendation{
			Source:         candidates[0].Source,
			Score:          candidates[0].Score,
			PredictedAfter: candidates[0].PredictedAfter,
			Reasons:        buildReasons(before, ideal, candidates[0], in.Strategy),
		}
		recommendation = &rec
	}

	nextSteps := []NextStep{}
	if in.IncludeNextSteps {
		nextSteps = defaultNextSteps()
	}

	out := Output{
		Summary: Summary{
			AdvisorVersion:    "v1",
			Strategy:          in.Strategy,
			WorkersConsidered: len(workers),
			GeneratedAt:       time.Now().UTC(),
			Note:              note(),
		},
		ClusterMetricsBefore: before,
		IdealTargetAfter:     ideal,
		Recommendation:       recommendation,
		Candidates:           candidates,
		NextSteps:            nextSteps,
		Warnings:             warnings,
	}
	return out, nil
}

func note() string {
	return "Snapshot-based node addition only rebalances between the chosen source and the clone."
}

func applyDefaults(in Input) Input {
	if in.Strategy == "" {
		in.Strategy = StrategyHybrid
	}
	if in.MaxCandidates <= 0 {
		in.MaxCandidates = 5
	}
	if in.MaxCandidates > 50 {
		in.MaxCandidates = 50
	}
	return in
}

func warnIfVersionLess(v string, major int, minor int) *Warning {
	mv, mm := parseVersion(v)
	if mv < major || (mv == major && mm < minor) {
		return &Warning{Code: "CAPABILITY_MISSING", Message: fmt.Sprintf("Citus %s < %d.%d; snapshot-based clone is supported in Citus 13.2+", v, major, minor)}
	}
	return nil
}

// parseVersion in version.go
