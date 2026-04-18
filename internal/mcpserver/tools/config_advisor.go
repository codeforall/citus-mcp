// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_config_advisor tool for configuration analysis.

package tools

import (
	"context"
	"fmt"
	"sort"

	"citus-mcp/internal/citus/guc"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ConfigAdvisorInput defines input parameters for the config advisor tool.
type ConfigAdvisorInput struct {
	// IncludeAllGUCs returns all Citus and relevant PostgreSQL GUCs (verbose)
	IncludeAllGUCs bool `json:"include_all_gucs,omitempty"`
	// Category filter: memory, connections, parallelism, replication, performance, etc.
	Category string `json:"category,omitempty"`
	// SeverityFilter: info, warning, critical (returns this level and above)
	SeverityFilter string `json:"severity_filter,omitempty"`
	// TotalRAMGB: Optional RAM size in GB for memory recommendations
	TotalRAMGB int `json:"total_ram_gb,omitempty"`
}

// ConfigAdvisorOutput is the result of configuration analysis.
type ConfigAdvisorOutput struct {
	Summary         ConfigSummary            `json:"summary"`
	Findings        []guc.ConfigFinding      `json:"findings"`
	CitusConfig     map[string]GUCDisplay    `json:"citus_config,omitempty"`
	PostgresConfig  map[string]GUCDisplay    `json:"postgres_config,omitempty"`
	CategorySummary map[string]CategoryStats `json:"category_summary"`
	Recommendations []string                 `json:"top_recommendations"`
}

// ConfigSummary provides overview statistics.
type ConfigSummary struct {
	TotalFindings   int    `json:"total_findings"`
	CriticalCount   int    `json:"critical_count"`
	WarningCount    int    `json:"warning_count"`
	InfoCount       int    `json:"info_count"`
	OverallHealth   string `json:"overall_health"`
	CitusVersion    string `json:"citus_version,omitempty"`
	PostgresVersion string `json:"postgres_version,omitempty"`
	WorkerCount     int    `json:"worker_count"`
	RequiresRestart int    `json:"changes_requiring_restart"`
}

// GUCDisplay is a simplified GUC value for output.
type GUCDisplay struct {
	Value       string `json:"value"`
	Unit        string `json:"unit,omitempty"`
	Source      string `json:"source"`
	Description string `json:"description,omitempty"`
	IsDefault   bool   `json:"is_default"`
	IsCritical  bool   `json:"is_critical,omitempty"`
}

// CategoryStats summarizes findings per category.
type CategoryStats struct {
	Total    int `json:"total"`
	Critical int `json:"critical"`
	Warning  int `json:"warning"`
	Info     int `json:"info"`
}

func configAdvisorTool(ctx context.Context, deps Dependencies, input ConfigAdvisorInput) (*mcp.CallToolResult, ConfigAdvisorOutput, error) {
	// Read-only guard
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ConfigAdvisorOutput{}, nil
	}

	// Fetch GUCs
	citusGUCs, err := guc.FetchCitusGUCs(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, "failed to fetch Citus GUCs: "+err.Error(), ""), ConfigAdvisorOutput{}, nil
	}

	postgresGUCs, err := guc.FetchRelevantPostgresGUCs(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, "failed to fetch PostgreSQL GUCs: "+err.Error(), ""), ConfigAdvisorOutput{}, nil
	}

	// Get worker count
	workerCount := 0
	if infos, err := deps.WorkerManager.Topology(ctx); err == nil {
		workerCount = len(infos)
	}

	// Get version info
	var citusVersion, pgVersion string
	_ = deps.Pool.QueryRow(ctx, "SELECT citus_version()::text").Scan(&citusVersion)
	_ = deps.Pool.QueryRow(ctx, "SHOW server_version").Scan(&pgVersion)

	// Build analysis context
	analysisCtx := &guc.AnalysisContext{
		CitusGUCs:     citusGUCs,
		PostgresGUCs:  postgresGUCs,
		WorkerCount:   workerCount,
		IsCoordinator: true, // Assume coordinator for now
	}

	// Measure existing replication-slot usage on the coordinator so
	// replication-budget rules recommend on top of the slots the user
	// is already consuming (CDC subscribers, standbys), not assume a
	// clean slate.
	var activeSlots int64
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots").Scan(&activeSlots); err == nil {
		analysisCtx.ExistingReplicationSlots = activeSlots
	}

	// Set RAM if provided
	if input.TotalRAMGB > 0 {
		analysisCtx.TotalRAMBytes = int64(input.TotalRAMGB) * 1024 * 1024 * 1024
	}

	// Run analysis rules
	allFindings := guc.EvaluateAllRules(analysisCtx)

	// Filter by category if specified
	if input.Category != "" {
		filtered := make([]guc.ConfigFinding, 0)
		for _, f := range allFindings {
			if string(f.Category) == input.Category {
				filtered = append(filtered, f)
			}
		}
		allFindings = filtered
	}

	// Filter by severity if specified
	if input.SeverityFilter != "" {
		filtered := make([]guc.ConfigFinding, 0)
		minSeverity := severityRank(guc.Severity(input.SeverityFilter))
		for _, f := range allFindings {
			if severityRank(f.Severity) >= minSeverity {
				filtered = append(filtered, f)
			}
		}
		allFindings = filtered
	}

	// Sort findings by severity (critical first)
	sort.Slice(allFindings, func(i, j int) bool {
		return severityRank(allFindings[i].Severity) > severityRank(allFindings[j].Severity)
	})

	// Calculate statistics
	var criticalCount, warningCount, infoCount, restartCount int
	categorySummary := make(map[string]CategoryStats)

	for _, f := range allFindings {
		switch f.Severity {
		case guc.SeverityCritical:
			criticalCount++
		case guc.SeverityWarning:
			warningCount++
		case guc.SeverityInfo:
			infoCount++
		}
		if f.RequiresRestart {
			restartCount++
		}

		// Update category summary
		cat := string(f.Category)
		cs := categorySummary[cat]
		cs.Total++
		switch f.Severity {
		case guc.SeverityCritical:
			cs.Critical++
		case guc.SeverityWarning:
			cs.Warning++
		case guc.SeverityInfo:
			cs.Info++
		}
		categorySummary[cat] = cs
	}

	// Determine overall health
	overallHealth := "healthy"
	if criticalCount > 0 {
		overallHealth = "critical"
	} else if warningCount > 0 {
		overallHealth = "needs_attention"
	}

	// Build output
	output := ConfigAdvisorOutput{
		Summary: ConfigSummary{
			TotalFindings:   len(allFindings),
			CriticalCount:   criticalCount,
			WarningCount:    warningCount,
			InfoCount:       infoCount,
			OverallHealth:   overallHealth,
			CitusVersion:    citusVersion,
			PostgresVersion: pgVersion,
			WorkerCount:     workerCount,
			RequiresRestart: restartCount,
		},
		Findings:        allFindings,
		CategorySummary: categorySummary,
	}

	// Extract top recommendations (critical and warning titles)
	topRecs := make([]string, 0, 5)
	for _, f := range allFindings {
		if f.Severity == guc.SeverityCritical || f.Severity == guc.SeverityWarning {
			topRecs = append(topRecs, fmt.Sprintf("[%s] %s", f.Severity, f.Title))
			if len(topRecs) >= 5 {
				break
			}
		}
	}
	output.Recommendations = topRecs

	// Include all GUCs if requested
	if input.IncludeAllGUCs {
		output.CitusConfig = make(map[string]GUCDisplay)
		output.PostgresConfig = make(map[string]GUCDisplay)

		for name, g := range citusGUCs {
			unit := ""
			if g.Unit != nil {
				unit = *g.Unit
			}
			isDefault := g.Source == "default"
			def := guc.CitusGUCs[name]
			output.CitusConfig[name] = GUCDisplay{
				Value:       g.Setting,
				Unit:        unit,
				Source:      g.Source,
				Description: g.ShortDesc,
				IsDefault:   isDefault,
				IsCritical:  def.IsCritical,
			}
		}

		for name, g := range postgresGUCs {
			unit := ""
			if g.Unit != nil {
				unit = *g.Unit
			}
			isDefault := g.Source == "default"
			def := guc.PostgresGUCs[name]
			output.PostgresConfig[name] = GUCDisplay{
				Value:       g.Setting,
				Unit:        unit,
				Source:      g.Source,
				Description: g.ShortDesc,
				IsDefault:   isDefault,
				IsCritical:  def.IsCritical,
			}
		}
	}

	return nil, output, nil
}

func severityRank(s guc.Severity) int {
	switch s {
	case guc.SeverityCritical:
		return 3
	case guc.SeverityWarning:
		return 2
	case guc.SeverityInfo:
		return 1
	default:
		return 0
	}
}

// ConfigAdvisor is the exported function for the tool.
func ConfigAdvisor(ctx context.Context, deps Dependencies, input ConfigAdvisorInput) (*mcp.CallToolResult, ConfigAdvisorOutput, error) {
	return configAdvisorTool(ctx, deps, input)
}
