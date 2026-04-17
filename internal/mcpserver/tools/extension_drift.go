// citus-mcp: B16 citus_extension_drift_scanner -- reads pg_extension on
// every node and flags version/installed-status drift for a curated list
// of Citus-adjacent extensions. Pure read-only.

package tools

import (
	"context"
	"fmt"
	"sort"

	"citus-mcp/internal/db"
	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ExtensionDriftInput struct {
	Extensions []string `json:"extensions,omitempty"` // default curated set
}

type NodeExtensionMap struct {
	Node      string            `json:"node"`
	Versions  map[string]string `json:"versions"` // extname -> version (or "" if absent)
}

type ExtensionDriftReport struct {
	Extension string            `json:"extension"`
	PerNode   map[string]string `json:"per_node"`
	Drifted   bool              `json:"drifted"`
	Absent    []string          `json:"absent_nodes,omitempty"`
}

type ExtensionDriftOutput struct {
	Nodes   []NodeExtensionMap     `json:"nodes"`
	Reports []ExtensionDriftReport `json:"reports"`
	Alarms  []diagnostics.Alarm    `json:"alarms"`
	Warnings []string              `json:"warnings,omitempty"`
}

var curatedExtensions = []string{
	"citus", "citus_columnar", "pg_partman", "pg_cron",
	"postgres_fdw", "pg_stat_statements", "pg_trgm", "pgcrypto",
	"hll", "topn",
}

func ExtensionDriftScannerTool(ctx context.Context, deps Dependencies, in ExtensionDriftInput) (*mcp.CallToolResult, ExtensionDriftOutput, error) {
	out := ExtensionDriftOutput{Nodes: []NodeExtensionMap{}, Reports: []ExtensionDriftReport{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	exts := in.Extensions
	if len(exts) == 0 {
		exts = curatedExtensions
	}

	// Query coordinator
	coordNM := NodeExtensionMap{Node: "coordinator", Versions: map[string]string{}}
	for _, e := range exts {
		coordNM.Versions[e] = ""
	}
	coordRows, err := deps.Pool.Query(ctx,
		`SELECT extname, extversion FROM pg_extension WHERE extname = ANY($1)`, exts)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("coordinator pg_extension: %v", err))
	} else {
		for coordRows.Next() {
			var name, ver string
			if err := coordRows.Scan(&name, &ver); err == nil {
				coordNM.Versions[name] = ver
			}
		}
		coordRows.Close()
	}
	out.Nodes = append(out.Nodes, coordNM)

	// Query workers via fanout
	if deps.Fanout != nil {
		// Build query with inline array literal (no parameters allowed in run_command_on_workers)
		workerSQL := `SELECT extname, COALESCE(extversion, '') AS extversion FROM pg_extension WHERE extname = ANY(ARRAY[`
		for i, e := range exts {
			if i > 0 {
				workerSQL += ","
			}
			workerSQL += db.QuoteLiteral(e)
		}
		workerSQL += `])`

		results, err := deps.Fanout.OnWorkers(ctx, workerSQL)
		if err != nil {
			out.Warnings = append(out.Warnings, fmt.Sprintf("fanout query failed: %v", err))
		} else {
			for _, r := range results {
				nm := NodeExtensionMap{Node: fmt.Sprintf("%s:%d", r.NodeName, r.NodePort), Versions: map[string]string{}}
				for _, e := range exts {
					nm.Versions[e] = ""
				}
				if !r.Success {
					out.Warnings = append(out.Warnings, fmt.Sprintf("%s pg_extension: %s", nm.Node, r.Error))
				} else {
					for _, row := range r.Rows {
						if name, ok := row["extname"].(string); ok {
							if ver, ok := row["extversion"].(string); ok {
								nm.Versions[name] = ver
							}
						}
					}
				}
				out.Nodes = append(out.Nodes, nm)
			}
		}
	}

	// Sort for stable output.
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].Node < out.Nodes[j].Node })

	// Build per-extension reports.
	for _, e := range exts {
		rep := ExtensionDriftReport{Extension: e, PerNode: map[string]string{}, Absent: []string{}}
		seen := map[string]bool{}
		for _, nm := range out.Nodes {
			v := nm.Versions[e]
			rep.PerNode[nm.Node] = v
			if v == "" {
				rep.Absent = append(rep.Absent, nm.Node)
			} else {
				seen[v] = true
			}
		}
		if len(seen) > 1 || (len(rep.Absent) > 0 && len(seen) > 0) {
			rep.Drifted = true
			if deps.Alarms != nil {
				sev := diagnostics.SeverityWarning
				if e == "citus" {
					sev = diagnostics.SeverityCritical
				}
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "extension.drift", Severity: sev,
					Source:  "citus_extension_drift_scanner",
					Message: fmt.Sprintf("Extension %q version/availability drift across nodes", e),
					Evidence: map[string]any{"extension": e, "per_node": rep.PerNode, "absent_nodes": rep.Absent},
					FixHint: "CREATE EXTENSION / ALTER EXTENSION UPDATE to match. For citus itself, version mismatch is unsupported.",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		}
		out.Reports = append(out.Reports, rep)
	}
	return nil, out, nil
}
