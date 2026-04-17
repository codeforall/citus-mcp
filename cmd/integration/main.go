// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Integration test runner for manual testing of MCP tools.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"citus-mcp/internal/cache"
	"citus-mcp/internal/citus/snapshotadvisor"
	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	"citus-mcp/internal/mcpserver/tools"
	"citus-mcp/internal/safety"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("CITUS_MCP_COORDINATOR_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/reb?sslmode=disable"
	}
	fmt.Println("Using DSN:", dsn)

	cfg := config.Config{
		CoordinatorDSN:              dsn,
		WorkerDSNs:                  []string{},
		ConnectTimeoutSeconds:       5,
		StatementTimeoutMs:          30000,
		AppName:                     "citus-mcp-integration",
		Mode:                        config.ModeReadOnly,
		AllowExecute:                false,
		ApprovalSecret:              "",
		MaxRows:                     200,
		MaxTextBytes:                200000,
		EnableCaching:               true,
		CacheTTLSeconds:             5,
		LogLevel:                    "info",
		SnapshotAdvisorCollectBytes: true,
	}

	logger, _ := zap.NewDevelopment()
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	guardrails := safety.NewGuardrails(cfg)
	wm := db.NewWorkerManager(cfg, pool, logger)
	caps, err := db.DetectCapabilities(ctx, pool)
	if err != nil {
		logger.Warn("capability detection failed", zap.Error(err))
	}
	fanout := db.NewFanout(pool, logger)

	deps := tools.Dependencies{
		Pool:          pool,
		Logger:        logger,
		Guardrails:    guardrails,
		Config:        cfg,
		WorkerManager: wm,
		Capabilities:  caps,
		Cache:         cache.New(),
		Fanout:        fanout,
	}

	// runners
	run("ping", func() (*mcp.CallToolResult, any, error) {
		return tools.Ping(ctx, deps, tools.PingInput{Message: "hello"})
	})
	run("server_info", func() (*mcp.CallToolResult, any, error) { return tools.ServerInfo(ctx, deps) })
	run("list_nodes", func() (*mcp.CallToolResult, any, error) { return tools.ListNodes(ctx, deps, tools.ListNodesInput{}) })
	run("list_distributed_tables", func() (*mcp.CallToolResult, any, error) {
		return tools.ListDistributedTables(ctx, deps, tools.ListDistributedTablesInput{})
	})
	run("list_shards", func() (*mcp.CallToolResult, any, error) { return tools.ListShards(ctx, deps, tools.ListShardsInput{}) })
	run("citus_cluster_summary", func() (*mcp.CallToolResult, any, error) {
		return tools.ClusterSummary(ctx, deps, tools.ClusterSummaryInput{})
	})
	v2res, v2out, err := tools.ListDistributedTablesV2(ctx, deps, tools.ListDistributedTablesV2Input{TableType: "distributed"})
	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else if v2res != nil && v2res.IsError {
		fmt.Printf("citus_list_distributed_tables error: %s\n", toJSON(v2res.StructuredContent))
	} else {
		fmt.Printf("citus_list_distributed_tables: %s\n", toJSON(v2out))
	}

	var table string
	if v2out.Tables != nil && len(v2out.Tables) > 0 {
		// prefer distributed table
		for _, t := range v2out.Tables {
			if t.TableType == "distributed" {
				table = fmt.Sprintf("%s.%s", t.Schema, t.Name)
				break
			}
		}
		if table == "" && len(v2out.Tables) > 0 {
			table = fmt.Sprintf("%s.%s", v2out.Tables[0].Schema, v2out.Tables[0].Name)
		}
		fmt.Println("Selected table (v2):", table)
	}

	ldrRes, ldrOut, err := tools.ListDistributedTables(ctx, deps, tools.ListDistributedTablesInput{})
	if err != nil {
		fmt.Printf("list_distributed_tables error: %v\n", err)
	} else if ldrRes != nil && ldrRes.IsError {
		fmt.Printf("list_distributed_tables tool error: %s\n", toJSON(ldrRes.StructuredContent))
	} else {
		fmt.Printf("list_distributed_tables: %s\n", toJSON(ldrOut))
		if table == "" && len(ldrOut.Tables) > 0 {
			table = ldrOut.Tables[0].LogicalRelID
			fmt.Println("Selected table (legacy):", table)
		}
	}

	run("citus_explain_query", func() (*mcp.CallToolResult, any, error) {
		return tools.ExplainQuery(ctx, deps, tools.ExplainQueryInput{SQL: "SELECT 1"})
	})

	if table != "" {
		run("citus_shard_skew_report", func() (*mcp.CallToolResult, any, error) {
			return tools.ShardSkewReport(ctx, deps, tools.ShardSkewInput{Table: table, Metric: "shard_count", IncludeTopShards: false})
		})
		run("citus_validate_rebalance_prereqs", func() (*mcp.CallToolResult, any, error) {
			return tools.ValidateRebalancePrereqs(ctx, deps, tools.ValidateRebalancePrereqsInput{Table: table})
		})
		run("rebalance_table_plan", func() (*mcp.CallToolResult, any, error) {
			return tools.RebalanceTablePlan(ctx, deps, tools.RebalanceTableInput{Table: table})
		})
		run("citus_rebalance_plan", func() (*mcp.CallToolResult, any, error) {
			return tools.RebalancePlan(ctx, deps, tools.RebalancePlanInput{Table: table})
		})
	} else {
		fmt.Println("No distributed table found; skipping table-dependent tools")
	}

	run("citus_snapshot_source_advisor", func() (*mcp.CallToolResult, any, error) {
		return tools.SnapshotSourceAdvisor(ctx, deps, tools.SnapshotSourceAdvisorInput{Strategy: string(snapshotadvisor.StrategyHybrid)})
	})

	run("citus_list_reference_tables", func() (*mcp.CallToolResult, any, error) {
		return tools.ListDistributedTablesV2(ctx, deps, tools.ListDistributedTablesV2Input{TableType: "reference"})
	})

	fmt.Println("Done at", time.Now().Format(time.RFC3339))
}

// run executes a tool function and prints result; returns raw output for chaining (best-effort type assert)
func run(name string, fn func() (*mcp.CallToolResult, any, error)) any {
	fmt.Printf("\n=== %s ===\n", name)
	res, out, err := fn()
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return nil
	}
	if res != nil && res.IsError {
		fmt.Printf("tool error: %s\n", toJSON(res.StructuredContent))
		return nil
	}
	fmt.Println(toJSON(out))
	return out
}

func toJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("<json error: %v>", err)
	}
	return string(b)
}

// import alias to avoid circular import
// (Place at bottom to keep main concise)
// sentinel
