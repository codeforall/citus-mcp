// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// MCP resource registration for cluster metadata exposure.

package resources

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"

	"citus-mcp/internal/mcpserver/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RegisterAll registers resources with the MCP server. Currently a no-op placeholder.
func RegisterAll(server *mcp.Server, deps tools.Dependencies) {
	server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "citus://cluster/summary", Name: "cluster summary", MIMEType: "application/json"}, resourceClusterSummary(deps))
	server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "citus://metadata/distributed_tables{?schema,cursor,limit}", Name: "distributed tables", MIMEType: "application/json"}, resourceDistributedTables(deps))
	server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "citus://shards/skew{?table,metric,include_top_shards}", Name: "shard skew", MIMEType: "application/json"}, resourceShardSkew(deps))
	server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "citus://alarms{?min_severity,kind,source,node,object,include_acked,limit}", Name: "diagnostic alarms", MIMEType: "application/json"}, resourceAlarms(deps))
}

func resourceClusterSummary(deps tools.Dependencies) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		_, out, err := tools.ClusterSummary(ctx, deps, tools.ClusterSummaryInput{})
		if err != nil {
			return nil, err
		}
		return jsonResource(out)
	}
}

func resourceDistributedTables(deps tools.Dependencies) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		u, _ := url.Parse(req.Params.URI)
		q := u.Query()
		limit := parseInt(q.Get("limit"), 50, 200)
		input := tools.ListDistributedTablesV2Input{
			Schema: q.Get("schema"),
			Cursor: q.Get("cursor"),
			Limit:  limit,
		}
		_, out, err := tools.ListDistributedTablesV2(ctx, deps, input)
		if err != nil {
			return nil, err
		}
		return jsonResource(out)
	}
}

func resourceShardSkew(deps tools.Dependencies) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		u, _ := url.Parse(req.Params.URI)
		q := u.Query()
		input := tools.ShardSkewInput{
			Table:            q.Get("table"),
			Metric:           q.Get("metric"),
			IncludeTopShards: parseBool(q.Get("include_top_shards"), true),
		}
		_, out, err := tools.ShardSkewReport(ctx, deps, input)
		if err != nil {
			return nil, err
		}
		return jsonResource(out)
	}
}

func resourceAlarms(deps tools.Dependencies) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		u, _ := url.Parse(req.Params.URI)
		q := u.Query()
		input := tools.AlarmsListInput{
			MinSeverity:  q.Get("min_severity"),
			Kind:         q.Get("kind"),
			Source:       q.Get("source"),
			Node:         q.Get("node"),
			Object:       q.Get("object"),
			IncludeAcked: parseBool(q.Get("include_acked"), false),
			Limit:        parseInt(q.Get("limit"), 200, 1000),
		}
		_, out, err := tools.AlarmsList(ctx, deps, input)
		if err != nil {
			return nil, err
		}
		return jsonResource(out)
	}
}

func jsonResource(data any) (*mcp.ReadResourceResult, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{Text: string(b)}}}, nil
}

func parseInt(s string, def int, max int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	if v > max {
		v = max
	}
	return v
}

func parseBool(s string, def bool) bool {
	if s == "" {
		return def
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return def
	}
	return b
}
