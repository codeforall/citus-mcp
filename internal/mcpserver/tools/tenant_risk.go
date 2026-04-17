// citus-mcp: B7 citus_tenant_risk -- surface hot tenants via
// citus_stat_tenants (13.1+). Flag tenants with outsized query counts or
// CPU time; recommend isolate_tenant_to_new_shard when appropriate.
// Pure read-only.

package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/diagnostics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type TenantRiskInput struct {
	TopN                   int     `json:"top_n,omitempty"`                       // default 20
	HotShareThresholdPct   float64 `json:"hot_share_threshold_pct,omitempty"`     // default 10 (tenant > 10% of total)
	MinQueryCount          int64   `json:"min_query_count,omitempty"`             // default 100
}

type TenantRow struct {
	TenantAttribute     string  `json:"tenant_attribute"`
	ColocationID        int32   `json:"colocation_id"`
	ReadCount           int64   `json:"read_count"`
	ReadCountInThisPeriod int64 `json:"read_count_in_this_period"`
	QueryCount          int64   `json:"query_count"`
	QueryCountInThisPeriod int64 `json:"query_count_in_this_period"`
	CpuUsageInThisPeriod float64 `json:"cpu_usage_in_this_period"`
	SharePct            float64 `json:"share_pct"`
	Risk                string  `json:"risk"` // ok | hot | very_hot
}

type TenantRiskOutput struct {
	Available bool                `json:"citus_stat_tenants_available"`
	Tenants   []TenantRow         `json:"tenants"`
	TotalQueries int64            `json:"total_queries"`
	Alarms    []diagnostics.Alarm `json:"alarms"`
	Warnings  []string            `json:"warnings,omitempty"`
}

func TenantRiskTool(ctx context.Context, deps Dependencies, in TenantRiskInput) (*mcp.CallToolResult, TenantRiskOutput, error) {
	out := TenantRiskOutput{Tenants: []TenantRow{}, Alarms: []diagnostics.Alarm{}, Warnings: []string{}}
	if in.TopN == 0 {
		in.TopN = 20
	}
	if in.HotShareThresholdPct == 0 {
		in.HotShareThresholdPct = 10
	}
	if in.MinQueryCount == 0 {
		in.MinQueryCount = 100
	}

	var ok bool
	if err := deps.Pool.QueryRow(ctx,
		"SELECT to_regclass('pg_catalog.citus_stat_tenants') IS NOT NULL").Scan(&ok); err != nil || !ok {
		out.Available = false
		out.Warnings = append(out.Warnings, "citus_stat_tenants view not available (requires Citus 13.1+ and citus.stat_tenants_track=all)")
		return nil, out, nil
	}
	out.Available = true

	rows, err := deps.Pool.Query(ctx, `
SELECT tenant_attribute, colocation_id,
       COALESCE(read_count_in_this_period,0), COALESCE(read_count_in_last_period,0),
       COALESCE(query_count_in_this_period,0), COALESCE(query_count_in_last_period,0),
       COALESCE(cpu_usage_in_this_period,0.0)
FROM citus_stat_tenants
ORDER BY query_count_in_this_period DESC NULLS LAST
LIMIT $1`, in.TopN)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf("citus_stat_tenants query failed: %v", err))
		return nil, out, nil
	}
	defer rows.Close()

	var total int64
	tenantRows := []TenantRow{}
	for rows.Next() {
		var t TenantRow
		if err := rows.Scan(&t.TenantAttribute, &t.ColocationID,
			&t.ReadCountInThisPeriod, &t.ReadCount,
			&t.QueryCountInThisPeriod, &t.QueryCount,
			&t.CpuUsageInThisPeriod); err != nil {
			continue
		}
		total += t.QueryCountInThisPeriod
		tenantRows = append(tenantRows, t)
	}
	out.TotalQueries = total

	for i := range tenantRows {
		t := &tenantRows[i]
		if total > 0 {
			t.SharePct = float64(t.QueryCountInThisPeriod) * 100.0 / float64(total)
		}
		switch {
		case t.QueryCountInThisPeriod < in.MinQueryCount:
			t.Risk = "ok"
		case t.SharePct >= 2*in.HotShareThresholdPct:
			t.Risk = "very_hot"
			if deps.Alarms != nil {
				a := deps.Alarms.Emit(diagnostics.Alarm{
					Kind: "tenant.very_hot", Severity: diagnostics.SeverityWarning,
					Source: "citus_tenant_risk",
					Message: fmt.Sprintf("Tenant %q accounts for %.1f%% of queries this period", t.TenantAttribute, t.SharePct),
					Evidence: map[string]any{"tenant": t.TenantAttribute, "share_pct": t.SharePct, "query_count": t.QueryCountInThisPeriod, "colocation_id": t.ColocationID},
					FixHint:  "Consider SELECT isolate_tenant_to_new_shard('<table>', '<tenant_id>');",
				})
				out.Alarms = append(out.Alarms, *a)
			}
		case t.SharePct >= in.HotShareThresholdPct:
			t.Risk = "hot"
		default:
			t.Risk = "ok"
		}
	}
	out.Tenants = tenantRows
	return nil, out, nil
}
