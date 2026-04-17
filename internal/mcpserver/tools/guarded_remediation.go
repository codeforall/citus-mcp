// citus-mcp: B26 guarded remediation tools --
//   citus_isolate_tenant (wraps isolate_tenant_to_new_shard)
//   citus_cleanup_orphaned (wraps citus_cleanup_orphaned_resources)
//
// Both require a valid approval token and allow_execute=true. Both are
// functions Citus itself provides; we just gate and audit them.

package tools

import (
	"context"
	"fmt"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type IsolateTenantInput struct {
	ApprovalToken string `json:"approval_token" jsonschema:"required"`
	TableName     string `json:"table_name" jsonschema:"required"`
	TenantValue   string `json:"tenant_value" jsonschema:"required"`
	// CascadeOption: 'CASCADE' or 'NO CASCADE'. Default CASCADE.
	CascadeOption string `json:"cascade_option,omitempty"`
}

type IsolateTenantOutput struct {
	NewShardID int64  `json:"new_shard_id"`
	Message    string `json:"message"`
}

func IsolateTenantTool(ctx context.Context, deps Dependencies, in IsolateTenantInput) (*mcp.CallToolResult, IsolateTenantOutput, error) {
	if err := deps.Guardrails.RequireToolAllowed("citus_isolate_tenant", true, in.ApprovalToken); err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), IsolateTenantOutput{}, nil
		}
		return callError(serr.CodeExecuteDisabled, err.Error(), ""), IsolateTenantOutput{}, nil
	}
	if in.CascadeOption == "" {
		in.CascadeOption = "CASCADE"
	}
	if in.CascadeOption != "CASCADE" && in.CascadeOption != "NO CASCADE" {
		return callError(serr.CodeInvalidInput, "cascade_option must be 'CASCADE' or 'NO CASCADE'", ""), IsolateTenantOutput{}, nil
	}
	var newShard int64
	q := fmt.Sprintf(`SELECT isolate_tenant_to_new_shard($1::regclass, $2, %q)`, in.CascadeOption)
	if err := deps.Pool.QueryRow(ctx, q, in.TableName, in.TenantValue).Scan(&newShard); err != nil {
		return callError(serr.CodeInternalError, err.Error(), "isolate_tenant_to_new_shard failed"), IsolateTenantOutput{}, nil
	}
	return nil, IsolateTenantOutput{NewShardID: newShard,
		Message: fmt.Sprintf("Isolated tenant %q on %s to new shard %d", in.TenantValue, in.TableName, newShard)}, nil
}

type CleanupOrphanedInput struct {
	ApprovalToken string `json:"approval_token" jsonschema:"required"`
}

type CleanupOrphanedOutput struct {
	Message string `json:"message"`
}

func CleanupOrphanedTool(ctx context.Context, deps Dependencies, in CleanupOrphanedInput) (*mcp.CallToolResult, CleanupOrphanedOutput, error) {
	if err := deps.Guardrails.RequireToolAllowed("citus_cleanup_orphaned", true, in.ApprovalToken); err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), CleanupOrphanedOutput{}, nil
		}
		return callError(serr.CodeExecuteDisabled, err.Error(), ""), CleanupOrphanedOutput{}, nil
	}
	if _, err := deps.Pool.Exec(ctx, `CALL citus_cleanup_orphaned_resources()`); err != nil {
		// Older versions had citus_cleanup_orphaned_shards() — try that too.
		if _, err2 := deps.Pool.Exec(ctx, `SELECT citus_cleanup_orphaned_shards()`); err2 != nil {
			return callError(serr.CodeInternalError, err.Error(), "neither citus_cleanup_orphaned_resources nor citus_cleanup_orphaned_shards available"), CleanupOrphanedOutput{}, nil
		}
		return nil, CleanupOrphanedOutput{Message: "Called citus_cleanup_orphaned_shards() (older API)"}, nil
	}
	return nil, CleanupOrphanedOutput{Message: "Called citus_cleanup_orphaned_resources()"}, nil
}
