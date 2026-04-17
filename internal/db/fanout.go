// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Fanout queries to workers through the coordinator's run_command_on_workers() UDF.

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// NodeResult is one row from run_command_on_workers: per-worker result.
type NodeResult struct {
	NodeName string           `json:"node_name"`
	NodePort int32            `json:"node_port"`
	Success  bool             `json:"success"`
	Error    string           `json:"error,omitempty"` // populated when Success=false
	Rows     []map[string]any `json:"rows,omitempty"`  // decoded from JSON-aggregated inner SELECT
	Raw      string           `json:"raw,omitempty"`   // raw text result (for debug)
}

// Fanout runs read-only queries on workers through the coordinator's
// run_command_on_workers() UDF. It never opens direct worker connections.
type Fanout struct {
	coordinator *pgxpool.Pool
	logger      *zap.Logger
}

func NewFanout(coordinator *pgxpool.Pool, logger *zap.Logger) *Fanout {
	return &Fanout{coordinator: coordinator, logger: logger}
}

// OnWorkers runs sql on every active, shouldhaveshards worker.
// sql MUST be a SELECT returning 0+ rows; it is wrapped in json_agg(row_to_json(_t))
// so callers get structured rows back.
//
// Parameters cannot be passed — inline them using QuoteLiteral / QuoteIdent
// before calling.
func (f *Fanout) OnWorkers(ctx context.Context, sql string) ([]NodeResult, error) {
	// Wrap the inner SQL to return JSON array
	wrapped := fmt.Sprintf(
		"SELECT COALESCE((SELECT json_agg(row_to_json(_t)) FROM (%s) _t)::text, '[]')",
		sql,
	)

	// Query run_command_on_workers with the wrapped SQL as a parameter
	// run_command_on_workers returns: nodename, nodeport, success, result
	rows, err := f.coordinator.Query(ctx,
		`SELECT nodename, nodeport, success, result FROM run_command_on_workers($1)`,
		wrapped,
	)
	if err != nil {
		return nil, fmt.Errorf("run_command_on_workers: %w", err)
	}
	defer rows.Close()

	var results []NodeResult
	for rows.Next() {
		var r NodeResult
		var resultText string
		if err := rows.Scan(&r.NodeName, &r.NodePort, &r.Success, &resultText); err != nil {
			return nil, fmt.Errorf("scan result: %w", err)
		}
		r.Raw = resultText

		if r.Success {
			// Try to decode JSON array
			if err := json.Unmarshal([]byte(resultText), &r.Rows); err != nil {
				r.Error = fmt.Sprintf("json decode failed: %v", err)
				f.logger.Warn("fanout json decode failed",
					zap.String("node", r.NodeName),
					zap.Int32("port", r.NodePort),
					zap.Error(err),
					zap.String("raw", resultText),
				)
				r.Rows = nil
			}
		} else {
			// Command failed on worker
			r.Error = resultText
			f.logger.Warn("fanout command failed on worker",
				zap.String("node", r.NodeName),
				zap.Int32("port", r.NodePort),
				zap.String("error", resultText),
			)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate results: %w", err)
	}
	return results, nil
}

// OnAllNodes = OnWorkers + a local query on the coordinator. Coordinator
// NodeResult has NodeName="coordinator", NodePort=0.
func (f *Fanout) OnAllNodes(ctx context.Context, sql string) ([]NodeResult, error) {
	// Execute on coordinator first
	coordRows, err := f.coordinator.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("coordinator query: %w", err)
	}
	defer coordRows.Close()

	// Collect coordinator results
	coordResult := NodeResult{
		NodeName: "coordinator",
		NodePort: 0,
		Success:  true,
		Rows:     make([]map[string]any, 0),
	}

	for coordRows.Next() {
		rowMap := make(map[string]any)
		vals, err := coordRows.Values()
		if err != nil {
			coordResult.Success = false
			coordResult.Error = fmt.Sprintf("scan coordinator: %v", err)
			break
		}
		fields := coordRows.FieldDescriptions()
		for i, val := range vals {
			rowMap[string(fields[i].Name)] = val
		}
		coordResult.Rows = append(coordResult.Rows, rowMap)
	}
	if err := coordRows.Err(); err != nil {
		coordResult.Success = false
		coordResult.Error = fmt.Sprintf("coordinator query: %v", err)
	}

	// Execute on workers
	workerResults, err := f.OnWorkers(ctx, sql)
	if err != nil {
		// Workers failed but coordinator succeeded - return both
		f.logger.Warn("fanout to workers failed", zap.Error(err))
		return append([]NodeResult{coordResult}, NodeResult{
			NodeName: "<workers>",
			Success:  false,
			Error:    fmt.Sprintf("fanout failed: %v", err),
		}), nil
	}

	// Combine coordinator + worker results
	return append([]NodeResult{coordResult}, workerResults...), nil
}

// QuoteLiteral wraps a value as a SQL string literal using PG's quote_literal semantics.
// Escapes single quotes by doubling them.
func QuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// QuoteIdent quotes an identifier (table name, column name, etc).
// Uses double quotes and escapes internal double quotes by doubling them.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Int extracts an integer field from the row map, handling JSON's float64 encoding.
func (r NodeResult) Int(key string) (int64, bool) {
	if len(r.Rows) == 0 {
		return 0, false
	}
	val, ok := r.Rows[0][key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// String extracts a string field from the row map.
func (r NodeResult) String(key string) (string, bool) {
	if len(r.Rows) == 0 {
		return "", false
	}
	val, ok := r.Rows[0][key]
	if !ok {
		return "", false
	}
	switch v := val.(type) {
	case string:
		return v, true
	case fmt.Stringer:
		return v.String(), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

// FirstRow returns the first row if present, or nil.
func (r NodeResult) FirstRow() map[string]any {
	if len(r.Rows) > 0 {
		return r.Rows[0]
	}
	return nil
}
