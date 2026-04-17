// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Shared helper functions for tools.

package tools

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// readGucInt reads a GUC as an integer. Returns 0 on error.
func readGucInt(ctx context.Context, conn *pgx.Conn, name string) int64 {
	var s string
	if err := conn.QueryRow(ctx, `SELECT setting FROM pg_settings WHERE name=$1`, name).Scan(&s); err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
