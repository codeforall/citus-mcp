// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT

package tools

import "testing"

func TestMedian(t *testing.T) {
	tests := []struct {
		name string
		in   []int64
		want int64
	}{
		{"empty", nil, 0},
		{"one", []int64{5}, 5},
		{"odd", []int64{3, 1, 2}, 2},
		{"even", []int64{4, 1, 2, 3}, 3}, // (2+3)/2 rounded = 3
		{"even-round-up", []int64{1, 2, 4, 5}, 3}, // (2+4)/2 = 3
		{"dupes", []int64{10, 10, 10}, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := median(tc.in)
			if got != tc.want {
				t.Fatalf("median(%v)=%d want %d", tc.in, got, tc.want)
			}
		})
	}
}
