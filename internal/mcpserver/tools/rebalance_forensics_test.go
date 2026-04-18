// citus-mcp: unit tests for rebalance forensics pure helpers.

package tools

import (
	"strings"
	"testing"
)

func TestTruncateMsg(t *testing.T) {
	if got := truncateMsg("hello", 10); got != "hello" {
		t.Errorf("no-truncate: got %q", got)
	}
	if got := truncateMsg("hello world", 5); got != "hello…" {
		t.Errorf("truncate: got %q", got)
	}
}

func TestBuildRebalancePlaybook(t *testing.T) {
	cases := []struct {
		class       string
		mustContain string
	}{
		{"blocked_by_ddl", "citus_rebalance_stop"},
		{"blocked_by_lock", "pg_cancel_backend"},
		{"bg_worker_starvation", "max_background_task_executors"},
		{"error_with_retries_exhausted", "citus_cleanup_orphaned_resources"},
		{"cleanup_backlog", "citus_cleanup_orphaned_resources"},
		{"retry_backoff", "not_before"},
		{"no_stall", "healthy"},
	}
	for _, c := range cases {
		t.Run(c.class, func(t *testing.T) {
			out := &RebalanceForensicsOutput{Playbook: []string{}, Recommendations: []string{}}
			out.Stall.Classification = c.class
			buildRebalancePlaybook(out)
			joined := strings.Join(out.Playbook, "\n")
			if !strings.Contains(joined, c.mustContain) {
				t.Errorf("class=%s: playbook missing %q; got:\n%s", c.class, c.mustContain, joined)
			}
		})
	}
}
