// citus-mcp: citus_rebalance_forensics
//
// Diagnoses WHY a rebalance (or any background job) is stuck, rather than
// just reporting progress like citus_rebalance_status. Answers the
// "nothing is moving — what's going on?" question with actionable
// classification and a resume/abort/cleanup playbook.
//
// Addresses citusdata/citus issues: #6681, #7103, #8236, #1210, #5284, #5286.
//
// Data sources (all read-only):
//   - pg_dist_background_job            (state machine, timing)
//   - pg_dist_background_task           (per-task status + pid + message + retry)
//   - pg_dist_background_task_depend    (inter-task dependency DAG)
//   - pg_dist_cleanup                   (pending cleanup backlog after failed moves)
//   - pg_stat_activity on coord         (wait events for running tasks, because
//                                        rebalance bg workers live on the coord)
//   - pg_blocking_pids(pid)             (who holds the lock a stuck task waits for)
//   - citus.max_background_task_executors{,_per_node}  (for starvation diagnosis)
//
// Stall classifications emitted per job:
//   - no_stall                  — job is healthy, tasks are advancing
//   - blocked_by_lock           — running task's pid is blocked by a non-rebalance txn
//   - blocked_by_ddl            — lock blocker is a DDL statement (AccessExclusive)
//   - bg_worker_starvation      — runnable tasks > configured executor count
//   - retry_backoff             — tasks in retry backoff (not_before > now())
//   - error_with_retries_exhausted — task errored and retry_count >= threshold
//   - cleanup_backlog           — many pg_dist_cleanup records waiting
//   - finished_with_errors      — job finished but some tasks failed
//   - unknown                   — data collected but no known pattern matched
//
// The tool never modifies state; it emits diagnostics, alarms, and a
// playbook (commit-prepared/rollback/cancel/resume SQL fragments).

package tools

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/diagnostics"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type RebalanceForensicsInput struct {
	// Specific job_id to inspect. If 0, inspect the most recent job in
	// states {running, cancelling, failing, scheduled, failed} within
	// lookback_hours.
	JobID int64 `json:"job_id,omitempty"`
	// Running tasks older than this are flagged as "running_too_long".
	// Default: 300 seconds.
	StallThresholdSeconds int `json:"stall_threshold_seconds,omitempty"`
	// How far back to search when JobID is unspecified. Default: 24 hours.
	LookbackHours int `json:"lookback_hours,omitempty"`
	// Include done / cancelled / finished tasks in task_details (default false).
	IncludeFinishedTasks bool `json:"include_finished_tasks,omitempty"`
	// Retry count threshold for "retries_exhausted" classification. Default: 3.
	RetriesExhaustedThreshold int `json:"retries_exhausted_threshold,omitempty"`
	// Cleanup backlog threshold for "cleanup_backlog" classification. Default: 100.
	CleanupBacklogThreshold int `json:"cleanup_backlog_threshold,omitempty"`
}

type RebalanceJobSummary struct {
	JobID       int64   `json:"job_id"`
	State       string  `json:"state"`
	JobType     string  `json:"job_type"`
	Description string  `json:"description"`
	StartedAt   *string `json:"started_at,omitempty"`
	FinishedAt  *string `json:"finished_at,omitempty"`
	DurationSec float64 `json:"duration_sec"`
}

type TaskCounts struct {
	Total       int `json:"total"`
	Blocked     int `json:"blocked"`
	Runnable    int `json:"runnable"`
	Running     int `json:"running"`
	Cancelling  int `json:"cancelling"`
	Done        int `json:"done"`
	Error       int `json:"error"`
	Unscheduled int `json:"unscheduled"`
	Cancelled   int `json:"cancelled"`
}

type RebalanceTaskDetail struct {
	TaskID         int64   `json:"task_id"`
	Status         string  `json:"status"`
	PID            *int32  `json:"pid,omitempty"`
	RetryCount     int     `json:"retry_count"`
	NotBefore      *string `json:"not_before,omitempty"`
	Message        string  `json:"message,omitempty"`
	Command        string  `json:"command,omitempty"`
	AgeSeconds     float64 `json:"age_seconds,omitempty"`
	// When the task is running, these are populated from pg_stat_activity
	// on the coordinator.
	WaitEventType  string  `json:"wait_event_type,omitempty"`
	WaitEvent      string  `json:"wait_event,omitempty"`
	BlockerPIDs    []int32 `json:"blocker_pids,omitempty"`
	BlockerQueries []string `json:"blocker_queries,omitempty"`
	BlockerAccess  string  `json:"blocker_access,omitempty"` // e.g. "AccessExclusiveLock on public.t"
	// Summary classification for *this* task.
	TaskClassification string `json:"task_classification,omitempty"`
}

type RebalanceForensicsOutput struct {
	Job                 *RebalanceJobSummary  `json:"job,omitempty"`
	TaskCounts          TaskCounts            `json:"task_counts"`
	TaskDetails         []RebalanceTaskDetail `json:"task_details"`
	CleanupPendingCount int                   `json:"cleanup_pending_count"`
	CleanupOldestAgeSec float64               `json:"cleanup_oldest_age_sec,omitempty"`
	MaxBackgroundExec   int                   `json:"max_background_task_executors"`
	Stall               struct {
		Classification    string   `json:"classification"`
		PrimaryReason     string   `json:"primary_reason,omitempty"`
		ContributingCauses []string `json:"contributing_causes,omitempty"`
	} `json:"stall"`
	Playbook        []string            `json:"playbook"`
	Findings        []string            `json:"findings"`
	Recommendations []string            `json:"recommendations"`
	Alarms          []diagnostics.Alarm `json:"alarms"`
	OverallStatus   string              `json:"overall_status"` // ok | warning | critical
}

func RebalanceForensicsTool(ctx context.Context, deps Dependencies, in RebalanceForensicsInput) (*mcp.CallToolResult, RebalanceForensicsOutput, error) {
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), RebalanceForensicsOutput{}, nil
	}
	if in.StallThresholdSeconds <= 0 {
		in.StallThresholdSeconds = 300
	}
	if in.LookbackHours <= 0 {
		in.LookbackHours = 24
	}
	if in.RetriesExhaustedThreshold <= 0 {
		in.RetriesExhaustedThreshold = 3
	}
	if in.CleanupBacklogThreshold <= 0 {
		in.CleanupBacklogThreshold = 100
	}

	out := RebalanceForensicsOutput{
		TaskDetails:     []RebalanceTaskDetail{},
		Playbook:        []string{},
		Findings:        []string{},
		Recommendations: []string{},
		Alarms:          []diagnostics.Alarm{},
		OverallStatus:   "ok",
	}
	out.Stall.Classification = "no_stall"

	// -------- 1. Resolve job_id (if not supplied, pick the most recent
	//             active job, or the most recent one in lookback window).
	jobID := in.JobID
	if jobID == 0 {
		// Prefer active jobs; else most recent finished/failed within lookback.
		err := deps.Pool.QueryRow(ctx, `
			SELECT job_id FROM pg_catalog.pg_dist_background_job
			WHERE started_at > now() - make_interval(hours => $1)
			   OR state IN ('scheduled','running','cancelling','failing')
			ORDER BY CASE WHEN state IN ('running','cancelling','failing','scheduled')
			              THEN 0 ELSE 1 END,
			         COALESCE(started_at, to_timestamp(0)) DESC,
			         job_id DESC
			LIMIT 1`, in.LookbackHours).Scan(&jobID)
		if err != nil {
			out.Findings = append(out.Findings,
				fmt.Sprintf("No background job found in the last %dh; nothing to diagnose.", in.LookbackHours))
			return nil, out, nil
		}
	}

	// -------- 2. Job summary.
	{
		job := RebalanceJobSummary{JobID: jobID}
		row := deps.Pool.QueryRow(ctx, `
			SELECT state::text, job_type::text, description,
			       to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			       to_char(finished_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			       EXTRACT(EPOCH FROM (COALESCE(finished_at, now()) - COALESCE(started_at, now())))::float8
			FROM pg_catalog.pg_dist_background_job
			WHERE job_id = $1`, jobID)
		var started, finished *string
		if err := row.Scan(&job.State, &job.JobType, &job.Description, &started, &finished, &job.DurationSec); err != nil {
			out.Findings = append(out.Findings, fmt.Sprintf("job_id=%d not found", jobID))
			return nil, out, nil
		}
		job.StartedAt = started
		job.FinishedAt = finished
		out.Job = &job
	}

	// -------- 3. Task counts + details.
	{
		rows, err := deps.Pool.Query(ctx, `
			SELECT task_id, status::text, pid,
			       COALESCE(retry_count, 0),
			       to_char(not_before AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			       COALESCE(message, ''),
			       left(COALESCE(command, ''), 4000)
			FROM pg_catalog.pg_dist_background_task
			WHERE job_id = $1
			ORDER BY task_id`, jobID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var td RebalanceTaskDetail
				var pidV *int32
				var notBefore *string
				if err := rows.Scan(&td.TaskID, &td.Status, &pidV, &td.RetryCount, &notBefore, &td.Message, &td.Command); err != nil {
					continue
				}
				td.PID = pidV
				td.NotBefore = notBefore
				out.TaskCounts.Total++
				switch td.Status {
				case "blocked":
					out.TaskCounts.Blocked++
				case "runnable":
					out.TaskCounts.Runnable++
				case "running":
					out.TaskCounts.Running++
				case "cancelling":
					out.TaskCounts.Cancelling++
				case "done":
					out.TaskCounts.Done++
				case "error":
					out.TaskCounts.Error++
				case "unscheduled":
					out.TaskCounts.Unscheduled++
				case "cancelled":
					out.TaskCounts.Cancelled++
				}
				// Always include non-finished; include finished only if requested.
				if in.IncludeFinishedTasks ||
					(td.Status != "done" && td.Status != "cancelled") {
					out.TaskDetails = append(out.TaskDetails, td)
				}
			}
		}
	}

	// -------- 4. For running tasks, fetch wait-event + blocker info via
	//             pg_stat_activity + pg_blocking_pids.
	for i := range out.TaskDetails {
		td := &out.TaskDetails[i]
		if td.Status != "running" || td.PID == nil {
			continue
		}
		// Task runtime (we don't have a started_at per-task in 11.x, so use
		// xact_start from pg_stat_activity as a proxy).
		var waitEventType, waitEvent *string
		var ageSec *float64
		err := deps.Pool.QueryRow(ctx, `
			SELECT wait_event_type, wait_event,
			       EXTRACT(EPOCH FROM (now() - COALESCE(xact_start, query_start, backend_start)))::float8
			FROM pg_catalog.pg_stat_activity WHERE pid = $1`, *td.PID).
			Scan(&waitEventType, &waitEvent, &ageSec)
		if err == nil {
			if waitEventType != nil {
				td.WaitEventType = *waitEventType
			}
			if waitEvent != nil {
				td.WaitEvent = *waitEvent
			}
			if ageSec != nil {
				td.AgeSeconds = *ageSec
			}
		}
		// If on a lock, get blockers + their current queries.
		if td.WaitEventType == "Lock" {
			blkRows, err := deps.Pool.Query(ctx, `
				SELECT bp.pid, COALESCE(left(bsa.query, 2000), '')
				FROM unnest(pg_catalog.pg_blocking_pids($1)) AS bp(pid)
				LEFT JOIN pg_catalog.pg_stat_activity bsa ON bsa.pid = bp.pid`, *td.PID)
			if err == nil {
				for blkRows.Next() {
					var blkPID int32
					var blkQuery string
					if err := blkRows.Scan(&blkPID, &blkQuery); err != nil {
						continue
					}
					td.BlockerPIDs = append(td.BlockerPIDs, blkPID)
					td.BlockerQueries = append(td.BlockerQueries, blkQuery)
				}
				blkRows.Close()
			}
			// Get a concise "AccessExclusiveLock on schema.table" description
			// of one of the locks we're waiting for.
			_ = deps.Pool.QueryRow(ctx, `
				SELECT mode || ' on ' ||
				       COALESCE(relation::regclass::text, locktype)
				FROM pg_catalog.pg_locks
				WHERE pid = $1 AND NOT granted
				ORDER BY (mode = 'AccessExclusiveLock') DESC, locktype
				LIMIT 1`, *td.PID).Scan(&td.BlockerAccess)
		}

		// Per-task classification.
		if td.AgeSeconds > float64(in.StallThresholdSeconds) {
			if len(td.BlockerPIDs) > 0 {
				if strings.Contains(td.BlockerAccess, "AccessExclusive") {
					td.TaskClassification = "blocked_by_ddl"
				} else {
					td.TaskClassification = "blocked_by_lock"
				}
			} else {
				td.TaskClassification = "running_too_long"
			}
		}
	}

	// -------- 5. pg_dist_cleanup backlog. (No timestamp column exists on
	//             pg_dist_cleanup, so we only return count.)
	{
		_ = deps.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_catalog.pg_dist_cleanup`).
			Scan(&out.CleanupPendingCount)
	}

	// -------- 6. Background-executor cap (for starvation diagnosis).
	{
		_ = deps.Pool.QueryRow(ctx, `
			SELECT COALESCE(NULLIF(current_setting('citus.max_background_task_executors', true), ''), '4')::int`).
			Scan(&out.MaxBackgroundExec)
	}

	// -------- 7. Stall classification.
	reasons := []string{}
	primary := ""

	// Priority 1: running tasks stuck on locks.
	for _, td := range out.TaskDetails {
		if td.TaskClassification == "blocked_by_ddl" {
			primary = "blocked_by_ddl"
			reasons = append(reasons, fmt.Sprintf(
				"task %d blocked by %s held by pid %v (DDL running against a table the rebalancer is moving; see #1210)",
				td.TaskID, td.BlockerAccess, td.BlockerPIDs))
			break
		}
	}
	if primary == "" {
		for _, td := range out.TaskDetails {
			if td.TaskClassification == "blocked_by_lock" {
				primary = "blocked_by_lock"
				reasons = append(reasons, fmt.Sprintf(
					"task %d waiting on lock (blockers: %v); see blocker_queries to identify the culprit",
					td.TaskID, td.BlockerPIDs))
				break
			}
		}
	}

	// Priority 2: bg-worker starvation — runnable tasks exist but none running
	// and fewer executors than runnable tasks.
	if primary == "" && out.TaskCounts.Runnable > 0 && out.TaskCounts.Running == 0 &&
		out.Job != nil && (out.Job.State == "running" || out.Job.State == "scheduled") {
		// Check if all runnables are in not_before backoff.
		allBackoff := true
		for _, td := range out.TaskDetails {
			if td.Status == "runnable" && td.NotBefore == nil {
				allBackoff = false
				break
			}
		}
		switch {
		case allBackoff && out.TaskCounts.Runnable > 0:
			primary = "retry_backoff"
			reasons = append(reasons, fmt.Sprintf(
				"all %d runnable task(s) have not_before in the future; waiting for retry backoff",
				out.TaskCounts.Runnable))
		case out.TaskCounts.Runnable > out.MaxBackgroundExec:
			primary = "bg_worker_starvation"
			reasons = append(reasons, fmt.Sprintf(
				"%d runnable tasks but citus.max_background_task_executors=%d and 0 running",
				out.TaskCounts.Runnable, out.MaxBackgroundExec))
		default:
			// Could be the maintenance daemon not picking up tasks (see #7103).
			primary = "bg_worker_starvation"
			reasons = append(reasons, fmt.Sprintf(
				"%d runnable tasks, 0 running — maintenance daemon may be absent or not scheduling; see #7103",
				out.TaskCounts.Runnable))
		}
	}

	// Priority 3: errors with retries exhausted.
	if primary == "" && out.TaskCounts.Error > 0 {
		for _, td := range out.TaskDetails {
			if td.Status == "error" && td.RetryCount >= in.RetriesExhaustedThreshold {
				primary = "error_with_retries_exhausted"
				reasons = append(reasons, fmt.Sprintf(
					"task %d: %d retries; last error: %s", td.TaskID, td.RetryCount,
					truncateMsg(td.Message, 200)))
				break
			}
		}
	}

	// Priority 4: cleanup backlog.
	if out.CleanupPendingCount > in.CleanupBacklogThreshold {
		if primary == "" {
			primary = "cleanup_backlog"
		}
		reasons = append(reasons, fmt.Sprintf(
			"%d pending pg_dist_cleanup records (> %d threshold); prior moves left residue that requires SELECT citus_cleanup_orphaned_resources();",
			out.CleanupPendingCount, in.CleanupBacklogThreshold))
	}

	// Priority 5: job finished but with errors.
	if primary == "" && out.Job != nil &&
		(out.Job.State == "failed" || out.Job.State == "failing") {
		primary = "finished_with_errors"
		reasons = append(reasons, fmt.Sprintf(
			"job is in state=%s with %d errored task(s)", out.Job.State, out.TaskCounts.Error))
	}

	if primary == "" {
		// If job is running with active tasks making progress, no stall.
		if out.TaskCounts.Running > 0 {
			out.Stall.Classification = "no_stall"
			out.Stall.PrimaryReason = fmt.Sprintf("%d task(s) currently running", out.TaskCounts.Running)
		} else if out.Job != nil && (out.Job.State == "finished" || out.Job.State == "cancelled") {
			out.Stall.Classification = "no_stall"
			out.Stall.PrimaryReason = fmt.Sprintf("job state=%s", out.Job.State)
		} else {
			out.Stall.Classification = "unknown"
			out.Stall.PrimaryReason = "no known stall pattern matched but job is not making progress"
		}
	} else {
		out.Stall.Classification = primary
		if len(reasons) > 0 {
			out.Stall.PrimaryReason = reasons[0]
			if len(reasons) > 1 {
				out.Stall.ContributingCauses = reasons[1:]
			}
		}
	}

	// -------- 8. Playbook + alarms.
	buildRebalancePlaybook(&out)

	// Findings summary line.
	if out.Job != nil {
		out.Findings = append(out.Findings, fmt.Sprintf(
			"job %d (%s, state=%s) has %d tasks: running=%d runnable=%d blocked=%d error=%d done=%d cancelled=%d",
			out.Job.JobID, out.Job.JobType, out.Job.State, out.TaskCounts.Total,
			out.TaskCounts.Running, out.TaskCounts.Runnable, out.TaskCounts.Blocked,
			out.TaskCounts.Error, out.TaskCounts.Done, out.TaskCounts.Cancelled))
	}

	// Emit alarms based on classification.
	switch out.Stall.Classification {
	case "blocked_by_ddl":
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.stall.blocked_by_ddl", Severity: diagnostics.SeverityCritical,
			Source:  "citus_rebalance_forensics",
			Message: fmt.Sprintf("rebalance job %d blocked by DDL", out.Job.JobID),
			Evidence: map[string]any{
				"job_id": out.Job.JobID, "reasons": reasons, "task_details_count": len(out.TaskDetails)},
			FixHint: "Cancel the blocking DDL transaction, or SELECT citus_rebalance_stop(); and retry later.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.OverallStatus = "critical"
	case "blocked_by_lock":
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.stall.blocked_by_lock", Severity: diagnostics.SeverityWarning,
			Source:  "citus_rebalance_forensics",
			Message: fmt.Sprintf("rebalance job %d waiting on locks", out.Job.JobID),
			Evidence: map[string]any{"job_id": out.Job.JobID, "reasons": reasons},
			FixHint: "Inspect blocker_queries for each task and cancel/commit offending transaction.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.OverallStatus = "warning"
	case "bg_worker_starvation":
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.stall.bg_worker_starvation", Severity: diagnostics.SeverityCritical,
			Source:  "citus_rebalance_forensics",
			Message: fmt.Sprintf("rebalance job %d has %d runnable tasks but none running", out.Job.JobID, out.TaskCounts.Runnable),
			Evidence: map[string]any{
				"job_id":               out.Job.JobID,
				"runnable":             out.TaskCounts.Runnable,
				"max_executors":        out.MaxBackgroundExec,
				"maintenance_daemon":   "check pg_stat_activity for 'Citus Maintenance Daemon'",
			},
			FixHint: "Verify maintenance daemon is running (pg_stat_activity WHERE application_name LIKE '%Maintenance%'); consider raising citus.max_background_task_executors.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.OverallStatus = "critical"
	case "error_with_retries_exhausted":
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.stall.retries_exhausted", Severity: diagnostics.SeverityCritical,
			Source:  "citus_rebalance_forensics",
			Message: fmt.Sprintf("rebalance job %d has tasks with exhausted retries", out.Job.JobID),
			Evidence: map[string]any{"job_id": out.Job.JobID, "reasons": reasons},
			FixHint: "Inspect task.message for root cause; SELECT citus_rebalance_stop(); then citus_cleanup_orphaned_resources();.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.OverallStatus = "critical"
	case "cleanup_backlog":
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.cleanup_backlog", Severity: diagnostics.SeverityWarning,
			Source:  "citus_rebalance_forensics",
			Message: fmt.Sprintf("%d pending cleanup records (prior moves left residue)", out.CleanupPendingCount),
			Evidence: map[string]any{"pending_records": out.CleanupPendingCount},
			FixHint: "SELECT citus_cleanup_orphaned_resources(); (runs on coordinator; approved tool citus_cleanup_orphaned can do it with gating).",
		})
		out.Alarms = append(out.Alarms, *a)
		if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
	case "retry_backoff":
		if out.OverallStatus == "ok" {
			out.OverallStatus = "warning"
		}
	case "finished_with_errors":
		a := deps.Alarms.Emit(diagnostics.Alarm{
			Kind: "rebalance.finished_with_errors", Severity: diagnostics.SeverityCritical,
			Source:  "citus_rebalance_forensics",
			Message: fmt.Sprintf("rebalance job %d finished in state=%s with %d errored task(s)",
				out.Job.JobID, out.Job.State, out.TaskCounts.Error),
			Evidence: map[string]any{"job_id": out.Job.JobID, "errors": out.TaskCounts.Error},
			FixHint: "Inspect failed task.message; cleanup residues with SELECT citus_cleanup_orphaned_resources(); before retrying.",
		})
		out.Alarms = append(out.Alarms, *a)
		out.OverallStatus = "critical"
	}

	return nil, out, nil
}

func buildRebalancePlaybook(out *RebalanceForensicsOutput) {
	switch out.Stall.Classification {
	case "blocked_by_ddl", "blocked_by_lock":
		out.Playbook = append(out.Playbook,
			"1. Identify offending session from task_details[*].blocker_pids / blocker_queries",
			"2. Decide: let DDL finish, or `SELECT pg_cancel_backend(<pid>);` / `pg_terminate_backend(<pid>);`",
			"3. If the lock never resolves, stop the rebalance:  `SELECT citus_rebalance_stop();`",
			"4. After unblocking, restart rebalance with the same options as the original `citus_rebalance_start(...)`")
		out.Recommendations = append(out.Recommendations,
			"Avoid concurrent DDL on distributed tables during rebalance (Citus issue #1210).")
	case "bg_worker_starvation":
		out.Playbook = append(out.Playbook,
			"1. Check maintenance daemon is running: SELECT * FROM pg_stat_activity WHERE application_name ILIKE '%citus%maintenance%';",
			"2. If absent, RESTART the PostgreSQL instance on the coordinator (the maintenance daemon crashed)",
			"3. Increase executor cap: `ALTER SYSTEM SET citus.max_background_task_executors = 8; SELECT pg_reload_conf();`",
			"4. Resume: `SELECT citus_rebalance_start();` is safe because completed tasks are marked done")
		out.Recommendations = append(out.Recommendations,
			fmt.Sprintf("Runnable queue depth (%d) exceeds citus.max_background_task_executors (%d); consider raising.",
				out.TaskCounts.Runnable, out.MaxBackgroundExec))
	case "error_with_retries_exhausted", "finished_with_errors":
		out.Playbook = append(out.Playbook,
			"1. Inspect each errored task's `message` field below for the root cause",
			"2. If residue remains: `SELECT citus_cleanup_orphaned_resources();`",
			"3. Stop the job:  `SELECT citus_rebalance_stop();`",
			"4. Address the root cause, then restart: `SELECT citus_rebalance_start();`")
		out.Recommendations = append(out.Recommendations,
			"After fixing the root cause, confirm no duplicate shards remain (citus issue #8236): SELECT logicalrelid::regclass, shardid, count(*) FROM pg_dist_placement GROUP BY 1,2 HAVING count(*) > shard_replication_factor;")
	case "cleanup_backlog":
		out.Playbook = append(out.Playbook,
			"1. `SELECT citus_cleanup_orphaned_resources();`  -- idempotent, safe",
			"2. Re-check: `SELECT COUNT(*) FROM pg_dist_cleanup;` should be near zero",
			"3. Then proceed with the rebalance")
	case "retry_backoff":
		out.Playbook = append(out.Playbook,
			"Tasks are waiting for their not_before timestamp; allow natural progress, or cancel and retry.")
	case "no_stall":
		out.Playbook = append(out.Playbook,
			"Job is healthy. Use citus_rebalance_status for continuous progress monitoring.")
	}
}

func truncateMsg(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
