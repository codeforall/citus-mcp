# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **BREAKING**: Default connection mode is now coordinator-only. Worker data is fetched via `run_command_on_workers()` UDF instead of direct connections. This matches production Citus deployments where only the coordinator is exposed.
- Added `coordinator_only` config flag (default: `true`) to control connection behavior.
- `worker_dsns` is now a dev/test override only. When `coordinator_only=true` and `worker_dsns` is provided, a warning is logged but the override is honored.
- Refactored `citus_mx_readiness`, `citus_extension_drift_scanner`, and `citus_worker_memcontexts` tools to use coordinator-based fanout.
- Memory context probing on workers now runs on fresh backends via `run_command_on_workers`, which changes semantics: measurements reflect a fresh connection rather than cached state.

### Added
- New `internal/db/fanout.go` package for coordinator-based worker queries via `run_command_on_workers()`.
- Helper functions `db.QuoteLiteral()` and `db.QuoteIdent()` for safe SQL literal/identifier quoting.
- Initial release of citus-mcp
- MCP server with stdio, SSE, and streamable HTTP transports
- **Cluster Inspection Tools**
  - `ping` - Health check
  - `server_info` - Server metadata with version/build info
  - `list_nodes` - List coordinator and worker nodes
  - `list_distributed_tables` - List distributed tables
  - `list_shards` - List shards with placements
  - `citus_cluster_summary` - Full cluster overview with configuration health
  - `citus_list_distributed_tables` - Paginated distributed table listing
  - `citus_list_reference_tables` - Paginated reference table listing
  - `citus_table_inspector` - Deep table metadata inspection
  - `citus_colocation_inspector` - Colocation group analysis
- **Monitoring Tools**
  - `citus_activity` - Cluster-wide query monitoring
  - `citus_lock_inspector` - Lock wait analysis
  - `citus_job_inspector` - Background job monitoring
  - `citus_shard_heatmap` - Hot shard detection
  - `citus_shard_skew_report` - Data distribution analysis
  - `citus_explain_query` - Distributed query plans
- **Advisor Tools**
  - `citus_advisor` - SRE and performance recommendations
  - `citus_config_advisor` - Configuration analysis
  - `citus_snapshot_source_advisor` - Node scaling recommendations
  - `citus_validate_rebalance_prereqs` - Rebalance readiness checks
  - `citus_metadata_health` - Metadata corruption detection
  - `citus_node_prepare_advisor` - Pre-flight node addition checks
- **Execute Tools** (require approval tokens)
  - `citus_rebalance_plan` - Preview rebalance operations
  - `citus_rebalance_execute` - Start cluster rebalance
  - `citus_rebalance_status` - Monitor rebalance progress
  - `citus_move_shard_plan` - Preview shard moves
  - `citus_move_shard_execute` - Execute shard migrations
  - `citus_request_approval_token` - Request time-limited approval tokens
- **Built-in Prompts**
  - `/citus.health_check` - Cluster health checklist
  - `/citus.rebalance_workflow` - Step-by-step rebalance guide
  - `/citus.skew_investigation` - Skew investigation playbook
  - `/citus.ops_triage` - Operational triage workflow
- Security features
  - Read-only mode by default
  - HMAC-based approval tokens for execute operations
  - DSN/password redaction in logs
  - SQL injection prevention via parameterized queries
- Configuration via YAML, environment variables, or CLI flags
- Docker Compose setup for local testing

### Security
- All sensitive data (passwords, tokens) redacted from logs
- Approval tokens are time-limited and action-specific

## [0.1.0] - 2026-01-22

Initial public release.

[Unreleased]: https://github.com/citusdata/citus-mcp/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/citusdata/citus-mcp/releases/tag/v0.1.0
