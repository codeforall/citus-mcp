// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Worker node connection management and fanout queries.

package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"citus-mcp/internal/cache"
	"citus-mcp/internal/config"
	dbsql "citus-mcp/internal/db/sql"
	"citus-mcp/internal/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Node struct {
	NodeID   int32  `json:"node_id"`
	NodeName string `json:"node_name"`
	NodePort int32  `json:"node_port"`
	NodeRole string `json:"node_role"`
}

type NodeStatus struct {
	Node
	IsActive         bool `json:"is_active"`
	ShouldHaveShards bool `json:"should_have_shards"`
}

// ListNodes returns nodes (coordinator + workers) without status flags, for compatibility.
func ListNodes(ctx context.Context, pool *pgxpool.Pool) ([]Node, error) {
	rows, err := pool.Query(ctx, dbsql.QueryPgDistNode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.NodeID, &n.NodeName, &n.NodePort, &n.NodeRole); err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

type WorkerInfo struct {
	NodeStatus
	DSN string `json:"dsn"`
}

// WorkerManager discovers workers and maintains worker pools with caching.
type WorkerManager struct {
	cfg         config.Config
	coordinator *pgxpool.Pool
	logger      *zap.Logger
	cache       *cache.Cache
	mu          sync.Mutex
	pools       map[int32]*pgxpool.Pool
	ttl         time.Duration
}

func NewWorkerManager(cfg config.Config, coordinator *pgxpool.Pool, logger *zap.Logger) *WorkerManager {
	ttl := time.Duration(cfg.CacheTTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return WorkerManagerWithCache(cfg, coordinator, logger, cache.New(), ttl)
}

func WorkerManagerWithCache(cfg config.Config, coordinator *pgxpool.Pool, logger *zap.Logger, c *cache.Cache, ttl time.Duration) *WorkerManager {
	return &WorkerManager{cfg: cfg, coordinator: coordinator, logger: logger, cache: c, pools: make(map[int32]*pgxpool.Pool), ttl: ttl}
}

// Topology returns worker infos and ensures pools are ready where possible.
func (m *WorkerManager) Topology(ctx context.Context) ([]WorkerInfo, error) {
	if m.cfg.EnableCaching {
		if val, ok := m.cache.Get("workers"); ok {
			if infos, ok := val.([]WorkerInfo); ok {
				return infos, nil
			}
		}
	}
	nodes, err := listNodesWithStatus(ctx, m.coordinator)
	if err != nil {
		return nil, err
	}
	workers := filterWorkers(nodes)
	if len(workers) == 0 {
		return []WorkerInfo{}, nil
	}
	infos := make([]WorkerInfo, 0, len(workers))
	for _, n := range workers {
		dsn := m.resolveDSN(n)
		info := WorkerInfo{NodeStatus: n, DSN: dsn}
		// create pools eagerly (best-effort)
		if dsn != "" {
			if _, err := m.ensurePool(ctx, n.NodeID, dsn); err != nil {
				m.logger.Warn("worker pool unreachable", logging.FieldDSN("dsn", dsn), zap.Error(err), zap.Int32("node_id", n.NodeID))
			}
		}
		infos = append(infos, info)
	}
	if m.cfg.EnableCaching {
		m.cache.Set("workers", infos, m.ttl)
	}
	return infos, nil
}

// Pools returns the current map of worker pools; may be partial if some are unreachable.
func (m *WorkerManager) Pools(ctx context.Context) (map[int32]*pgxpool.Pool, []WorkerInfo, error) {
	infos, err := m.Topology(ctx)
	if err != nil {
		return nil, nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy to avoid external mutation
	out := make(map[int32]*pgxpool.Pool, len(m.pools))
	for k, v := range m.pools {
		out[k] = v
	}
	return out, infos, nil
}

func (m *WorkerManager) ensurePool(ctx context.Context, nodeID int32, dsn string) (*pgxpool.Pool, error) {
	// First check with read lock
	m.mu.Lock()
	pool, ok := m.pools[nodeID]
	if ok {
		m.mu.Unlock()
		return pool, nil
	}
	// Keep lock held during pool creation to prevent race condition
	// where two goroutines create duplicate pools

	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	pcfg.ConnConfig.ConnectTimeout = time.Duration(m.cfg.ConnectTimeoutSeconds) * time.Second
	if pcfg.ConnConfig.RuntimeParams == nil {
		pcfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	pcfg.ConnConfig.RuntimeParams["application_name"] = m.cfg.AppName
	pcfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", m.cfg.StatementTimeoutMs)

	// Release lock during potentially slow network operations
	m.mu.Unlock()

	pool, err = pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, err
	}

	// health check
	pingCtx, cancel := context.WithTimeout(ctx, time.Duration(m.cfg.ConnectTimeoutSeconds)*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close() // Fix: close pool on ping failure to prevent resource leak
		return nil, err
	}

	// Re-acquire lock and check if another goroutine already created the pool
	m.mu.Lock()
	if existingPool, ok := m.pools[nodeID]; ok {
		// Another goroutine beat us - close our pool and return existing
		m.mu.Unlock()
		pool.Close()
		return existingPool, nil
	}
	m.pools[nodeID] = pool
	m.mu.Unlock()
	return pool, nil
}

func filterWorkers(nodes []NodeStatus) []NodeStatus {
	var workers []NodeStatus
	for _, n := range nodes {
		if n.NodeRole != "primary" { // primary typically coordinator
			workers = append(workers, n)
		}
	}
	if len(workers) == 0 && len(nodes) > 1 {
		// Fallback for clusters where all nodes report 'primary': assume smallest nodeid is coordinator
		minID := nodes[0].NodeID
		for _, n := range nodes {
			if n.NodeID < minID {
				minID = n.NodeID
			}
		}
		for _, n := range nodes {
			if n.NodeID != minID {
				workers = append(workers, n)
			}
		}
	}
	return workers
}

func listNodesWithStatus(ctx context.Context, pool *pgxpool.Pool) ([]NodeStatus, error) {
	rows, err := pool.Query(ctx, dbsql.QueryPgDistNodeStatus)
	if err != nil {
		// fallback if columns missing
		rows2, err2 := pool.Query(ctx, dbsql.QueryPgDistNode)
		if err2 != nil {
			return nil, err
		}
		defer rows2.Close()
		var nodes []NodeStatus
		for rows2.Next() {
			var n NodeStatus
			if err := rows2.Scan(&n.NodeID, &n.NodeName, &n.NodePort, &n.NodeRole); err != nil {
				return nil, err
			}
			n.IsActive = true
			n.ShouldHaveShards = n.NodeRole != "primary"
			nodes = append(nodes, n)
		}
		return nodes, rows2.Err()
	}
	defer rows.Close()
	var nodes []NodeStatus
	for rows.Next() {
		var n NodeStatus
		if err := rows.Scan(&n.NodeID, &n.NodeName, &n.NodePort, &n.NodeRole, &n.IsActive, &n.ShouldHaveShards); err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

// resolveDSN builds a worker DSN from explicit overrides or coordinator credentials.
func (m *WorkerManager) resolveDSN(n NodeStatus) string {
	// If coordinator_only is enabled and no explicit worker_dsns, return empty
	// to prevent direct worker connections
	if m.cfg.CoordinatorOnly && len(m.cfg.WorkerDSNs) == 0 {
		return ""
	}

	if len(m.cfg.WorkerDSNs) > 0 {
		// best-effort: match by index when counts align
		// if mismatch, fallback to derive
		if len(m.cfg.WorkerDSNs) == 1 {
			return m.cfg.WorkerDSNs[0]
		}
		if int(n.NodeID) <= len(m.cfg.WorkerDSNs) && n.NodeID > 0 {
			return m.cfg.WorkerDSNs[n.NodeID-1]
		}
	}
	// derive from coordinator DSN
	ccfg, err := pgx.ParseConfig(m.cfg.CoordinatorDSN)
	if err != nil {
		m.logger.Warn("cannot parse coordinator dsn to derive worker dsn", zap.Error(err))
		return ""
	}
	if m.cfg.CoordinatorUser != "" {
		ccfg.User = m.cfg.CoordinatorUser
	}
	if m.cfg.CoordinatorPassword != "" {
		ccfg.Password = m.cfg.CoordinatorPassword
	}
	ccfg.Host = n.NodeName
	ccfg.Port = uint16(n.NodePort)
	if ccfg.RuntimeParams == nil {
		ccfg.RuntimeParams = map[string]string{}
	}
	ccfg.RuntimeParams["application_name"] = m.cfg.AppName
	ccfg.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", m.cfg.StatementTimeoutMs)
	return ccfg.ConnString()
}
