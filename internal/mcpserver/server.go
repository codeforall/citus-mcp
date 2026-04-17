// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// MCP server initialization and lifecycle management.

package mcpserver

import (
	"context"
	"time"

	"citus-mcp/internal/cache"
	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	"citus-mcp/internal/diagnostics"
	"citus-mcp/internal/mcpserver/prompts"
	"citus-mcp/internal/mcpserver/resources"
	"citus-mcp/internal/mcpserver/tools"
	"citus-mcp/internal/safety"
	"citus-mcp/internal/snapshot"
	"citus-mcp/internal/version"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

const serverName = "citus-mcp"

type Server struct {
	cfg        config.Config
	logger     *zap.Logger
	pool       *pgxpool.Pool
	guardrails *safety.Guardrails
	deps       tools.Dependencies
	srv        *mcp.Server
}

// New builds the server with all dependencies and returns a wrapper Server.
func New(cfg config.Config, logger *zap.Logger) (*Server, error) {
	ctx := context.Background()
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		return nil, err
	}
	guard := safety.NewGuardrails(cfg)
	wm := db.NewWorkerManager(cfg, pool, logger)
	caps, err := db.DetectCapabilities(ctx, pool)
	if err != nil {
		logger.Warn("capability detection failed", zap.Error(err))
	}
	cacheTTL := time.Duration(cfg.CacheTTLSeconds) * time.Second
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Second
	}
	cacheInstance := cache.New()
	alarmSink := diagnostics.NewSink(0)

	var snapStore *snapshot.Store
	if cfg.SnapshotDB != "" {
		s, err := snapshot.Open(cfg.SnapshotDB)
		if err != nil {
			logger.Warn("snapshot store disabled (open failed)", zap.Error(err), zap.String("path", cfg.SnapshotDB))
		} else {
			snapStore = s
			logger.Info("snapshot store enabled", zap.String("path", s.Path()))
		}
	}

	impl := &mcp.Implementation{Name: serverName, Version: version.Version}
	m := mcp.NewServer(impl, nil)
	fanout := db.NewFanout(pool, logger)
	deps := tools.Dependencies{
		Pool:          pool,
		Logger:        logger,
		Guardrails:    guard,
		Config:        cfg,
		WorkerManager: wm,
		Capabilities:  caps,
		Cache:         cacheInstance,
		Alarms:        alarmSink,
		Snapshot:      snapStore,
		Fanout:        fanout,
	}
	tools.RegisterAll(m, deps)
	resources.RegisterAll(m, deps)
	prompts.RegisterAll(m, deps)
	prompts.RegisterRunbooks(m, deps)

	return &Server{cfg: cfg, logger: logger, pool: pool, guardrails: guard, deps: deps, srv: m}, nil
}

// MCP returns the underlying mcp.Server
func (s *Server) MCP() *mcp.Server { return s.srv }

// Close cleans up resources.
func (s *Server) Close() { s.pool.Close() }

// Run runs the server with the provided transport (e.g., &mcp.StdioTransport{}).
func (s *Server) Run(ctx context.Context, transport mcp.Transport) error {
	return s.srv.Run(ctx, transport)
}
