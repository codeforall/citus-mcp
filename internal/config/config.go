// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Server configuration loading and validation.

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Mode string

const (
	ModeReadOnly Mode = "read_only"
	ModeAdmin    Mode = "admin"
)

type TransportType string

const (
	TransportStdio      TransportType = "stdio"
	TransportSSE        TransportType = "sse"
	TransportStreamable TransportType = "streamable"
)

type Config struct {
	CoordinatorDSN              string   `mapstructure:"coordinator_dsn"`
	CoordinatorUser             string   `mapstructure:"coordinator_user"`
	CoordinatorPassword         string   `mapstructure:"coordinator_password"`
	CoordinatorOnly             bool     `mapstructure:"coordinator_only"`
	WorkerDSNs                  []string `mapstructure:"worker_dsns"`
	ConnectTimeoutSeconds       int      `mapstructure:"connect_timeout_seconds"`
	StatementTimeoutMs          int      `mapstructure:"statement_timeout_ms"`
	AppName                     string   `mapstructure:"app_name"`
	Mode                        Mode     `mapstructure:"mode"`
	AllowExecute                bool     `mapstructure:"allow_execute"`
	ApprovalSecret              string   `mapstructure:"approval_secret"`
	MaxRows                     int      `mapstructure:"max_rows"`
	MaxTextBytes                int      `mapstructure:"max_text_bytes"`
	EnableCaching               bool     `mapstructure:"enable_caching"`
	CacheTTLSeconds             int      `mapstructure:"cache_ttl_seconds"`
	LogLevel                    string   `mapstructure:"log_level"`
	SnapshotAdvisorCollectBytes bool     `mapstructure:"snapshot_advisor_collect_bytes"`
	SnapshotDB                  string   `mapstructure:"snapshot_db"`

	// Transport configuration
	Transport    TransportType `mapstructure:"transport"`
	HTTPAddr     string        `mapstructure:"http_addr"`
	HTTPPort     int           `mapstructure:"http_port"`
	HTTPPath     string        `mapstructure:"http_path"`
	SSEKeepAlive int           `mapstructure:"sse_keepalive_seconds"`
}

func defaults(v *viper.Viper) {
	v.SetDefault("coordinator_dsn", "")
	v.SetDefault("coordinator_user", "")
	v.SetDefault("coordinator_password", "")
	v.SetDefault("coordinator_only", true)
	v.SetDefault("worker_dsns", []string{})
	v.SetDefault("connect_timeout_seconds", 5)
	v.SetDefault("statement_timeout_ms", 30000)
	v.SetDefault("app_name", "citus-mcp")
	v.SetDefault("mode", string(ModeReadOnly))
	v.SetDefault("allow_execute", false)
	v.SetDefault("approval_secret", "")
	v.SetDefault("max_rows", 200)
	v.SetDefault("max_text_bytes", 200000)
	v.SetDefault("enable_caching", true)
	v.SetDefault("cache_ttl_seconds", 5)
	v.SetDefault("log_level", "info")
	v.SetDefault("snapshot_advisor_collect_bytes", true)
	v.SetDefault("snapshot_db", "")

	// Transport defaults
	v.SetDefault("transport", string(TransportStdio))
	v.SetDefault("http_addr", "127.0.0.1")
	v.SetDefault("http_port", 8080)
	v.SetDefault("http_path", "/mcp")
	v.SetDefault("sse_keepalive_seconds", 30)
}

func Load() (Config, error) {
	v := viper.New()
	defaults(v)
	v.SetEnvPrefix("CITUS_MCP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Flags override (parse early to locate config file)
	fs := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	var cfgPathFlag string
	fs.StringVarP(&cfgPathFlag, "config", "c", "", "Config file path (yaml|json|toml)")
	fs.String("coordinator_dsn", "", "Coordinator DSN (postgres://…)")
	fs.String("dsn", "", "Coordinator DSN (alias for coordinator_dsn)")
	fs.String("coordinator_user", "", "Coordinator user (optional override)")
	fs.String("coordinator_password", "", "Coordinator password (optional override)")
	fs.Bool("coordinator_only", true, "Connect only to coordinator (use run_command_on_workers for worker queries)")
	fs.StringSlice("worker_dsns", []string{}, "Worker DSNs (dev/test override, repeatable)")
	fs.Int("connect_timeout_seconds", 5, "Connection timeout in seconds")
	fs.Int("statement_timeout_ms", 30000, "Statement timeout in milliseconds")
	fs.String("app_name", "citus-mcp", "Application name")
	fs.String("mode", string(ModeReadOnly), "Mode: read_only|admin")
	fs.Bool("allow_execute", false, "Allow execute tools")
	fs.String("approval_secret", "", "Approval secret (required if allow_execute)")
	fs.Int("max_rows", 200, "Maximum rows returned by tools")
	fs.Int("max_text_bytes", 200000, "Maximum text bytes returned by tools")
	fs.Bool("enable_caching", true, "Enable caching")
	fs.Int("cache_ttl_seconds", 5, "Cache TTL in seconds")
	fs.String("log_level", "info", "Log level")
	fs.String("snapshot_db", "", "Opt-in local SQLite snapshot store path (e.g., ~/.local/state/citus-mcp/history.db). Empty = disabled.")
	fs.Bool("snapshot_advisor_collect_bytes", true, "Collect bytes for snapshot advisor (may be heavy)")

	// Transport flags
	fs.String("transport", string(TransportStdio), "Transport type: stdio|sse|streamable")
	fs.String("http_addr", "127.0.0.1", "HTTP listen address (for sse/streamable)")
	fs.Int("http_port", 8080, "HTTP listen port (for sse/streamable)")
	fs.String("http_path", "/mcp", "HTTP endpoint path (for sse/streamable)")
	fs.Int("sse_keepalive_seconds", 30, "SSE keepalive interval in seconds")

	// pflag -> std flag compatibility
	_ = fs.Parse(os.Args[1:])

	// Config file resolution
	cfgPath := cfgPathFlag
	if cfgPath == "" {
		cfgPath = os.Getenv("CITUS_MCP_CONFIG")
	}
	if cfgPath != "" {
		if err := readConfigFile(v, cfgPath); err != nil {
			return Config{}, err
		}
	} else {
		_ = readDefaultConfig(v) // best-effort
	}

	// Flags override config
	_ = v.BindPFlags(fs)

	// positional DSN fallback
	if v.GetString("coordinator_dsn") == "" {
		if dsn := v.GetString("dsn"); dsn != "" {
			v.Set("coordinator_dsn", dsn)
		} else if args := fs.Args(); len(args) > 0 && args[0] != "" {
			v.Set("coordinator_dsn", args[0])
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	// Warn if coordinator_only is true but worker_dsns are provided (dev override)
	if cfg.CoordinatorOnly && len(cfg.WorkerDSNs) > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: coordinator_only=true but worker_dsns provided; honoring worker_dsns (dev override)\n")
	}
	return cfg, nil
}

func validate(cfg Config) error {
	if cfg.CoordinatorDSN == "" {
		return errors.New("config: coordinator_dsn is required")
	}
	if cfg.Mode != ModeReadOnly && cfg.Mode != ModeAdmin {
		return fmt.Errorf("config: mode must be one of [%s,%s]", ModeReadOnly, ModeAdmin)
	}
	if cfg.AllowExecute && cfg.ApprovalSecret == "" {
		return errors.New("config: approval_secret is required when allow_execute=true")
	}
	if cfg.ConnectTimeoutSeconds <= 0 {
		return errors.New("config: connect_timeout_seconds must be > 0")
	}
	if cfg.StatementTimeoutMs <= 0 {
		return errors.New("config: statement_timeout_ms must be > 0")
	}
	if cfg.MaxRows <= 0 {
		return errors.New("config: max_rows must be > 0")
	}
	if cfg.MaxTextBytes <= 0 {
		return errors.New("config: max_text_bytes must be > 0")
	}
	// Validate transport
	switch cfg.Transport {
	case TransportStdio, TransportSSE, TransportStreamable:
		// valid
	default:
		return fmt.Errorf("config: transport must be one of [stdio, sse, streamable]")
	}
	if cfg.Transport != TransportStdio {
		if cfg.HTTPPort <= 0 || cfg.HTTPPort > 65535 {
			return errors.New("config: http_port must be between 1 and 65535")
		}
	}
	return nil
}

func readConfigFile(v *viper.Viper, path string) error {
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	return nil
}

func readDefaultConfig(v *viper.Viper) error {
	paths := defaultConfigCandidates()
	exts := []string{"yaml", "yml", "json", "toml"}
	for _, base := range paths {
		for _, ext := range exts {
			candidate := base + "." + ext
			if _, err := os.Stat(candidate); err == nil {
				v.SetConfigFile(candidate)
				if err := v.ReadInConfig(); err != nil {
					return fmt.Errorf("read default config %s: %w", candidate, err)
				}
				return nil
			}
		}
	}
	return nil
}

func defaultConfigCandidates() []string {
	var out []string
	cwd, _ := os.Getwd()
	if cwd != "" {
		out = append(out,
			filepath.Join(cwd, "citus-mcp"),
			filepath.Join(cwd, "config", "citus-mcp"),
		)
	}
	xdg := os.Getenv("XDG_CONFIG_HOME")
	if xdg == "" {
		home, _ := os.UserHomeDir()
		if home != "" {
			xdg = filepath.Join(home, ".config")
		}
	}
	if xdg != "" {
		out = append(out, filepath.Join(xdg, "citus-mcp", "config"))
	}
	return out
}
