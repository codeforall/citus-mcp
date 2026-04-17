// citus-mcp: B10 snapshot store -- opt-in local SQLite history.
// Security guardrails: 0700 dir / 0600 file; never logs DSNs or
// parameter values; stores only aggregated counters + catalog
// metadata (no user row data). Path overridable.

package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

// DefaultPath returns the default snapshot DB path in XDG state dir.
func DefaultPath() string {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".local", "state", "citus-mcp")
	return filepath.Join(base, "history.db")
}

// Open opens (and migrates) the snapshot DB at path. Creates parents
// with 0700 and the DB file with 0600.
func Open(path string) (*Store, error) {
	if path == "" {
		path = DefaultPath()
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	s := &Store{db: db, path: path}
	// Re-chmod file after sqlite creates it.
	_ = os.Chmod(path, 0o600)
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	// Chmod again: WAL mode may create/alter the file.
	_ = os.Chmod(path, 0o600)
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }

type Snapshot struct {
	ID          int64     `json:"id"`
	Cluster     string    `json:"cluster"`
	CollectedAt time.Time `json:"collected_at"`
	Kind        string    `json:"kind"`
	Node        string    `json:"node,omitempty"`
	Data        string    `json:"data_json"`
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster TEXT NOT NULL,
  collected_at TIMESTAMP NOT NULL,
  kind TEXT NOT NULL,
  node TEXT,
  data_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snap_kind ON snapshots(cluster, kind, collected_at);
CREATE INDEX IF NOT EXISTS idx_snap_node ON snapshots(cluster, node, kind, collected_at);
`)
	if err != nil {
		return err
	}
	var v int
	_ = s.db.QueryRow("SELECT COALESCE(MAX(version),0) FROM schema_version").Scan(&v)
	if v < 1 {
		_, err = s.db.Exec("INSERT INTO schema_version(version) VALUES(1)")
	}
	return err
}

func (s *Store) Record(ctx context.Context, cluster, kind, node, data string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO snapshots(cluster, collected_at, kind, node, data_json) VALUES(?,?,?,?,?)`,
		cluster, time.Now().UTC(), kind, node, data)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) List(ctx context.Context, cluster, kind string, since time.Time, limit int) ([]Snapshot, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, cluster, collected_at, kind, COALESCE(node,''), data_json
		 FROM snapshots
		 WHERE cluster=? AND kind=? AND collected_at>=?
		 ORDER BY collected_at DESC LIMIT ?`,
		cluster, kind, since.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Snapshot{}
	for rows.Next() {
		var s Snapshot
		if err := rows.Scan(&s.ID, &s.Cluster, &s.CollectedAt, &s.Kind, &s.Node, &s.Data); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (s *Store) Prune(ctx context.Context, retain time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-retain)
	res, err := s.db.ExecContext(ctx, `DELETE FROM snapshots WHERE collected_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
