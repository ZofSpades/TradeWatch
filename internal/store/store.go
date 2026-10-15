// Package store is TradeWatch's Postgres storage: alerts with their analyst
// lifecycle and audit trail, and the rule versions detectors have loaded.
//
// Writes are idempotent where Kafka redelivers: an alert is keyed by its
// deterministic alert key, and rule-set records by their digests, so
// processing the same record twice changes only delivery bookkeeping.
// Schema changes are goose migrations embedded in the binary.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx", used by goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ErrNotFound is returned when an alert does not exist.
var ErrNotFound = errors.New("store: not found")

// Migrate applies every pending migration to the database at url and
// returns the versions it applied. A Postgres advisory lock serializes
// concurrent callers, so each migration is applied exactly once.
func Migrate(ctx context.Context, url string, log *slog.Logger) ([]int64, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, err
	}
	fsys, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return nil, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, fsys,
		goose.WithSessionLocker(locker), goose.WithSlog(log))
	if err != nil {
		return nil, err
	}
	results, err := p.Up(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	applied := make([]int64, 0, len(results))
	for _, r := range results {
		applied = append(applied, r.Source.Version)
	}
	return applied, nil
}

// Store is a connection pool with TradeWatch's queries.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the database at url. It does not migrate.
func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = queryTracer{}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases every connection.
func (s *Store) Close() { s.pool.Close() }
