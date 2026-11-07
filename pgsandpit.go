// Package pgsandpit gives each test a PostgreSQL session wrapped in a
// transaction that is rolled back when the test ends, and hands the test
// *sql.DB values that run on that session.
//
//	var sandpit = pgsandpit.New("postgres://postgres@localhost:5432/postgres")
//
//	func TestFoo(t *testing.T) {
//		t.Parallel()
//		s := sandpit.Session(t)
//		db := s.DB()
//		ro := s.With().ReadOnly().DB()
//		// db and ro share the session: ro sees what db wrote, and nothing is
//		// left in the database after the test.
//	}
package pgsandpit

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
)

// DefaultTimeout is how long a call waits for the session while another
// transaction holds it, unless the *sql.DB was built with Builder.Timeout.
const DefaultTimeout = 10 * time.Second

// cancelDeadlineDelay is how long pgx waits for a cancel request to take effect
// before it gives up on the connection. A canceled query normally returns well
// within it, so the session survives the cancellation.
const cancelDeadlineDelay = 30 * time.Second

// Sandpit opens sessions on one database. Create it once per test package.
type Sandpit struct {
	dsn  string
	cfg  *pgx.ConnConfig
	opts []stdlib.OptionOpenDB

	once sync.Once
	db   *sql.DB
	err  error
}

// New returns a Sandpit for the database dsn points to. It does not connect
// until the first Session call.
func New(dsn string) *Sandpit {
	return &Sandpit{dsn: dsn}
}

// NewWithConfig returns a Sandpit that connects with a copy of cfg and opts,
// as stdlib.OpenDB does. It does not connect until the first Session call.
//
// pgsandpit replaces cfg.BuildContextWatcherHandler, so that a canceled context
// cancels the query instead of closing the session's connection.
//
// The connections are pooled across sessions: a hook such as
// stdlib.OptionAfterConnect runs once per connection, not once per session.
func NewWithConfig(cfg *pgx.ConnConfig, opts ...stdlib.OptionOpenDB) *Sandpit {
	return &Sandpit{cfg: cfg.Copy(), opts: opts}
}

func (sp *Sandpit) pool() (*sql.DB, error) {
	sp.once.Do(func() {
		cfg := sp.cfg
		if cfg == nil {
			var err error
			cfg, err = pgx.ParseConfig(sp.dsn)
			if err != nil {
				sp.err = err
				return
			}
		}
		// pgx's default closes the connection when a context is canceled, which
		// would end the whole session. Ask the server to cancel the query
		// instead, so the connection and the transaction stay usable.
		cfg.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
			return &pgconn.CancelRequestContextWatcherHandler{
				Conn:          pgConn,
				DeadlineDelay: cancelDeadlineDelay,
			}
		}
		sp.db = sql.OpenDB(stdlib.GetConnector(*cfg, sp.opts...))
	})
	return sp.db, sp.err
}

// Session opens a connection, begins a transaction on it, and returns a
// Session for it. The transaction is rolled back and the connection closed when
// t and its subtests finish. Session fails t if it cannot connect.
func (sp *Sandpit) Session(t *testing.T) *Session {
	t.Helper()
	pool, err := sp.pool()
	if err != nil {
		t.Fatalf("pgsandpit: %s", err)
	}
	ctx := context.Background()
	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatalf("pgsandpit: failed to connect: %s", err)
	}
	// The transaction must not end with a test's context, so it gets its own.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("pgsandpit: failed to begin the session transaction: %s", err)
	}
	s := &Session{
		t:    t,
		conn: conn,
		tx:   tx,
		sem:  make(chan struct{}, 1),
	}
	t.Cleanup(s.close)
	return s
}

// Close closes the connection pool the sessions are taken from. Call it from
// TestMain after the tests have run, if at all.
func (sp *Sandpit) Close() error {
	if sp.db == nil {
		return nil
	}
	return sp.db.Close()
}
