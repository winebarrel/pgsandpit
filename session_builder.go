package pgsandpit

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// SessionBuilder builds a Session for a Sandpit. Each method returns a new
// SessionBuilder, so a SessionBuilder can be shared and branched, and made
// before the tests run:
//
//	var base = sandpit.With().Timeout(5 * time.Second)
//
//	func TestFoo(t *testing.T) {
//		s := base.Session(t)
//		// ...
//	}
type SessionBuilder struct {
	sp        *Sandpit
	timeout   time.Duration
	connector func(driver.Connector) driver.Connector
	conn      func(*pgx.Conn)
}

// Timeout sets how long a call waits for the session while another transaction
// holds it. It is the default of DBBuilder.Timeout for the *sql.DB values of
// the session, and also bounds how long the end of the test waits for the
// session. The default is DefaultTimeout.
func (b *SessionBuilder) Timeout(d time.Duration) *SessionBuilder {
	c := *b
	c.timeout = d
	return &c
}

// Connector sets a function that is given the connector pgsandpit made for each
// *sql.DB of the session and returns the connector the *sql.DB is opened with.
// It is called each time a *sql.DB is made. A function set with
// DBBuilder.Connector is given what f returns.
func (b *SessionBuilder) Connector(f func(driver.Connector) driver.Connector) *SessionBuilder {
	c := *b
	c.connector = f
	return &c
}

// Conn sets a function that Session calls once, before it returns, with the
// session's *pgx.Conn, for example to SET a parameter for the session or to
// insert data the test needs. A parameter SET this way applies to every *sql.DB
// of the session and is reset when the session's transaction is rolled back.
//
// What f runs, runs in the session's transaction as it is: a statement that
// fails aborts the session's transaction. f must not begin, commit or roll back
// a transaction, nor use c after it returns.
func (b *SessionBuilder) Conn(f func(c *pgx.Conn)) *SessionBuilder {
	c := *b
	c.conn = f
	return &c
}

// Session opens a connection, begins a transaction on it, and returns a
// Session for it. The transaction is rolled back and the connection closed when
// t and its subtests finish. Session fails t if it cannot connect.
func (b *SessionBuilder) Session(t *testing.T) *Session {
	t.Helper()
	pool, err := b.sp.pool()
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
		t:         t,
		conn:      conn,
		tx:        tx,
		timeout:   b.timeout,
		connector: b.connector,
		sem:       make(chan struct{}, 1),
	}
	// Registered before b.conn runs, so that the session is rolled back even if
	// b.conn fails the test.
	t.Cleanup(s.close)
	if b.conn != nil {
		if err := s.withPgxConn(s.timeout, b.conn); err != nil {
			t.Fatal(err)
		}
	}
	return s
}
