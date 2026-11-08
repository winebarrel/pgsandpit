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
	sp         *Sandpit
	readOnly   bool
	timeout    time.Duration
	connector  func(driver.Connector) driver.Connector
	conn       func(*testing.T, *pgx.Conn)
	cache      bool
	interleave bool
}

// ReadOnly makes every *sql.DB of the session reject writes, as
// DBBuilder.ReadOnly does. A function set with Conn is not affected by it, so
// it can insert the data the test reads.
func (b *SessionBuilder) ReadOnly() *SessionBuilder {
	c := *b
	c.readOnly = true
	return &c
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

// Conn sets a function that Session calls once, before it returns, with the t
// given to Session and the session's *pgx.Conn, for example to SET a parameter
// for the session or to insert data the test needs. A parameter SET this way
// applies to every *sql.DB of the session and is reset when the session's
// transaction is rolled back.
//
// What f runs, runs in the session's transaction as it is: a statement that
// fails aborts the session's transaction. f must not begin, commit or roll back
// a transaction, nor use c after it returns.
func (b *SessionBuilder) Conn(f func(t *testing.T, c *pgx.Conn)) *SessionBuilder {
	c := *b
	c.conn = f
	return &c
}

// Cache makes Session return the session cached for t in the Sandpit if there is
// one, and otherwise open one and cache it until t and its subtests finish, so
// that helpers can get the session from t alone.
//
// The options of the call that opened the session apply: those of a later call
// that finds it in the cache are ignored. Only sessions built with Cache are
// cached. A subtest has its own t, so it gets its own session rather than its
// parent's.
func (b *SessionBuilder) Cache() *SessionBuilder {
	c := *b
	c.cache = true
	return &c
}

// Interleave lets a statement run outside an application transaction while
// that transaction is open, instead of waiting for it to end. The statement
// runs at once, inside the open transaction, in a savepoint of its own: it sees
// what the transaction wrote, a failure fails only that statement, and what it
// writes is rolled back with the transaction if the transaction rolls back.
//
// It is for code that begins a transaction on a *sql.DB and then, before the
// transaction ends, uses the *sql.DB outside the transaction, which runs on
// another connection outside a test. Without Interleave, such a statement waits
// for the transaction, which waits for the statement, until ErrTimeout.
//
// A transaction begun while another is open does not wait for it either: it
// nests inside it, in a savepoint inside the open transaction's savepoint.
// Nested transactions must end in the reverse order they began. If one ends
// before a transaction begun after it, PostgreSQL ends the later one with it:
// a Commit of the earlier one rolls both back and returns an error wrapping
// ErrTxOrder, and the later one's Commit or Rollback returns such an error.
//
// Statements still run one at a time. A statement or a transaction in another
// goroutine also runs inside an open transaction instead of waiting for it, so
// transactions that overlap in different goroutines can end out of order.
func (b *SessionBuilder) Interleave() *SessionBuilder {
	c := *b
	c.interleave = true
	return &c
}

// Session opens a connection, begins a transaction on it, and returns a
// Session for it. The transaction is rolled back and the connection closed when
// t and its subtests finish. Session fails t if it cannot connect. With Cache,
// it returns the session cached for t if there is one.
func (b *SessionBuilder) Session(t *testing.T) *Session {
	t.Helper()
	if !b.cache {
		return b.open(t)
	}
	v, _ := b.sp.cached.LoadOrStore(t, &cachedSession{})
	e := v.(*cachedSession)
	e.once.Do(func() {
		t.Helper()
		t.Cleanup(func() { b.sp.cached.Delete(t) })
		e.s = b.open(t)
	})
	if e.s == nil {
		// Another call failed to open the session and has reported why.
		t.FailNow()
	}
	return e.s
}

func (b *SessionBuilder) open(t *testing.T) *Session {
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
		t:          t,
		conn:       conn,
		tx:         tx,
		readOnly:   b.readOnly,
		timeout:    b.timeout,
		connector:  b.connector,
		interleave: b.interleave,
		sem:        make(chan struct{}, 1),
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
