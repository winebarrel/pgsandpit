package pgsandpit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// DBBuilder builds a *sql.DB for a Session. Each method returns a new
// DBBuilder, so a DBBuilder can be shared and branched.
type DBBuilder struct {
	s         *Session
	readOnly  bool
	timeout   time.Duration
	connector func(*testing.T, driver.Connector) driver.Connector
	conn      func(*testing.T, *pgx.Conn)
}

// ReadOnly makes the *sql.DB reject writes. It still sees what the other
// *sql.DB values of the session wrote. A rejected write fails only that
// statement, as in a read-only transaction outside a test, except that inside
// an application transaction it aborts that transaction. Every *sql.DB of a
// session built with SessionBuilder.ReadOnly is read-only.
func (b *DBBuilder) ReadOnly() *DBBuilder {
	c := *b
	c.readOnly = true
	return &c
}

// Timeout sets how long a call on the *sql.DB waits for the session while
// another transaction holds it. The default is the session's timeout (see
// SessionBuilder.Timeout).
func (b *DBBuilder) Timeout(d time.Duration) *DBBuilder {
	c := *b
	c.timeout = d
	return &c
}

// Connector sets a function that is given the connector pgsandpit made for the
// *sql.DB and returns the connector the *sql.DB is opened with, as by
// sql.OpenDB. If the session was built with SessionBuilder.Connector, f is
// given the connector that function returned. t is the test the session was
// opened for.
func (b *DBBuilder) Connector(f func(t *testing.T, c driver.Connector) driver.Connector) *DBBuilder {
	c := *b
	c.connector = f
	return &c
}

// Conn sets a function that DB calls once, before it returns, with the
// test the session was opened for and the session's *pgx.Conn. DB waits for the
// session as the *sql.DB's calls do, and holds it while f runs.
//
// What f runs, runs in the session's transaction as it is: it is not read-only,
// and a statement that fails aborts the session's transaction. f must not
// begin, commit or roll back a transaction, nor use c after it returns.
func (b *DBBuilder) Conn(f func(t *testing.T, c *pgx.Conn)) *DBBuilder {
	c := *b
	c.conn = f
	return &c
}

// DB returns the *sql.DB. It is closed when the test ends; closing it earlier
// does not end the session.
func (b *DBBuilder) DB() *sql.DB {
	if b.conn != nil {
		if err := b.s.withPgxConn(b.timeout, b.conn); err != nil {
			b.s.t.Fatal(err)
		}
	}
	var c driver.Connector = &connector{b: *b}
	if b.s.connector != nil {
		c = b.s.connector(b.s.t, c)
	}
	if b.connector != nil {
		c = b.connector(b.s.t, c)
	}
	db := sql.OpenDB(c)
	// Registered after the session's cleanup, so it runs before it.
	b.s.t.Cleanup(func() { _ = db.Close() })
	return db
}

type connector struct {
	b DBBuilder
}

func (c *connector) Connect(context.Context) (driver.Conn, error) {
	return &conn{s: c.b.s, readOnly: c.b.readOnly, timeout: c.b.timeout}, nil
}

func (c *connector) Driver() driver.Driver {
	return sandpitDriver{}
}

type sandpitDriver struct{}

func (sandpitDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("pgsandpit: open a *sql.DB with Session.DB")
}
