package pgsandpit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

// Builder builds a *sql.DB for a Session. Each method returns a new Builder,
// so a Builder can be shared and branched.
type Builder struct {
	s        *Session
	readOnly bool
	timeout  time.Duration
}

// ReadOnly makes the *sql.DB reject writes. It still sees what the other
// *sql.DB values of the session wrote. A rejected write fails only that
// statement, as in a read-only transaction outside a test, except that inside
// an application transaction it aborts that transaction.
func (b *Builder) ReadOnly() *Builder {
	c := *b
	c.readOnly = true
	return &c
}

// Timeout sets how long a call on the *sql.DB waits for the session while
// another transaction holds it. The default is DefaultTimeout.
func (b *Builder) Timeout(d time.Duration) *Builder {
	c := *b
	c.timeout = d
	return &c
}

// DB returns the *sql.DB. It is closed when the test ends; closing it earlier
// does not end the session.
func (b *Builder) DB() *sql.DB {
	db := sql.OpenDB(&connector{b: *b})
	// Registered after the session's cleanup, so it runs before it.
	b.s.t.Cleanup(func() { _ = db.Close() })
	return db
}

type connector struct {
	b Builder
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
