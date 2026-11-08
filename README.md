# pgsandpit

[![GitHub Tag](https://img.shields.io/github/v/tag/winebarrel/pgsandpit.svg)](https://pkg.go.dev/github.com/winebarrel/pgsandpit?tab=versions)
[![Go Reference](https://pkg.go.dev/badge/github.com/winebarrel/pgsandpit.svg)](https://pkg.go.dev/github.com/winebarrel/pgsandpit)
[![test](https://github.com/winebarrel/pgsandpit/actions/workflows/test.yml/badge.svg)](https://github.com/winebarrel/pgsandpit/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/winebarrel/pgsandpit/graph/badge.svg)](https://codecov.io/gh/winebarrel/pgsandpit)
[![AI Generated](https://img.shields.io/badge/AI%20Generated-Claude-orange?logo=anthropic)](https://claude.com/claude-code)

pgsandpit is a Go testing library for PostgreSQL. It gives each test its own session wrapped in a transaction, hands the test `*sql.DB` values that run on that session, and rolls everything back when the test ends.

## Usage

```go
package myapp_test

import (
	"testing"

	"github.com/winebarrel/pgsandpit"
)

var sandpit = pgsandpit.New("postgres://postgres@localhost:5432/postgres")

func TestReport(t *testing.T) {
	t.Parallel()

	s := sandpit.Session(t)             // one connection, one transaction; rolled back when t ends
	db := s.DB()                        // a *sql.DB on the session
	ro := s.With().ReadOnly().DB()      // another one that rejects writes

	db.Exec(`INSERT INTO orders ...`)   // ro sees this: both share the session
	svc := NewReportService(db, ro)
	// ...
}
```

Options for a `*sql.DB` are chained after `With()`:

```go
db := s.With().ReadOnly().Timeout(5 * time.Second).DB()
```

`Connector` is given the connector pgsandpit made for the `*sql.DB` and returns the connector to open it with, as `sql.OpenDB` takes one. For example, a connector that wraps it can hook into the queries:

```go
db := s.With().Connector(func(c driver.Connector) driver.Connector {
	return &MyConnector{c}
}).DB()
```

`Conn` is called once, when `DB` makes the `*sql.DB`, with the test the session was opened for and the session's `*pgx.Conn`, for example to use pgx's API:

```go
db := s.With().Conn(func(t *testing.T, c *pgx.Conn) {
	_, err := c.CopyFrom(t.Context(), pgx.Identifier{"users"}, []string{"email"}, pgx.CopyFromRows(rows))
	if err != nil {
		t.Fatal(err)
	}
}).DB()
```

What it runs, runs in the session's transaction as it is: it is not read-only, and a failing statement aborts the session's transaction. It must not begin, commit or roll back a transaction.

Options for a session are chained after `sandpit.With()`. A `SessionBuilder` can be made once and shared by the tests:

```go
var base = sandpit.With().
	Timeout(5 * time.Second).
	Conn(func(t *testing.T, c *pgx.Conn) {
		if _, err := c.Exec(t.Context(), "SET TIME ZONE 'UTC'"); err != nil {
			t.Fatal(err)
		}
	})

func TestFoo(t *testing.T) {
	s := base.Session(t)
	db := s.DB()                        // waits at most 5 seconds for the session
	// ...
}
```

- `ReadOnly` makes every `*sql.DB` of the session read-only. `Conn` is not affected by it, so it can insert the data the test reads.
- `Timeout` is the default of the session's `*sql.DB` values, which `s.With().Timeout(...)` replaces. It also bounds how long the end of the test waits for a session that is still in use.
- `Connector` is applied to every `*sql.DB` of the session, each time one is made. A `Connector` set with `s.With().Connector(...)` is given the connector the session's returned.
- `Conn` is called once, when `Session` makes the session, with the `t` given to `Session` and the session's `*pgx.Conn`. A parameter it `SET`s applies to every `*sql.DB` of the session and is reset when the session is rolled back.
- `Cache` makes `Session` return the session cached for `t` if there is one, and otherwise open one and cache it, so helpers can get the session from `t` alone. The options of the call that opened the session apply, and those of later calls are ignored. A subtest has its own `t`, so it gets its own session rather than its parent's.
- `Interleave` lets a statement, or a transaction, run while an application transaction is open, instead of waiting for it to end (see Interleave below).

```go
var cached = sandpit.With().Cache()

func insertOrder(t *testing.T) {
	t.Helper()
	db := cached.Session(t).DB()
	// ...
}

func TestOrder(t *testing.T) {
	insertOrder(t)
	s := cached.Session(t)              // the session insertOrder used
	// ...
}
```

`TxResults` returns how the transactions of the session's `*sql.DB` values ended, in order, so a test can check that the code under test rolled back:

```go
s := sandpit.Session(t)

if err := svc.Transfer(ctx, s.DB(), from, to, 100); err == nil {
	t.Fatal("Transfer must fail")
}

want := []pgsandpit.TxResult{pgsandpit.TxRolledBack}
if got := s.TxResults(); !slices.Equal(got, want) {
	t.Errorf("TxResults() = %v, want %v", got, want)
}
```

- `TxCommitted`: `Commit` succeeded.
- `TxRolledBack`: `Rollback` was called, or the transaction's context was canceled. `database/sql` rolls back a canceled transaction in another goroutine, so its result may appear shortly after the cancellation.
- `TxCommitFailed`: `Commit` was called but did not commit, such as when the transaction had hit an error.

A transaction that is still open is not included. Savepoints the application creates with SQL are not transactions here, so they are not included either.

To configure pgx, pass a `*pgx.ConnConfig` and the options of `stdlib.OpenDB`:

```go
cfg, err := pgx.ParseConfig("postgres://postgres@localhost:5432/postgres")
if err != nil {
	log.Fatal(err)
}
sandpit := pgsandpit.NewWithConfig(cfg,
	stdlib.OptionAfterConnect(func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET TIME ZONE 'UTC'")
		return err
	}),
)
```

The connections are pooled across sessions, so `OptionAfterConnect` runs once per connection, not once per session. pgsandpit replaces `BuildContextWatcherHandler` (see Cancellation below).

To choose a database by name, for example in a table-driven test, register a `Sandpit` under a name and get it with `Lookup`. Each test package runs in its own process, so a package shared by the tests can register the names in its `init` function:

```go
// internal/testdb/testdb.go
package testdb

func init() {
	pgsandpit.Register("testdb1", pgsandpit.New("postgres://xxx"))
	pgsandpit.Register("testdb2", pgsandpit.New("postgres://yyy"))
}
```

```go
import _ "example.com/myapp/internal/testdb"

func TestFoo(t *testing.T) {
	s := pgsandpit.Lookup("testdb1").Session(t)
	// ...
}
```

`Register` panics if the name is already registered, and `Lookup` panics if it is not registered. A `Sandpit` does not connect until the first `Session` call, so registering one does not connect to the database.

## Behavior

- **Isolation**: each `Session` has its own connection and transaction, so parallel tests do not see each other's data. Tests that write the same rows or unique keys still wait for each other's locks until the first test ends.
- **Transactions**: `Begin` creates a savepoint, `Commit` releases it, and `Rollback` rolls back to it. `TxOptions` are ignored. Committing a transaction that hit an error rolls it back and returns `pgx.ErrTxCommitRollback`, as PostgreSQL does.
- **Statements outside a transaction** each run in their own savepoint, so a failing statement (such as a unique violation) fails alone, as in autocommit mode, instead of aborting the rest of the test.
- **Serialization**: one statement, or one transaction, runs on a session at a time; the others wait. A call that waits longer than the timeout (60 seconds by default) returns an error wrapping `pgsandpit.ErrTimeout`, with the stack of the `Begin` that holds the session. This is usually a transaction left open, or a `*sql.DB` used outside a transaction while that transaction is open in the same goroutine.
- **Interleave**: code that begins a transaction on a `*sql.DB` and then, before the transaction ends, uses the `*sql.DB` outside the transaction, or begins another transaction on it, runs on several connections outside a test, but on one session in a test, where the second would wait for the first until it times out. With `sandpit.With().Interleave()`, it does not wait:
  - A statement outside the open transaction runs inside it, in a savepoint of its own: it sees what the transaction wrote, a failure fails only that statement, and what it writes is rolled back with the transaction if the transaction rolls back.
  - A transaction begun while another is open nests inside it. Nested transactions must end in the reverse order they began. If one ends before a transaction begun after it, PostgreSQL ends the later one with it: a `Commit` of the earlier one rolls both back and returns an error wrapping `pgsandpit.ErrTxOrder`, and so do the later one's statements, `Commit` and `Rollback`.
  - Statements still run one at a time. Statements and transactions in other goroutines do not wait for an open transaction either, so transactions that overlap in different goroutines can end out of order.
  - A transaction left open at the end of the test fails the test with the stack of its `Begin`.

  ```go
  db := sandpit.With().Interleave().Session(t).DB()

  tx, _ := db.Begin()
  tx.Exec(`INSERT INTO orders ...`)
  db.QueryRow(`SELECT count(*) FROM orders`)  // runs inside tx instead of waiting for it
  inner, _ := db.Begin()                      // nests inside tx
  inner.Commit()
  tx.Commit()
  ```
- **Read-only**: a read-only `*sql.DB` runs each statement with `transaction_read_only` on, so PostgreSQL rejects writes, including writes inside functions.
- **Rows** are read in full before a query returns, so an open `*sql.Rows` does not hold the session.
- **Cancellation**: a canceled context cancels the query on the server and leaves the session usable. A transaction whose context is canceled is rolled back.

## Out of scope

Tests of concurrency itself (lock waits, `SELECT ... FOR UPDATE`, isolation levels) and of session state changed through a `*sql.DB` (`SET`, advisory locks, temporary tables shared across `*sql.DB` values) need real separate connections. A parameter that every `*sql.DB` of the session needs can be set with `SessionBuilder.Conn`.

## Running the tests

```sh
docker compose up -d
make test
```
