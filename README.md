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

`Conn` is called once, when `DB` makes the `*sql.DB`, with the session's `*pgx.Conn`, for example to use pgx's API:

```go
db := s.With().Conn(func(c *pgx.Conn) {
	_, err := c.CopyFrom(t.Context(), pgx.Identifier{"users"}, []string{"email"}, pgx.CopyFromRows(rows))
	require.NoError(t, err)
}).DB()
```

What it runs, runs in the session's transaction as it is: it is not read-only, and a failing statement aborts the session's transaction. It must not begin, commit or roll back a transaction.

Options for a session are chained after `sandpit.With()`. A `SessionBuilder` can be made once and shared by the tests:

```go
var base = sandpit.With().
	Timeout(5 * time.Second).
	Conn(func(c *pgx.Conn) {
		if _, err := c.Exec(context.Background(), "SET TIME ZONE 'UTC'"); err != nil {
			panic(err)
		}
	})

func TestFoo(t *testing.T) {
	s := base.Session(t)
	db := s.DB()                        // waits at most 5 seconds for the session
	// ...
}
```

- `Timeout` is the default of the session's `*sql.DB` values, which `s.With().Timeout(...)` replaces. It also bounds how long the end of the test waits for a session that is still in use.
- `Connector` is applied to every `*sql.DB` of the session, each time one is made. A `Connector` set with `s.With().Connector(...)` is given the connector the session's returned.
- `Conn` is called once, when `Session` makes the session, with the session's `*pgx.Conn`. A parameter it `SET`s applies to every `*sql.DB` of the session and is reset when the session is rolled back.

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

## Behavior

- **Isolation**: each `Session` has its own connection and transaction, so parallel tests do not see each other's data. Tests that write the same rows or unique keys still wait for each other's locks until the first test ends.
- **Transactions**: `Begin` creates a savepoint, `Commit` releases it, and `Rollback` rolls back to it. `TxOptions` are ignored. Committing a transaction that hit an error rolls it back and returns `pgx.ErrTxCommitRollback`, as PostgreSQL does.
- **Statements outside a transaction** each run in their own savepoint, so a failing statement (such as a unique violation) fails alone, as in autocommit mode, instead of aborting the rest of the test.
- **Serialization**: one statement, or one transaction, runs on a session at a time; the others wait. A call that waits longer than the timeout (60 seconds by default) returns an error wrapping `pgsandpit.ErrTimeout`, with the stack of the `Begin` that holds the session. This is usually a transaction left open, or a `*sql.DB` used outside a transaction while that transaction is open in the same goroutine.
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
