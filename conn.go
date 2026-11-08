package pgsandpit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// inFailedSQLTransaction is the SQLSTATE of "current transaction is aborted".
const inFailedSQLTransaction = "25P02"

// conn is what database/sql sees as one connection. A *sql.DB may hold many of
// them; they all run on the session's connection.
type conn struct {
	s        *Session
	readOnly bool
	timeout  time.Duration

	// tx is the open application transaction. database/sql uses a conn from
	// one goroutine at a time, so it needs no lock.
	tx *tx
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
)

// acquire makes the session this conn's for one call. Without interleave, a
// conn that holds the session for an application transaction already has it.
func (c *conn) acquire(ctx context.Context) (func(), error) {
	if c.tx != nil && !c.s.interleave {
		return func() {}, nil
	}
	return c.s.acquire(ctx, c.timeout)
}

// do runs fn on the session.
//
// Outside an application transaction, fn runs in its own savepoint, so a
// failing statement fails alone, as it would in autocommit mode, instead of
// aborting the session's transaction. Inside one, fn runs as is and a failure
// aborts that transaction, as it would outside a test.
//
// A read-only conn runs fn in a savepoint with transaction_read_only on, which
// PostgreSQL turns off again when the savepoint ends.
func (c *conn) do(ctx context.Context, fn func(context.Context) error) error {
	release, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	inTx := c.tx != nil
	if inTx && c.tx.endedByOuter {
		return errEndedByOuter
	}
	if inTx && !c.readOnly {
		return fn(ctx)
	}

	// Savepoint statements must run even when ctx is canceled, or the session
	// would be left in a failed state.
	ctl := context.WithoutCancel(ctx)
	sp := c.s.savepoint("stmt")
	if _, err := c.s.tx.ExecContext(ctl, "SAVEPOINT "+sp); err != nil {
		return err
	}
	if c.readOnly {
		if _, err := c.s.tx.ExecContext(ctl, "SET LOCAL transaction_read_only = on"); err != nil {
			return withCleanup(err, c.s.rollbackTo(ctl, sp))
		}
	}

	if err := fn(ctx); err != nil {
		if inTx {
			return err
		}
		return withCleanup(err, c.s.rollbackTo(ctl, sp))
	}

	_, err = c.s.tx.ExecContext(ctl, "RELEASE SAVEPOINT "+sp)
	return err
}

func (s *Session) rollbackTo(ctx context.Context, sp string) error {
	if _, err := s.tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+sp); err != nil {
		return err
	}
	_, err := s.tx.ExecContext(ctx, "RELEASE SAVEPOINT "+sp)
	return err
}

// withCleanup returns err, joined with cleanupErr if cleaning up failed too.
func withCleanup(err, cleanupErr error) error {
	if cleanupErr == nil {
		return err
	}
	return errors.Join(err, cleanupErr)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var res driver.Result
	err := c.do(ctx, func(ctx context.Context) error {
		r, err := c.s.tx.ExecContext(ctx, query, namedArgs(args)...)
		res = r
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// QueryContext reads all rows before it returns, so that an open *sql.Rows
// does not keep the session from other calls.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var rows driver.Rows
	err := c.do(ctx, func(ctx context.Context) error {
		rs, err := c.s.tx.QueryContext(ctx, query, namedArgs(args)...)
		if err != nil {
			return err
		}
		rows, err = readRows(rs)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// BeginTx emulates a transaction with a savepoint. The transaction holds the
// session until it ends. With interleave, it does not: it holds the session
// only while each of its statements runs, and a transaction begun while it is
// open nests inside it. TxOptions are ignored.
func (c *conn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if c.tx != nil {
		return nil, errors.New("pgsandpit: a transaction is already open on this connection")
	}
	release, err := c.s.acquire(ctx, c.timeout)
	if err != nil {
		return nil, err
	}
	if c.s.interleave {
		defer release()
		release = func() {}
	}

	name := c.s.savepoint("tx")
	if _, err := c.s.tx.ExecContext(context.WithoutCancel(ctx), "SAVEPOINT "+name); err != nil {
		release()
		return nil, err
	}
	stack := currentStack()
	t := &tx{c: c, name: name, release: release, begunAt: stack}
	if c.s.interleave {
		c.s.txs = append(c.s.txs, t)
	} else {
		c.s.setHolder(stack)
	}
	c.tx = t
	return t, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return &stmt{c: c, query: query}, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) Ping(ctx context.Context) error {
	_, err := c.ExecContext(ctx, "SELECT 1", nil)
	return err
}

// CheckNamedValue passes every argument through: pgx accepts sql.Scanner and
// driver.Valuer values itself.
func (c *conn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

// Close rolls back a transaction database/sql left open, such as when it
// discards a conn whose transaction's context was canceled.
func (c *conn) Close() error {
	if c.tx != nil {
		return c.tx.Rollback()
	}
	return nil
}

func namedArgs(args []driver.NamedValue) []any {
	res := make([]any, len(args))
	for i, a := range args {
		if a.Name != "" {
			res[i] = sql.Named(a.Name, a.Value)
		} else {
			res[i] = a.Value
		}
	}
	return res
}

type tx struct {
	c       *conn
	name    string
	release func()
	begunAt string // stack of the Begin

	// endedByOuter is set, with interleave, when a transaction that this one
	// is nested in ended first and took this one's savepoint with it. Read and
	// written only while the session is held.
	endedByOuter bool
}

// Commit releases the savepoint. If the transaction is aborted, it rolls back
// to the savepoint and returns pgx.ErrTxCommitRollback, as a COMMIT of an
// aborted transaction does.
func (t *tx) Commit() error {
	result := TxCommitFailed
	defer func() { t.finish(result) }()

	ctx := context.Background()
	release, err := t.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if t.endedByOuter {
		return errEndedByOuter
	}
	if inner := t.unnest(); len(inner) > 0 {
		// Releasing the savepoint would commit what the inner transactions
		// wrote, which they have not committed. Roll back instead.
		if err := t.c.s.rollbackTo(ctx, t.name); err != nil {
			return err
		}
		return fmt.Errorf("%w: a transaction was committed while %d transaction(s) begun after it were still open, so it was rolled back with them", ErrTxOrder, len(inner))
	}
	_, err = t.c.s.tx.ExecContext(ctx, "RELEASE SAVEPOINT "+t.name)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == inFailedSQLTransaction {
		if err := t.c.s.rollbackTo(ctx, t.name); err != nil {
			return err
		}
		return pgx.ErrTxCommitRollback
	}
	if err == nil {
		result = TxCommitted
	}
	return err
}

func (t *tx) Rollback() error {
	defer t.finish(TxRolledBack)
	ctx := context.Background()
	release, err := t.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if t.endedByOuter {
		return errEndedByOuter
	}
	// Rolling back to the savepoint also rolls back the inner transactions.
	t.unnest()
	return t.c.s.rollbackTo(ctx, t.name)
}

// errEndedByOuter is returned when a transaction ends after a transaction it
// is nested in ended and took it with it.
var errEndedByOuter = fmt.Errorf("%w: a transaction begun before this one ended first and rolled this one back", ErrTxOrder)

// unnest removes the transaction from the open ones, with interleave, and
// returns the transactions nested in it, which end with it. It is called while
// the session is held.
func (t *tx) unnest() []*tx {
	s := t.c.s
	i := slices.Index(s.txs, t)
	if i < 0 {
		return nil
	}
	inner := slices.Clone(s.txs[i+1:])
	s.txs = s.txs[:i]
	for _, in := range inner {
		in.endedByOuter = true
	}
	return inner
}

// acquire takes the session for the statement that ends the transaction. Only
// with interleave does the transaction not hold it already.
func (t *tx) acquire(ctx context.Context) (func(), error) {
	if !t.c.s.interleave {
		return func() {}, nil
	}
	return t.c.s.acquire(ctx, t.c.timeout)
}

func (t *tx) finish(result TxResult) {
	t.c.tx = nil
	t.c.s.endTx(result)
	t.release()
}

type stmt struct {
	c     *conn
	query string
}

var (
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), toNamed(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamed(args))
}

func toNamed(args []driver.Value) []driver.NamedValue {
	res := make([]driver.NamedValue, len(args))
	for i, v := range args {
		res[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return res
}
