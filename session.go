package pgsandpit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var (
	// ErrTimeout is returned, wrapped, when a call waited longer than the
	// timeout for the session. It usually means a transaction was left open,
	// or the *sql.DB was used outside a transaction while that transaction was
	// open in the same goroutine.
	ErrTimeout = errors.New("pgsandpit: timed out waiting for the session")

	// ErrSessionClosed is returned when a *sql.DB is used after its session
	// ended.
	ErrSessionClosed = errors.New("pgsandpit: session is closed")

	// ErrTxOrder is returned, wrapped, with SessionBuilder.Interleave, when
	// nested transactions do not end in the reverse order they began, such as
	// when transactions in different goroutines overlap. The transactions that
	// could not end as asked are rolled back.
	ErrTxOrder = errors.New("pgsandpit: transactions ended out of order")
)

// Session is one PostgreSQL connection with one transaction open on it. Every
// *sql.DB made from a Session runs on that connection, so they all see each
// other's changes. Only one statement, or one application transaction, runs on
// the session at a time; the others wait. With SessionBuilder.Interleave,
// statements and transactions do not wait for an open application transaction:
// they run, or nest, inside it (see there).
type Session struct {
	t    *testing.T
	conn *sql.Conn
	tx   *sql.Tx

	// From the SessionBuilder: whether every *sql.DB is read-only, the default
	// timeout of the *sql.DB values, the connector function every *sql.DB is
	// opened through, and whether statements interleave with transactions.
	readOnly   bool
	timeout    time.Duration
	connector  func(driver.Connector) driver.Connector
	interleave bool

	// sem is held while a statement runs. Without interleave, an application
	// transaction holds it until it ends; with interleave, a transaction holds
	// it only while each of its statements runs.
	sem    chan struct{}
	closed bool // read and written only while sem is held

	// txs are the open application transactions with interleave, the innermost
	// last. Read and written only while sem is held.
	txs []*tx

	seq atomic.Uint64

	mu        sync.Mutex
	holder    string     // stack of the Begin that holds the session, for ErrTimeout
	txResults []TxResult // how the application transactions ended, in order
}

// TxResult is how an application transaction ended.
type TxResult int

const (
	// TxCommitted means Commit succeeded.
	TxCommitted TxResult = iota
	// TxRolledBack means the transaction ended with a rollback: Rollback was
	// called, or database/sql rolled it back because its context was canceled.
	TxRolledBack
	// TxCommitFailed means Commit was called but did not commit, such as when
	// the transaction had hit an error, which rolls it back and makes Commit
	// return pgx.ErrTxCommitRollback.
	TxCommitFailed
)

// String returns the name of the constant, so that a failed assertion on
// TxResults reads well.
func (r TxResult) String() string {
	switch r {
	case TxCommitted:
		return "TxCommitted"
	case TxRolledBack:
		return "TxRolledBack"
	case TxCommitFailed:
		return "TxCommitFailed"
	default:
		return fmt.Sprintf("TxResult(%d)", int(r))
	}
}

// TxResults returns how the application transactions of every *sql.DB of the
// session ended, in the order they ended. A transaction that is still open is
// not included. It does not wait for the session, so it can be called while a
// transaction is open.
//
// database/sql rolls back a transaction whose context was canceled in another
// goroutine, so its result may appear shortly after the cancellation.
func (s *Session) TxResults() []TxResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.txResults)
}

// DB returns a *sql.DB that runs on the session. It is the same as
// s.With().DB().
func (s *Session) DB() *sql.DB {
	return s.With().DB()
}

// With starts building a *sql.DB with options, starting from the defaults the
// session was built with. The Session itself is not changed.
func (s *Session) With() *DBBuilder {
	return &DBBuilder{s: s, readOnly: s.readOnly, timeout: s.timeout}
}

func (s *Session) savepoint(kind string) string {
	return fmt.Sprintf("pgsandpit_%s_%d", kind, s.seq.Add(1))
}

// acquire waits until the session is free, for at most timeout or until ctx is
// done. The returned function frees it.
func (s *Session) acquire(ctx context.Context, timeout time.Duration) (func(), error) {
	// The timeout bounds only the wait: the statement itself runs with ctx.
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case s.sem <- struct{}{}:
	case <-waitCtx.Done():
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, s.timeoutError(timeout)
	}

	if s.closed {
		<-s.sem
		return nil, ErrSessionClosed
	}
	return func() { <-s.sem }, nil
}

func (s *Session) timeoutError(timeout time.Duration) error {
	s.mu.Lock()
	holder := s.holder
	s.mu.Unlock()
	// With interleave, transactions do not hold the session, so a wait for it
	// is not a wait for a transaction.
	if holder == "" || s.interleave {
		return fmt.Errorf("%w after %s", ErrTimeout, timeout)
	}
	return fmt.Errorf("%w after %s: the session is held by a transaction that was begun at:\n%s", ErrTimeout, timeout, holder)
}

func (s *Session) setHolder(stack string) {
	s.mu.Lock()
	s.holder = stack
	s.mu.Unlock()
}

// endTx records how the application transaction that held the session ended.
func (s *Session) endTx(result TxResult) {
	s.mu.Lock()
	s.holder = ""
	s.txResults = append(s.txResults, result)
	s.mu.Unlock()
}

func (s *Session) close() {
	// The *sql.DB cleanups ran first and rolled back any transaction they
	// still had. A transaction that is still open here was leaked.
	release, err := s.acquire(context.Background(), s.timeout)
	if err != nil {
		s.t.Errorf("pgsandpit: the session is still in use at the end of the test: %s", err)
	}
	s.closed = true
	for _, t := range s.txs {
		s.t.Errorf("pgsandpit: a transaction is still open at the end of the test; it was begun at:\n%s", t.begunAt)
	}

	if err := s.tx.Rollback(); err != nil {
		s.t.Errorf("pgsandpit: failed to roll back the session transaction: %s", err)
	}
	if err := s.conn.Close(); err != nil {
		s.t.Errorf("pgsandpit: failed to close the session connection: %s", err)
	}
	if release != nil {
		release()
	}
}

// withPgxConn runs f with the session's *pgx.Conn while it holds the session.
func (s *Session) withPgxConn(timeout time.Duration, f func(*pgx.Conn)) error {
	release, err := s.acquire(context.Background(), timeout)
	if err != nil {
		return err
	}
	defer release()

	var c *pgx.Conn
	if err := s.conn.Raw(func(dc any) error {
		c = dc.(*stdlib.Conn).Conn()
		return nil
	}); err != nil {
		return fmt.Errorf("pgsandpit: %w", err)
	}
	// f runs outside Raw: if it ended the goroutine inside Raw, as t.FailNow
	// does, Raw would take it for a panic and release the connection with
	// driver.ErrBadConn, so that database/sql may discard it. Holding the session
	// keeps c from being used by anything else meanwhile.
	f(c)
	return nil
}

func currentStack() string {
	return string(debug.Stack())
}
