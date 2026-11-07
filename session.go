package pgsandpit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
)

// Session is one PostgreSQL connection with one transaction open on it. Every
// *sql.DB made from a Session runs on that connection, so they all see each
// other's changes. Only one statement, or one application transaction, runs on
// the session at a time; the others wait.
type Session struct {
	t    *testing.T
	conn *sql.Conn
	tx   *sql.Tx

	// sem is held while a statement or an application transaction runs.
	sem    chan struct{}
	closed bool // read and written only while sem is held

	seq atomic.Uint64

	mu     sync.Mutex
	holder string // stack of the Begin that holds the session, for ErrTimeout
}

// DB returns a *sql.DB that runs on the session. It is the same as
// s.With().DB().
func (s *Session) DB() *sql.DB {
	return s.With().DB()
}

// With starts building a *sql.DB with options. The Session itself is not
// changed.
func (s *Session) With() *Builder {
	return &Builder{s: s, timeout: DefaultTimeout}
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
	if holder == "" {
		return fmt.Errorf("%w after %s", ErrTimeout, timeout)
	}
	return fmt.Errorf("%w after %s: the session is held by a transaction that was begun at:\n%s", ErrTimeout, timeout, holder)
}

func (s *Session) setHolder(stack string) {
	s.mu.Lock()
	s.holder = stack
	s.mu.Unlock()
}

func (s *Session) close() {
	// The *sql.DB cleanups ran first and rolled back any transaction they
	// still had. A transaction that is still open here was leaked.
	release, err := s.acquire(context.Background(), DefaultTimeout)
	if err != nil {
		s.t.Errorf("pgsandpit: the session is still in use at the end of the test: %s", err)
	}
	s.closed = true

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

func currentStack() string {
	return string(debug.Stack())
}
