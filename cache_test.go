package pgsandpit_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

func backendPID(t *testing.T, s *pgsandpit.Session) int {
	t.Helper()
	var pid int
	require.NoError(t, s.DB().QueryRow("SELECT pg_backend_pid()").Scan(&pid))
	return pid
}

func TestCacheSameTest(t *testing.T) {
	t.Parallel()

	s := sandpit.With().Cache().Session(t)
	assert.Same(t, s, sandpit.With().Cache().Session(t))
}

func TestCacheNotUsedWithoutCache(t *testing.T) {
	t.Parallel()

	plain := sandpit.Session(t)
	s := sandpit.With().Cache().Session(t)
	assert.NotSame(t, plain, s, "a session built without Cache must not be cached")
	assert.NotSame(t, s, sandpit.Session(t), "Session without Cache must open a new session")
}

func TestCacheFirstOptionsApply(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	conn := func(*pgx.Conn) { calls.Add(1) }

	s := sandpit.With().Cache().ReadOnly().Conn(conn).Session(t)
	assert.Same(t, s, sandpit.With().Cache().Conn(conn).Session(t))
	assert.Equal(t, int32(1), calls.Load())

	_, err := s.DB().Exec("CREATE TEMP TABLE cache_first_options (id int)")
	require.Error(t, err, "the session must stay read-only")
}

func TestCacheConcurrentCalls(t *testing.T) {
	t.Parallel()

	const n = 10
	b := sandpit.With().Cache()
	got := make([]*pgsandpit.Session, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { got[i] = b.Session(t) })
	}
	wg.Wait()

	for _, s := range got {
		assert.Same(t, got[0], s)
	}
}

func TestCacheSeparatesTests(t *testing.T) {
	t.Parallel()

	b := sandpit.With().Cache()
	parent := b.Session(t)
	parentPID := backendPID(t, parent)

	t.Run("subtest", func(t *testing.T) {
		s := b.Session(t)
		assert.NotSame(t, parent, s)
		assert.NotEqual(t, parentPID, backendPID(t, s))
	})

	assert.Same(t, parent, b.Session(t), "a subtest must not replace its parent's session")
}

func TestCacheSeparatesSandpits(t *testing.T) {
	t.Parallel()

	other := pgsandpit.New(dsn())
	t.Cleanup(func() { assert.NoError(t, other.Close()) })

	assert.NotSame(t, sandpit.With().Cache().Session(t), other.With().Cache().Session(t))
}

func TestCacheForgottenAfterSessionCloses(t *testing.T) {
	t.Parallel()

	var s *pgsandpit.Session
	t.Run("subtest", func(t *testing.T) {
		// Runs after the session is rolled back and forgotten, and gets a new one.
		t.Cleanup(func() {
			assert.NotSame(t, s, sandpit.With().Cache().Session(t))
		})
		s = sandpit.With().Cache().Session(t)
	})
}

func TestCacheFailsTestAfterFailedCall(t *testing.T) {
	if os.Getenv("PGSANDPIT_CHILD_CACHE") != "" {
		b := pgsandpit.New("postgres://localhost:notaport/postgres").With().Cache()
		done := make(chan struct{})
		go func() {
			defer close(done)
			b.Session(t)
		}()
		<-done
		b.Session(t)
		t.Log("Session returned")
		return
	}
	t.Parallel()

	out, err := runChild(t, "TestCacheFailsTestAfterFailedCall", "PGSANDPIT_CHILD_CACHE=1")
	require.Error(t, err)
	assert.Contains(t, out, "pgsandpit: cannot parse")
	assert.NotContains(t, out, "Session returned")
	assert.NotContains(t, out, "panic")
}
