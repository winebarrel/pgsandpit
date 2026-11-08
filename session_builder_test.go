package pgsandpit_test

import (
	"database/sql/driver"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

func TestSessionBuilderReadOnly(t *testing.T) {
	t.Parallel()

	t.Run("makes every *sql.DB read-only", func(t *testing.T) {
		s := sandpit.With().ReadOnly().Session(t)

		_, err := s.DB().Exec(`INSERT INTO users (email) VALUES ('session-ro1@example.com')`)
		require.ErrorContains(t, err, "read-only transaction")
		_, err = s.With().Timeout(time.Second).DB().Exec(`INSERT INTO users (email) VALUES ('session-ro2@example.com')`)
		require.ErrorContains(t, err, "read-only transaction")

		tx, err := s.DB().Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('session-ro3@example.com')`)
		require.ErrorContains(t, err, "read-only transaction")
		require.NoError(t, tx.Rollback())
	})

	t.Run("does not affect Conn", func(t *testing.T) {
		s := sandpit.With().ReadOnly().Conn(func(t *testing.T, c *pgx.Conn) {
			_, err := c.Exec(t.Context(), `INSERT INTO users (email) VALUES ('session-ro-conn@example.com')`)
			require.NoError(t, err)
		}).Session(t)

		assert.Equal(t, 1, countUsers(t, s.DB(), "session-ro-conn@example.com"))
	})

	t.Run("does not change the builder", func(t *testing.T) {
		b := sandpit.With()
		_ = b.ReadOnly()

		_, err := b.Session(t).DB().Exec(`INSERT INTO users (email) VALUES ('session-ro-builder@example.com')`)
		require.NoError(t, err)
	})
}

func TestSessionBuilderTimeout(t *testing.T) {
	t.Parallel()

	t.Run("is the default of the *sql.DB values", func(t *testing.T) {
		s := sandpit.With().Timeout(100 * time.Millisecond).Session(t)
		db := s.DB()
		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		start := time.Now()
		_, err = db.Exec(`SELECT 1`)
		require.ErrorIs(t, err, pgsandpit.ErrTimeout)
		assert.Contains(t, err.Error(), "after 100ms")
		assert.Less(t, time.Since(start), 5*time.Second)
	})

	t.Run("is overridden by DBBuilder.Timeout", func(t *testing.T) {
		s := sandpit.With().Timeout(100 * time.Millisecond).Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = s.With().Timeout(200 * time.Millisecond).DB().Exec(`SELECT 1`)
		require.ErrorIs(t, err, pgsandpit.ErrTimeout)
		assert.Contains(t, err.Error(), "after 200ms")
	})

	t.Run("does not change the builder", func(t *testing.T) {
		b := sandpit.With()
		_ = b.Timeout(100 * time.Millisecond)
		s := b.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = s.With().Timeout(200 * time.Millisecond).DB().Exec(`SELECT 1`)
		require.ErrorIs(t, err, pgsandpit.ErrTimeout)
		assert.Contains(t, err.Error(), "after 200ms")
	})
}

func TestSessionBuilderConnector(t *testing.T) {
	t.Parallel()

	t.Run("is applied to every *sql.DB", func(t *testing.T) {
		var hooks []*hookConnector
		s := sandpit.With().Connector(func(c driver.Connector) driver.Connector {
			h := &hookConnector{Connector: c}
			hooks = append(hooks, h)
			return h
		}).Session(t)

		_, err := s.DB().Exec(`SELECT 1`)
		require.NoError(t, err)
		_, err = s.With().ReadOnly().DB().Exec(`SELECT 2`)
		require.NoError(t, err)

		require.Len(t, hooks, 2)
		assert.Equal(t, []string{": SELECT 1"}, hooks[0].queries)
		assert.Equal(t, []string{": SELECT 2"}, hooks[1].queries)
	})

	t.Run("is wrapped by DBBuilder.Connector", func(t *testing.T) {
		sessionHook := &hookConnector{}
		s := sandpit.With().Connector(func(c driver.Connector) driver.Connector {
			sessionHook.Connector = c
			return sessionHook
		}).Session(t)

		dbHook := &hookConnector{}
		_, err := s.With().Connector(func(c driver.Connector) driver.Connector {
			// Given what the session's function returned.
			assert.Same(t, sessionHook, c)
			dbHook.Connector = c
			return dbHook
		}).DB().Exec(`SELECT 1`)
		require.NoError(t, err)

		assert.Equal(t, []string{": SELECT 1"}, dbHook.queries)
		assert.Equal(t, []string{": SELECT 1"}, sessionHook.queries)
	})

	t.Run("does not change the builder", func(t *testing.T) {
		b := sandpit.With()
		called := false
		_ = b.Connector(func(c driver.Connector) driver.Connector {
			called = true
			return c
		})

		require.NoError(t, b.Session(t).DB().Ping())
		assert.False(t, called)
	})
}

func TestSessionBuilderConn(t *testing.T) {
	t.Parallel()

	t.Run("runs once when the session is made", func(t *testing.T) {
		calls := 0
		b := sandpit.With().Conn(func(t *testing.T, c *pgx.Conn) {
			calls++
			_, err := c.Exec(t.Context(), `INSERT INTO users (email) VALUES ('session-conn@example.com')`)
			require.NoError(t, err)
		})
		assert.Equal(t, 0, calls)

		s := b.Session(t)
		assert.Equal(t, 1, calls)

		assert.Equal(t, 1, countUsers(t, s.DB(), "session-conn@example.com"))
		assert.Equal(t, 1, countUsers(t, s.With().ReadOnly().DB(), "session-conn@example.com"))
		assert.Equal(t, 1, calls)
	})

	t.Run("sets a parameter for every *sql.DB of the session", func(t *testing.T) {
		s := sandpit.With().Conn(func(t *testing.T, c *pgx.Conn) {
			_, err := c.Exec(t.Context(), `SET application_name = 'pgsandpit_session_conn'`)
			require.NoError(t, err)
		}).Session(t)

		var name string
		require.NoError(t, s.DB().QueryRow(`SHOW application_name`).Scan(&name))
		assert.Equal(t, "pgsandpit_session_conn", name)
		require.NoError(t, s.With().ReadOnly().DB().QueryRow(`SHOW application_name`).Scan(&name))
		assert.Equal(t, "pgsandpit_session_conn", name)
	})

	t.Run("does not change the builder", func(t *testing.T) {
		b := sandpit.With()
		called := false
		_ = b.Conn(func(*testing.T, *pgx.Conn) { called = true })

		require.NoError(t, b.Session(t).DB().Ping())
		assert.False(t, called)
	})
}

func TestBuilderConnIsGivenTheSessionsTest(t *testing.T) {
	t.Parallel()

	var got []*testing.T
	conn := func(t *testing.T, _ *pgx.Conn) { got = append(got, t) }

	s := sandpit.With().Conn(conn).Session(t)
	t.Run("from a subtest", func(*testing.T) {
		s.With().Conn(conn).DB()
	})

	// SessionBuilder.Conn and DBBuilder.Conn.
	require.Len(t, got, 2)
	for _, g := range got {
		assert.Same(t, t, g)
	}
}

func TestSessionBuilderConnFailsTest(t *testing.T) {
	if os.Getenv("PGSANDPIT_CHILD_SESSION_CONN") != "" {
		sandpit.With().Conn(func(*testing.T, *pgx.Conn) {
			require.Fail(t, "failed in the callback")
		}).Session(t)
		t.Log("Session returned")
		return
	}
	t.Parallel()

	out, err := runChild(t, "TestSessionBuilderConnFailsTest", "PGSANDPIT_CHILD_SESSION_CONN=1")
	require.Error(t, err)
	assert.Contains(t, out, "failed in the callback")
	assert.NotContains(t, out, "Session returned")
	// The session is still rolled back and closed.
	assert.NotContains(t, out, "failed to roll back")
	assert.NotContains(t, out, "still in use")
}
