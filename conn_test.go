package pgsandpit_test

import (
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBBuilderConn(t *testing.T) {
	t.Parallel()

	t.Run("runs once on the session when the *sql.DB is made", func(t *testing.T) {
		s := sandpit.Session(t)
		calls := 0
		b := s.With().Conn(func(c *pgx.Conn) {
			calls++
			_, err := c.CopyFrom(t.Context(), pgx.Identifier{"users"}, []string{"email"},
				pgx.CopyFromRows([][]any{{"copy1@example.com"}, {"copy2@example.com"}}))
			require.NoError(t, err)
		})
		assert.Equal(t, 0, calls)

		db := b.DB()
		assert.Equal(t, 1, calls)

		// What the function wrote is in the session.
		assert.Equal(t, 1, countUsers(t, db, "copy1@example.com"))
		assert.Equal(t, 1, countUsers(t, s.DB(), "copy2@example.com"))
		assert.Equal(t, 1, calls)
	})

	t.Run("does not change the builder", func(t *testing.T) {
		s := sandpit.Session(t)
		b := s.With()
		called := false
		_ = b.Conn(func(*pgx.Conn) { called = true })

		require.NoError(t, b.DB().Ping())
		assert.False(t, called)
	})

	t.Run("waits for the session", func(t *testing.T) {
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			s.With().Conn(func(c *pgx.Conn) {
				var n int
				assert.NoError(t, c.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE email = 'waited@example.com'`).Scan(&n))
				assert.Equal(t, 1, n)
			}).DB()
		}()

		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('waited@example.com')`)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, tx.Commit())
		<-done
	})
}

func TestDBBuilderConnFailsTest(t *testing.T) {
	switch os.Getenv("PGSANDPIT_CHILD_CONN") {
	case "":
	case "assertion":
		s := sandpit.Session(t)
		s.With().Conn(func(*pgx.Conn) {
			require.Fail(t, "failed in the callback")
		}).DB()
		t.Log("DB returned")
		return
	case "timeout":
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		defer tx.Rollback()
		s.With().Timeout(100 * time.Millisecond).Conn(func(*pgx.Conn) {}).DB()
		t.Log("DB returned")
		return
	}
	t.Parallel()

	tests := map[string]string{
		// Only the assertion fails the test: the session is still usable at its end.
		"assertion": "failed in the callback",
		"timeout":   "pgsandpit: timed out waiting for the session",
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, err := runChild(t, "TestDBBuilderConnFailsTest", "PGSANDPIT_CHILD_CONN="+name)
			require.Error(t, err)
			assert.Contains(t, out, want)
			assert.NotContains(t, out, "DB returned")
			assert.NotContains(t, out, "pgsandpit: pgsandpit:")
			assert.NotContains(t, out, "failed to roll back")
			assert.NotContains(t, out, "still in use")
		})
	}
}
