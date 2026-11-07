package pgsandpit_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

var sandpit *pgsandpit.Sandpit

func dsn() string {
	if v := os.Getenv("TEST_PGSANDPIT_DSN"); v != "" {
		return v
	}
	return "postgres://postgres@localhost:15432/postgres?sslmode=disable"
}

func TestMain(m *testing.M) {
	// A child process (see runChild) uses the schema its parent set up, which
	// the parent's running tests hold locks on.
	if os.Getenv(childEnv) == "" {
		if err := setupSchema(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	sandpit = pgsandpit.New(dsn())
	code := m.Run()
	sandpit.Close()
	os.Exit(code)
}

func setupSchema() error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn())
	if err != nil {
		return fmt.Errorf("failed to connect (run `docker compose up -d`): %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `
		DROP TABLE IF EXISTS users;
		CREATE TABLE users (
			id    serial PRIMARY KEY,
			email text NOT NULL UNIQUE
		);
		INSERT INTO users (email) VALUES ('committed@example.com');
	`)
	if err != nil {
		return fmt.Errorf("failed to set up the schema: %w", err)
	}
	return nil
}

type querier interface {
	QueryRow(string, ...any) *sql.Row
}

func countUsers(t *testing.T, q querier, email string) int {
	t.Helper()
	var n int
	require.NoError(t, q.QueryRow(`SELECT count(*) FROM users WHERE email = $1`, email).Scan(&n))
	return n
}

func TestRolledBackAfterTest(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		db := sandpit.Session(t).DB()
		_, err := db.Exec(`INSERT INTO users (email) VALUES ('rolledback@example.com')`)
		require.NoError(t, err)
		assert.Equal(t, 1, countUsers(t, db, "rolledback@example.com"))
	})

	t.Run("after", func(t *testing.T) {
		db := sandpit.Session(t).DB()
		assert.Equal(t, 0, countUsers(t, db, "rolledback@example.com"))
		assert.Equal(t, 1, countUsers(t, db, "committed@example.com"))
	})
}

func TestSessionsAreIsolated(t *testing.T) {
	t.Parallel()
	db1 := sandpit.Session(t).DB()
	db2 := sandpit.Session(t).DB()

	_, err := db1.Exec(`INSERT INTO users (email) VALUES ('isolated@example.com')`)
	require.NoError(t, err)
	assert.Equal(t, 1, countUsers(t, db1, "isolated@example.com"))
	assert.Equal(t, 0, countUsers(t, db2, "isolated@example.com"))
}

func TestDBsShareSession(t *testing.T) {
	t.Parallel()
	s := sandpit.Session(t)
	db1 := s.DB()
	db2 := s.DB()

	_, err := db1.Exec(`INSERT INTO users (email) VALUES ('shared@example.com')`)
	require.NoError(t, err)
	assert.Equal(t, 1, countUsers(t, db2, "shared@example.com"))

	// Closing a *sql.DB does not end the session.
	require.NoError(t, db1.Close())
	assert.Equal(t, 1, countUsers(t, db2, "shared@example.com"))
}

func TestOpenRowsDoNotHoldSession(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	rows, err := db.Query(`SELECT email FROM users`)
	require.NoError(t, err)
	defer rows.Close()

	_, err = db.Exec(`INSERT INTO users (email) VALUES ('openrows@example.com')`)
	require.NoError(t, err)
	assert.True(t, rows.Next())
}

func TestFailedStatementDoesNotAbortSession(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	_, err := db.Exec(`INSERT INTO users (email) VALUES ('dup@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO users (email) VALUES ('dup@example.com')`)
	require.ErrorContains(t, err, "duplicate key")

	assert.Equal(t, 1, countUsers(t, db, "dup@example.com"))
}

func TestTx(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	t.Run("commit", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('commit@example.com')`)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		assert.Equal(t, 1, countUsers(t, db, "commit@example.com"))
	})

	t.Run("rollback", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('rollback@example.com')`)
		require.NoError(t, err)
		assert.Equal(t, 1, countUsers(t, tx, "rollback@example.com"))
		require.NoError(t, tx.Rollback())
		assert.Equal(t, 0, countUsers(t, db, "rollback@example.com"))
	})

	t.Run("commit of an aborted tx rolls back", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('aborted@example.com')`)
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('aborted@example.com')`)
		require.ErrorContains(t, err, "duplicate key")
		_, err = tx.Exec(`SELECT 1`)
		require.ErrorContains(t, err, "current transaction is aborted")

		require.ErrorIs(t, tx.Commit(), pgx.ErrTxCommitRollback)
		assert.Equal(t, 0, countUsers(t, db, "aborted@example.com"))
	})

	t.Run("prepared statement", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		st, err := tx.Prepare(`INSERT INTO users (email) VALUES ($1)`)
		require.NoError(t, err)
		_, err = st.Exec("prepared1@example.com")
		require.NoError(t, err)
		_, err = st.Exec("prepared2@example.com")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		assert.Equal(t, 1, countUsers(t, db, "prepared2@example.com"))
	})
}

func TestConcurrentTxsAreSerialized(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	const n = 8
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			tx, err := db.Begin()
			if !assert.NoError(t, err) {
				return
			}
			defer tx.Rollback()
			_, err = tx.Exec(`INSERT INTO users (email) VALUES ($1)`, fmt.Sprintf("concurrent%d@example.com", i))
			if !assert.NoError(t, err) {
				return
			}
			if i%2 == 0 {
				assert.NoError(t, tx.Commit())
			}
		})
	}
	wg.Wait()

	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE email LIKE 'concurrent%'`).Scan(&count))
	assert.Equal(t, n/2, count)
}

func TestTimeout(t *testing.T) {
	t.Parallel()
	s := sandpit.Session(t)
	db := s.With().Timeout(100 * time.Millisecond).DB()

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO users (email) VALUES ('timeout@example.com')`)
	require.NoError(t, err)

	// Using db outside the open tx in the same goroutine would wait forever.
	start := time.Now()
	_, err = db.Exec(`SELECT 1`)
	require.ErrorIs(t, err, pgsandpit.ErrTimeout)
	assert.Contains(t, err.Error(), "pgsandpit_test.TestTimeout")
	assert.Less(t, time.Since(start), 5*time.Second)

	require.NoError(t, tx.Commit())
	assert.Equal(t, 1, countUsers(t, db, "timeout@example.com"))
}

func TestCanceledTxIsRolledBack(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO users (email) VALUES ('canceled@example.com')`)
	require.NoError(t, err)
	cancel()

	// database/sql rolls the tx back, which frees the session.
	assert.Equal(t, 0, countUsers(t, db, "canceled@example.com"))
}

func TestCanceledQueryKeepsSession(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	_, err := db.Exec(`INSERT INTO users (email) VALUES ('keep@example.com')`)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = db.ExecContext(ctx, `SELECT pg_sleep(10)`)
	require.Error(t, err)

	assert.Equal(t, 1, countUsers(t, db, "keep@example.com"))
}

func TestReadOnly(t *testing.T) {
	t.Parallel()
	s := sandpit.Session(t)
	b := s.With()
	ro := b.ReadOnly().DB()
	db := b.DB() // b itself is not read-only

	_, err := db.Exec(`INSERT INTO users (email) VALUES ('readonly@example.com')`)
	require.NoError(t, err)
	assert.Equal(t, 1, countUsers(t, ro, "readonly@example.com"))

	_, err = ro.Exec(`INSERT INTO users (email) VALUES ('ro-write@example.com')`)
	require.ErrorContains(t, err, "read-only transaction")

	// The rejected write failed alone, and db can still write.
	assert.Equal(t, 1, countUsers(t, ro, "readonly@example.com"))
	_, err = db.Exec(`INSERT INTO users (email) VALUES ('after-ro@example.com')`)
	require.NoError(t, err)
	assert.Equal(t, 0, countUsers(t, db, "ro-write@example.com"))

	t.Run("write in a function", func(t *testing.T) {
		_, err := ro.Exec(`SELECT nextval('users_id_seq')`)
		require.ErrorContains(t, err, "read-only transaction")
	})

	t.Run("in a tx", func(t *testing.T) {
		tx, err := ro.Begin()
		require.NoError(t, err)
		assert.Equal(t, 1, countUsers(t, tx, "readonly@example.com"))
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('ro-tx@example.com')`)
		require.ErrorContains(t, err, "read-only transaction")
		require.ErrorIs(t, tx.Commit(), pgx.ErrTxCommitRollback)

		_, err = db.Exec(`INSERT INTO users (email) VALUES ('after-ro-tx@example.com')`)
		require.NoError(t, err)
	})

	t.Run("tx that only reads", func(t *testing.T) {
		tx, err := ro.Begin()
		require.NoError(t, err)
		assert.Equal(t, 1, countUsers(t, tx, "readonly@example.com"))
		require.NoError(t, tx.Commit())

		_, err = db.Exec(`INSERT INTO users (email) VALUES ('after-ro-read-tx@example.com')`)
		require.NoError(t, err)
	})
}

func TestUseAfterSessionEnds(t *testing.T) {
	var db *sql.DB
	t.Run("session", func(t *testing.T) {
		db = sandpit.Session(t).DB()
	})
	_, err := db.Exec(`SELECT 1`)
	require.Error(t, err)
}
