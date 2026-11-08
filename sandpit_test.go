package pgsandpit_test

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

// childEnv is set in the environment of a child process started by runChild.
const childEnv = "PGSANDPIT_CHILD"

// runChild runs the top-level test name in a child process with env added, for
// tests whose subject is a test failing. It returns the child's output.
func runChild(t *testing.T, name string, env ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	args := []string{"-test.run=^" + name + "$", "-test.v"}
	// Under `go test -cover`, write the child's coverage where go test collects
	// this process's, so that what the child runs is counted too.
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+f.Value.String())
	}
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(append(os.Environ(), childEnv+"=1"), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestSessionFailsTestWithoutDatabase(t *testing.T) {
	if dsn, ok := os.LookupEnv("PGSANDPIT_CHILD_DSN"); ok {
		pgsandpit.New(dsn).Session(t)
		t.Log("Session returned")
		return
	}
	t.Parallel()

	tests := map[string]struct {
		dsn  string
		want string
	}{
		"invalid dsn": {
			dsn:  "postgres://localhost:notaport/postgres",
			want: "pgsandpit: cannot parse",
		},
		"no server": {
			dsn:  "postgres://postgres@127.0.0.1:1/postgres?connect_timeout=5",
			want: "pgsandpit: failed to connect",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, err := runChild(t, "TestSessionFailsTestWithoutDatabase", "PGSANDPIT_CHILD_DSN="+tt.dsn)
			require.Error(t, err)
			assert.Contains(t, out, tt.want)
			assert.NotContains(t, out, "Session returned")
		})
	}
}

func TestLeakedTxFailsTest(t *testing.T) {
	if os.Getenv("PGSANDPIT_CHILD_LEAK") != "" {
		// The end of the test waits for the session as long as its timeout.
		db := sandpit.With().Timeout(100 * time.Millisecond).Session(t).DB()
		_, err := db.Begin() // never committed or rolled back
		require.NoError(t, err)
		return
	}
	t.Parallel()

	start := time.Now()
	out, err := runChild(t, "TestLeakedTxFailsTest", "PGSANDPIT_CHILD_LEAK=1")
	require.Error(t, err)
	assert.Less(t, time.Since(start), pgsandpit.DefaultTimeout)
	assert.Contains(t, out, "pgsandpit: the session is still in use at the end of the test")
	assert.Contains(t, out, "after 100ms")
	assert.Contains(t, out, "pgsandpit_test.TestLeakedTxFailsTest")
}

func TestSandpitClose(t *testing.T) {
	t.Parallel()

	t.Run("before any session", func(t *testing.T) {
		assert.NoError(t, pgsandpit.New(dsn()).Close())
	})

	t.Run("after sessions", func(t *testing.T) {
		sp := pgsandpit.New(dsn())
		t.Run("session", func(t *testing.T) {
			db := sp.Session(t).DB()
			require.NoError(t, db.Ping())
		})
		assert.NoError(t, sp.Close())
	})
}

func TestDriverCannotOpenConnections(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	_, err := db.Driver().Open(dsn())
	require.ErrorContains(t, err, "Session.DB")
}

func TestTerminatedSessionFailsTest(t *testing.T) {
	if os.Getenv("PGSANDPIT_CHILD_TERMINATE") != "" {
		db := sandpit.Session(t).DB()
		_, err := db.Exec(`SELECT pg_terminate_backend(pg_backend_pid())`)
		assert.Error(t, err)

		// Calls after the connection is gone fail instead of waiting.
		_, err = db.Exec(`SELECT 1`)
		assert.Error(t, err)
		_, err = db.Begin()
		assert.Error(t, err)
		return
	}
	t.Parallel()

	out, err := runChild(t, "TestTerminatedSessionFailsTest", "PGSANDPIT_CHILD_TERMINATE=1")
	require.Error(t, err)
	assert.Contains(t, out, "pgsandpit: failed to roll back the session transaction")
	// The child's own assertions passed: the only failure is the cleanup's.
	assert.NotContains(t, out, "Error Trace")
}

func TestNewWithConfig(t *testing.T) {
	t.Parallel()

	showSetting := func(t *testing.T, sp *pgsandpit.Sandpit, name string) string {
		t.Helper()
		var v string
		db := sp.Session(t).DB()
		require.NoError(t, db.QueryRow(`SELECT current_setting($1)`, name).Scan(&v))
		return v
	}

	t.Run("config", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(dsn())
		require.NoError(t, err)
		cfg.RuntimeParams["application_name"] = "pgsandpit-config"
		sp := pgsandpit.NewWithConfig(cfg)
		defer sp.Close()

		// Changing cfg afterwards does not change the Sandpit.
		cfg.RuntimeParams["application_name"] = "changed"

		assert.Equal(t, "pgsandpit-config", showSetting(t, sp, "application_name"))
	})

	t.Run("options", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(dsn())
		require.NoError(t, err)
		sp := pgsandpit.NewWithConfig(cfg,
			stdlib.OptionAfterConnect(func(ctx context.Context, c *pgx.Conn) error {
				_, err := c.Exec(ctx, `SET TIME ZONE 'Asia/Tokyo'`)
				return err
			}),
		)
		defer sp.Close()

		assert.Equal(t, "Asia/Tokyo", showSetting(t, sp, "TimeZone"))
	})

	t.Run("a canceled context does not close the connection", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(dsn())
		require.NoError(t, err)
		// pgx's default, which closes the connection; pgsandpit replaces it.
		cfg.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
			return &pgconn.DeadlineContextWatcherHandler{Conn: pgConn.Conn()}
		}
		sp := pgsandpit.NewWithConfig(cfg)
		defer sp.Close()

		t.Run("session", func(t *testing.T) {
			db := sp.Session(t).DB()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, err := db.ExecContext(ctx, `SELECT pg_sleep(10)`)
			require.Error(t, err)
			assert.NoError(t, db.Ping())
		})
	})
}
