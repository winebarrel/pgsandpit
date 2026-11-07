package pgsandpit_test

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"testing"
	"time"

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
		db := sandpit.Session(t).DB()
		_, err := db.Begin() // never committed or rolled back
		require.NoError(t, err)
		return
	}
	t.Parallel()

	out, err := runChild(t, "TestLeakedTxFailsTest", "PGSANDPIT_CHILD_LEAK=1")
	require.Error(t, err)
	assert.Contains(t, out, "pgsandpit: the session is still in use at the end of the test")
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
