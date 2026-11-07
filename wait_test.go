package pgsandpit_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

func TestBeginInsideTxTimesOut(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).With().Timeout(100 * time.Millisecond).DB()

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO users (email) VALUES ('nested@example.com')`)
	require.NoError(t, err)

	// A second transaction would wait for the first, which this goroutine holds.
	_, err = db.Begin()
	require.ErrorIs(t, err, pgsandpit.ErrTimeout)
	assert.Contains(t, err.Error(), "pgsandpit_test.TestBeginInsideTxTimesOut")

	require.NoError(t, tx.Commit())
	assert.Equal(t, 1, countUsers(t, db, "nested@example.com"))
}

func TestWaitEndsWithContext(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB() // DefaultTimeout

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = db.ExecContext(ctx, `SELECT 1`)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, pgsandpit.ErrTimeout)
	assert.Less(t, time.Since(start), pgsandpit.DefaultTimeout)
}

func TestTimeoutBoundsOnlyWait(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).With().Timeout(100 * time.Millisecond).DB()

	_, err := db.Exec(`SELECT pg_sleep(0.5)`)
	assert.NoError(t, err)
}

func TestTimeoutWhileStatementRuns(t *testing.T) {
	t.Parallel()
	s := sandpit.Session(t)
	slow := s.DB()
	db := s.With().Timeout(100 * time.Millisecond).DB()

	done := make(chan error)
	go func() {
		_, err := slow.Exec(`SELECT pg_sleep(1)`)
		done <- err
	}()
	time.Sleep(300 * time.Millisecond) // let the slow statement take the session

	_, err := db.Exec(`SELECT 1`)
	require.ErrorIs(t, err, pgsandpit.ErrTimeout)
	// No transaction holds the session, so there is no Begin to point at.
	assert.NotContains(t, err.Error(), "begun at")

	require.NoError(t, <-done)
	_, err = db.Exec(`SELECT 1`)
	assert.NoError(t, err)
}
