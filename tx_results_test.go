package pgsandpit_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

func TestTxResults(t *testing.T) {
	t.Parallel()

	t.Run("empty without a transaction", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		_, err := s.DB().Exec(`SELECT 1`)
		require.NoError(t, err)
		_, err = s.DB().Exec(`SELECT * FROM no_such_table`)
		require.Error(t, err)

		assert.Empty(t, s.TxResults())
	})

	t.Run("commit", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxCommitted}, s.TxResults())
	})

	t.Run("rollback", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxRolledBack}, s.TxResults())
	})

	t.Run("rollback after commit is not recorded", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Error(t, tx.Rollback())

		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxCommitted}, s.TxResults())
	})

	t.Run("commit of an aborted transaction", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`SELECT * FROM no_such_table`)
		require.Error(t, err)
		require.ErrorIs(t, tx.Commit(), pgx.ErrTxCommitRollback)

		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxCommitFailed}, s.TxResults())
	})

	t.Run("canceled context", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.DB().BeginTx(ctx, nil)
		require.NoError(t, err)
		cancel()

		assert.Eventually(t, func() bool {
			return assert.ObjectsAreEqual([]pgsandpit.TxResult{pgsandpit.TxRolledBack}, s.TxResults())
		}, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("in order across every *sql.DB of the session", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		db := s.DB()
		ro := s.With().ReadOnly().DB()

		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		tx, err = ro.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		tx, err = db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		assert.Equal(t, []pgsandpit.TxResult{
			pgsandpit.TxCommitted,
			pgsandpit.TxRolledBack,
			pgsandpit.TxCommitted,
		}, s.TxResults())
	})

	t.Run("does not wait for an open transaction, which is not included", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		assert.Empty(t, s.TxResults())
	})

	t.Run("returns a copy", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		s.TxResults()[0] = pgsandpit.TxRolledBack

		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxCommitted}, s.TxResults())
	})

	t.Run("sessions do not share results", func(t *testing.T) {
		t.Parallel()
		s := sandpit.Session(t)
		other := sandpit.Session(t)
		tx, err := s.DB().Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		assert.Empty(t, other.TxResults())
	})
}

func TestTxResultString(t *testing.T) {
	t.Parallel()

	tests := map[pgsandpit.TxResult]string{
		pgsandpit.TxCommitted:    "TxCommitted",
		pgsandpit.TxRolledBack:   "TxRolledBack",
		pgsandpit.TxCommitFailed: "TxCommitFailed",
		pgsandpit.TxResult(99):   "TxResult(99)",
	}
	for r, want := range tests {
		assert.Equal(t, want, r.String())
	}
}
