package pgsandpit_test

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

func TestInterleave(t *testing.T) {
	t.Parallel()

	t.Run("a statement outside an open transaction does not wait for it", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Timeout(time.Second).Session(t)
		db := s.DB()

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('interleave-tx@example.com')`)
		require.NoError(t, err)

		// Without Interleave, this would wait for tx, which this goroutine holds.
		assert.Equal(t, 1, countUsers(t, db, "interleave-tx@example.com"))
		_, err = db.Exec(`INSERT INTO users (email) VALUES ('interleave-db@example.com')`)
		require.NoError(t, err)
		assert.Equal(t, 1, countUsers(t, tx, "interleave-db@example.com"))

		require.NoError(t, tx.Commit())
		assert.Equal(t, 1, countUsers(t, db, "interleave-tx@example.com"))
		assert.Equal(t, 1, countUsers(t, db, "interleave-db@example.com"))
		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxCommitted}, s.TxResults())
	})

	t.Run("a statement outside a transaction is rolled back with it", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Session(t)
		db := s.DB()

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO users (email) VALUES ('interleave-rollback@example.com')`)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		assert.Equal(t, 0, countUsers(t, db, "interleave-rollback@example.com"))
		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxRolledBack}, s.TxResults())
	})

	t.Run("a failing statement outside a transaction fails alone", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Session(t)
		db := s.DB()

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('interleave-alone@example.com')`)
		require.NoError(t, err)

		_, err = db.Exec(`SELECT * FROM no_such_table`)
		require.Error(t, err)

		require.NoError(t, tx.Commit())
		assert.Equal(t, 1, countUsers(t, db, "interleave-alone@example.com"))
		assert.Equal(t, []pgsandpit.TxResult{pgsandpit.TxCommitted}, s.TxResults())
	})

	t.Run("a read-only statement outside a transaction", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Session(t)
		db := s.DB()
		ro := s.With().ReadOnly().DB()

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('interleave-ro@example.com')`)
		require.NoError(t, err)

		assert.Equal(t, 1, countUsers(t, ro, "interleave-ro@example.com"))
		_, err = ro.Exec(`INSERT INTO users (email) VALUES ('interleave-ro-write@example.com')`)
		require.Error(t, err)

		// The transaction is still writable and usable.
		_, err = tx.Exec(`INSERT INTO users (email) VALUES ('interleave-ro-after@example.com')`)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		assert.Equal(t, 1, countUsers(t, db, "interleave-ro-after@example.com"))
		assert.Equal(t, 0, countUsers(t, db, "interleave-ro-write@example.com"))
	})

	t.Run("a transaction begun while another is open nests inside it", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Timeout(time.Second).Session(t)
		db := s.DB()

		outer, err := db.Begin()
		require.NoError(t, err)
		_, err = outer.Exec(`INSERT INTO users (email) VALUES ('interleave-outer@example.com')`)
		require.NoError(t, err)

		// Without Interleave, this would wait for outer, which this goroutine holds.
		inner, err := db.Begin()
		require.NoError(t, err)
		assert.Equal(t, 1, countUsers(t, inner, "interleave-outer@example.com"))
		_, err = inner.Exec(`INSERT INTO users (email) VALUES ('interleave-inner@example.com')`)
		require.NoError(t, err)
		require.NoError(t, inner.Commit())
		assert.Equal(t, 1, countUsers(t, outer, "interleave-inner@example.com"))

		rolledBack, err := db.Begin()
		require.NoError(t, err)
		_, err = rolledBack.Exec(`INSERT INTO users (email) VALUES ('interleave-inner-rollback@example.com')`)
		require.NoError(t, err)
		require.NoError(t, rolledBack.Rollback())
		assert.Equal(t, 0, countUsers(t, outer, "interleave-inner-rollback@example.com"))

		require.NoError(t, outer.Commit())
		assert.Equal(t, 1, countUsers(t, db, "interleave-outer@example.com"))
		assert.Equal(t, 1, countUsers(t, db, "interleave-inner@example.com"))
		assert.Equal(t, []pgsandpit.TxResult{
			pgsandpit.TxCommitted, pgsandpit.TxRolledBack, pgsandpit.TxCommitted,
		}, s.TxResults())
	})

	t.Run("a nested transaction is rolled back with the outer one", func(t *testing.T) {
		t.Parallel()
		db := sandpit.With().Interleave().Session(t).DB()

		outer, err := db.Begin()
		require.NoError(t, err)
		inner, err := db.Begin()
		require.NoError(t, err)
		_, err = inner.Exec(`INSERT INTO users (email) VALUES ('interleave-nested-rollback@example.com')`)
		require.NoError(t, err)
		require.NoError(t, inner.Commit())
		require.NoError(t, outer.Rollback())

		assert.Equal(t, 0, countUsers(t, db, "interleave-nested-rollback@example.com"))
	})

	t.Run("committing the outer transaction first rolls back both", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Session(t)
		db := s.DB()

		outer, err := db.Begin()
		require.NoError(t, err)
		_, err = outer.Exec(`INSERT INTO users (email) VALUES ('interleave-order-outer@example.com')`)
		require.NoError(t, err)
		inner, err := db.Begin()
		require.NoError(t, err)
		_, err = inner.Exec(`INSERT INTO users (email) VALUES ('interleave-order-inner@example.com')`)
		require.NoError(t, err)

		err = outer.Commit()
		require.ErrorIs(t, err, pgsandpit.ErrTxOrder)
		require.ErrorIs(t, inner.Commit(), pgsandpit.ErrTxOrder)

		assert.Equal(t, 0, countUsers(t, db, "interleave-order-outer@example.com"))
		assert.Equal(t, 0, countUsers(t, db, "interleave-order-inner@example.com"))
		assert.Equal(t, []pgsandpit.TxResult{
			pgsandpit.TxCommitFailed, pgsandpit.TxCommitFailed,
		}, s.TxResults())

		// The session is still usable.
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	})

	t.Run("rolling back the outer transaction first ends the inner one", func(t *testing.T) {
		t.Parallel()
		db := sandpit.With().Interleave().Session(t).DB()

		outer, err := db.Begin()
		require.NoError(t, err)
		inner, err := db.Begin()
		require.NoError(t, err)
		_, err = inner.Exec(`INSERT INTO users (email) VALUES ('interleave-order-rollback@example.com')`)
		require.NoError(t, err)

		require.NoError(t, outer.Rollback())
		_, err = inner.Exec(`SELECT 1`)
		require.ErrorIs(t, err, pgsandpit.ErrTxOrder)
		require.ErrorIs(t, inner.Rollback(), pgsandpit.ErrTxOrder)
		assert.Equal(t, 0, countUsers(t, db, "interleave-order-rollback@example.com"))
	})

	t.Run("statements run one at a time", func(t *testing.T) {
		t.Parallel()
		db := sandpit.With().Interleave().Session(t).DB()

		tx, err := db.Begin()
		require.NoError(t, err)

		const n = 10
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				_, err := db.Exec(`INSERT INTO users (email) VALUES ($1)`, emailN("interleave-concurrent", i))
				assert.NoError(t, err)
			})
			wg.Go(func() {
				_, err := tx.Exec(`INSERT INTO users (email) VALUES ($1)`, emailN("interleave-concurrent-tx", i))
				assert.NoError(t, err)
			})
		}
		wg.Wait()
		require.NoError(t, tx.Commit())

		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE email LIKE 'interleave-concurrent%'`).Scan(&count))
		assert.Equal(t, 2*n, count)
	})

	t.Run("overlapping transactions in goroutines end in order or fail with ErrTxOrder", func(t *testing.T) {
		t.Parallel()
		db := sandpit.With().Interleave().Session(t).DB()

		const n = 10
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				tx, err := db.Begin()
				if !assert.NoError(t, err) {
					return
				}
				if _, err := tx.Exec(`INSERT INTO users (email) VALUES ($1)`, emailN("interleave-overlap", i)); err != nil {
					assert.ErrorIs(t, tx.Rollback(), pgsandpit.ErrTxOrder)
					return
				}
				if err := tx.Commit(); err != nil {
					assert.ErrorIs(t, err, pgsandpit.ErrTxOrder)
				}
			})
		}
		wg.Wait()

		// The session is still usable.
		_, err := db.Exec(`SELECT 1`)
		require.NoError(t, err)
	})

	t.Run("without Interleave a statement outside the transaction waits", func(t *testing.T) {
		t.Parallel()
		db := sandpit.With().Timeout(100 * time.Millisecond).Session(t).DB()

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = db.Exec(`SELECT 1`)
		require.ErrorIs(t, err, pgsandpit.ErrTimeout)
	})

	t.Run("a wait for a statement does not point at a transaction", func(t *testing.T) {
		t.Parallel()
		s := sandpit.With().Interleave().Session(t)
		slow := s.DB()
		db := s.With().Timeout(100 * time.Millisecond).DB()

		tx, err := slow.Begin()
		require.NoError(t, err)

		done := make(chan error)
		go func() {
			_, err := tx.Exec(`SELECT pg_sleep(1)`)
			done <- err
		}()
		time.Sleep(300 * time.Millisecond) // let the slow statement take the session

		_, err = db.Exec(`SELECT 1`)
		require.ErrorIs(t, err, pgsandpit.ErrTimeout)
		assert.NotContains(t, err.Error(), "begun at")

		require.NoError(t, <-done)
		require.NoError(t, tx.Commit())
	})
}

func emailN(prefix string, i int) string {
	return prefix + "-" + string(rune('a'+i)) + "@example.com"
}

func TestInterleaveLeakedTxFailsTest(t *testing.T) {
	if os.Getenv("PGSANDPIT_CHILD_INTERLEAVE_LEAK") != "" {
		db := sandpit.With().Interleave().Session(t).DB()
		_, err := db.Begin() // never committed or rolled back
		require.NoError(t, err)
		return
	}
	t.Parallel()

	start := time.Now()
	out, err := runChild(t, "TestInterleaveLeakedTxFailsTest", "PGSANDPIT_CHILD_INTERLEAVE_LEAK=1")
	require.Error(t, err)
	// The leaked transaction does not hold the session, so the end of the test
	// does not wait for it.
	assert.Less(t, time.Since(start), pgsandpit.DefaultTimeout)
	assert.Contains(t, out, "pgsandpit: a transaction is still open at the end of the test")
	assert.Contains(t, out, "pgsandpit_test.TestInterleaveLeakedTxFailsTest")
}
