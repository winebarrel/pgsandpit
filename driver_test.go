package pgsandpit_test

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// database/sql calls only the context methods of the driver. The others are
// reachable through sql.Conn.Raw, and behave the same.
func TestDriverMethodsWithoutContext(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()
	ctx := context.Background()

	c, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c.Close()

	err = c.Raw(func(dc any) error {
		conn := dc.(driver.Conn)

		tx, err := conn.Begin() //nolint:staticcheck // the method under test
		require.NoError(t, err)

		insert, err := conn.Prepare(`INSERT INTO users (email) VALUES ($1)`)
		require.NoError(t, err)
		defer insert.Close()
		res, err := insert.Exec([]driver.Value{"driver@example.com"}) //nolint:staticcheck // the method under test
		require.NoError(t, err)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)

		sel, err := conn.Prepare(`SELECT email FROM users WHERE email = $1`)
		require.NoError(t, err)
		defer sel.Close()
		rows, err := sel.Query([]driver.Value{"driver@example.com"}) //nolint:staticcheck // the method under test
		require.NoError(t, err)
		defer rows.Close()
		assert.Equal(t, []string{"email"}, rows.Columns())
		dest := make([]driver.Value, 1)
		require.NoError(t, rows.Next(dest))
		assert.Equal(t, "driver@example.com", dest[0])
		assert.ErrorIs(t, rows.Next(dest), io.EOF)

		require.NoError(t, tx.Commit())
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, 1, countUsers(t, db, "driver@example.com"))
}
