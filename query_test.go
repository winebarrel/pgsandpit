package pgsandpit_test

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" to compare against
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queryAll runs query and returns its column types and every row.
func queryAll(db *sql.DB, query string, args ...any) ([]*sql.ColumnType, [][]any, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	var all [][]any
	for rows.Next() {
		vals := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		all = append(all, vals)
	}
	return types, all, rows.Err()
}

func openPgx(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRowsMatchPgx(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()
	pgxDB := openPgx(t)

	const query = `
		SELECT
			i,
			i::int8 AS bigint,
			i * 1.5::float8 AS float,
			12.34::numeric(5,2) AS numeric,
			'text ' || i AS text,
			'varchar'::varchar(10) AS varchar,
			i % 2 = 0 AS bool,
			NULLIF(i, 2) AS nullable,
			'\xdead'::bytea AS bytea,
			'2024-01-02 03:04:05+00'::timestamptz AS timestamptz,
			'2024-01-02'::date AS date,
			'{"a": 1}'::jsonb AS jsonb,
			'00000000-0000-0000-0000-000000000001'::uuid AS uuid,
			ARRAY[i, i + 1] AS array
		FROM generate_series(1, 3) AS i`

	wantTypes, wantRows, err := queryAll(pgxDB, query)
	require.NoError(t, err)
	gotTypes, gotRows, err := queryAll(db, query)
	require.NoError(t, err)

	assert.Equal(t, wantRows, gotRows)
	require.Len(t, gotTypes, len(wantTypes))
	for i, want := range wantTypes {
		got := gotTypes[i]
		assert.Equal(t, want.Name(), got.Name())
		assert.Equal(t, want.DatabaseTypeName(), got.DatabaseTypeName(), want.Name())
		assert.Equal(t, want.ScanType(), got.ScanType(), want.Name())

		wantLen, wantOK := want.Length()
		gotLen, gotOK := got.Length()
		assert.Equal(t, [2]any{wantLen, wantOK}, [2]any{gotLen, gotOK}, want.Name())

		wantP, wantS, wantOK := want.DecimalSize()
		gotP, gotS, gotOK := got.DecimalSize()
		assert.Equal(t, [3]any{wantP, wantS, wantOK}, [3]any{gotP, gotS, gotOK}, want.Name())

		_, wantOK = want.Nullable()
		_, gotOK = got.Nullable()
		assert.Equal(t, wantOK, gotOK, want.Name())
	}
}

func TestArgsMatchPgx(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()
	pgxDB := openPgx(t)

	tests := map[string][]any{
		"positional": {"a", 1},
		"nil":        {nil, 1},
		// pgx binds sql.Named arguments by position, ignoring the names.
		"named": {sql.Named("y", "b"), sql.Named("x", 2)},
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			const query = `SELECT $1::text, $2::int`
			_, want, err := queryAll(pgxDB, query, args...)
			require.NoError(t, err)
			_, got, err := queryAll(db, query, args...)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestFailedQueryDoesNotAbortSession(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	_, err := db.Exec(`INSERT INTO users (email) VALUES ('failedquery@example.com')`)
	require.NoError(t, err)

	t.Run("syntax error", func(t *testing.T) {
		_, _, err := queryAll(db, `SELEC 1`)
		require.ErrorContains(t, err, "syntax error")
		assert.Equal(t, 1, countUsers(t, db, "failedquery@example.com"))
	})

	t.Run("error after some rows", func(t *testing.T) {
		_, _, err := queryAll(db, `SELECT 10 / (3 - i) FROM generate_series(1, 5) AS i`)
		require.ErrorContains(t, err, "division by zero")
		assert.Equal(t, 1, countUsers(t, db, "failedquery@example.com"))
	})
}

func TestPreparedStatement(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	insert, err := db.Prepare(`INSERT INTO users (email) VALUES ($1)`)
	require.NoError(t, err)
	defer insert.Close()
	sel, err := db.Prepare(`SELECT email FROM users WHERE email LIKE $1 ORDER BY email`)
	require.NoError(t, err)
	defer sel.Close()

	for _, email := range []string{"stmt1@example.com", "stmt2@example.com"} {
		_, err := insert.Exec(email)
		require.NoError(t, err)
	}
	_, err = insert.Exec("stmt1@example.com")
	require.ErrorContains(t, err, "duplicate key")

	rows, err := sel.Query("stmt%")
	require.NoError(t, err)
	defer rows.Close()
	var emails []string
	for rows.Next() {
		var email string
		require.NoError(t, rows.Scan(&email))
		emails = append(emails, email)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"stmt1@example.com", "stmt2@example.com"}, emails)
}

func TestConcurrentStatements(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()

	const n = 8
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			_, err := db.Exec(`INSERT INTO users (email) VALUES ($1)`, fmt.Sprintf("stmt-concurrent%d@example.com", i))
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE email LIKE 'stmt-concurrent%'`).Scan(&count))
	assert.Equal(t, n, count)
}

func TestPing(t *testing.T) {
	t.Parallel()
	db := sandpit.Session(t).DB()
	assert.NoError(t, db.Ping())
}
