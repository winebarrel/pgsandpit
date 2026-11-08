package pgsandpit_test

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tagKey struct{}

// hookConnector records each query with the tag its context carries.
type hookConnector struct {
	driver.Connector
	mu      sync.Mutex
	queries []string
}

func (c *hookConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &hookConn{Conn: conn, c: c}, nil
}

func (c *hookConnector) record(ctx context.Context, query string) {
	tag, _ := ctx.Value(tagKey{}).(string)
	c.mu.Lock()
	c.queries = append(c.queries, tag+": "+query)
	c.mu.Unlock()
}

type hookConn struct {
	driver.Conn
	c *hookConnector
}

func (c *hookConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.c.record(ctx, query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *hookConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.c.record(ctx, query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestConnector(t *testing.T) {
	t.Parallel()

	t.Run("wrap the connector to hook into queries", func(t *testing.T) {
		s := sandpit.Session(t)
		hook := &hookConnector{}
		db := s.With().Connector(func(c driver.Connector) driver.Connector {
			hook.Connector = c
			return hook
		}).DB()

		ctx := context.WithValue(context.Background(), tagKey{}, "tagged")
		_, err := db.ExecContext(ctx, `INSERT INTO users (email) VALUES ('hook@example.com')`)
		require.NoError(t, err)

		// The *sql.DB still runs on the session.
		assert.Equal(t, 1, countUsers(t, s.DB(), "hook@example.com"))
		assert.Equal(t, []string{
			"tagged: INSERT INTO users (email) VALUES ('hook@example.com')",
		}, hook.queries)
	})

	t.Run("use another connector", func(t *testing.T) {
		s := sandpit.Session(t)
		_, err := s.DB().Exec(`INSERT INTO users (email) VALUES ('another@example.com')`)
		require.NoError(t, err)

		cfg, err := pgx.ParseConfig(dsn())
		require.NoError(t, err)
		db := s.With().Connector(func(driver.Connector) driver.Connector {
			return stdlib.GetConnector(*cfg)
		}).DB()

		// The returned connector is used as is: this *sql.DB is not on the session.
		assert.Equal(t, 0, countUsers(t, db, "another@example.com"))
	})

	t.Run("does not change the builder", func(t *testing.T) {
		s := sandpit.Session(t)
		b := s.With()
		called := false
		_ = b.Connector(func(c driver.Connector) driver.Connector {
			called = true
			return c
		})

		require.NoError(t, b.DB().Ping())
		assert.False(t, called)
	})
}
