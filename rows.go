package pgsandpit

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
)

// rows holds every row of every result set of a query, read before the query
// returned.
type rows struct {
	sets []*resultSet
	pos  int
}

type resultSet struct {
	cols  []string
	types []*sql.ColumnType
	rows  [][]driver.Value
	next  int
}

var (
	_ driver.RowsNextResultSet              = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*rows)(nil)
)

func readRows(rs *sql.Rows) (*rows, error) {
	defer rs.Close()
	r := &rows{}
	for {
		set, err := readResultSet(rs)
		if err != nil {
			return nil, err
		}
		r.sets = append(r.sets, set)
		if !rs.NextResultSet() {
			break
		}
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return r, nil
}

func readResultSet(rs *sql.Rows) (*resultSet, error) {
	cols, err := rs.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rs.ColumnTypes()
	if err != nil {
		return nil, err
	}
	set := &resultSet{cols: cols, types: types}
	for rs.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]driver.Value, len(cols))
		for i, v := range vals {
			row[i] = v
		}
		set.rows = append(set.rows, row)
	}
	return set, rs.Err()
}

func (r *rows) cur() *resultSet { return r.sets[r.pos] }

func (r *rows) Columns() []string { return r.cur().cols }

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	set := r.cur()
	if set.next >= len(set.rows) {
		return io.EOF
	}
	copy(dest, set.rows[set.next])
	set.next++
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.pos+1 < len(r.sets)
}

func (r *rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.pos++
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	return r.cur().types[i].DatabaseTypeName()
}

func (r *rows) ColumnTypeScanType(i int) reflect.Type {
	return r.cur().types[i].ScanType()
}

func (r *rows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return r.cur().types[i].Nullable()
}
