package pgsandpit

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
)

// rows holds every row of a query, read before the query returned. pgx returns
// one result set per query, so rows holds one too.
type rows struct {
	cols  []string
	types []*sql.ColumnType
	rows  [][]driver.Value
	next  int
}

// The column type interfaces are those pgx's driver implements.
var (
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeLength           = (*rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
)

func readRows(rs *sql.Rows) (*rows, error) {
	defer rs.Close()
	cols, err := rs.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rs.ColumnTypes()
	if err != nil {
		return nil, err
	}
	r := &rows{cols: cols, types: types}
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
		r.rows = append(r.rows, row)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rows) Columns() []string { return r.cols }

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	return r.types[i].DatabaseTypeName()
}

func (r *rows) ColumnTypeLength(i int) (int64, bool) {
	return r.types[i].Length()
}

func (r *rows) ColumnTypePrecisionScale(i int) (precision, scale int64, ok bool) {
	return r.types[i].DecimalSize()
}

func (r *rows) ColumnTypeScanType(i int) reflect.Type {
	return r.types[i].ScanType()
}
