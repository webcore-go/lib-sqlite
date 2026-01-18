package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/uptrace/bun/dialect/sqlitedialect"
	libsql "github.com/webcore-go/lib-sql"
	"github.com/webcore-go/webcore/infra/config"
	"github.com/webcore-go/webcore/port"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteLoader struct {
	name string
}

func (a *SqliteLoader) SetName(name string) {
	a.name = name
}

func (a *SqliteLoader) Name() string {
	return a.name
}

func (l *SqliteLoader) Init(args ...any) (port.Library, error) {
	config := args[1].(config.DatabaseConfig)
	dsn := libsql.BuildDSN(config)

	db := &libsql.SQLDatabase{}

	driver := libsql.NewConnector("sqlite", &Connector{dsn: dsn})
	dialect := sqlitedialect.New()

	// Set up Bun SQL database wrapper
	db.SetBunDB(driver, dialect)

	err := db.Install(args...)
	if err != nil {
		return nil, err
	}

	db.Connect()

	// l.DB = db
	return db, nil
}

// Connector wraps the SQLite standard driver
type Connector struct {
	dsn string
}

var _ driver.Connector = (*Connector)(nil)

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	db, err := sql.Open("sqlite3", c.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping sqlite: %w", err)
	}

	return &sqliteConn{db: db}, nil
}

func (c *Connector) Driver() driver.Driver {
	return libsql.NewDriver()
}

// sqliteConn wraps the SQLite database connection
type sqliteConn struct {
	db *sql.DB
}

func (c *sqliteConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.db.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return &sqliteStmt{stmt: stmt}, nil
}

func (c *sqliteConn) Close() error {
	return c.db.Close()
}

func (c *sqliteConn) Begin() (driver.Tx, error) {
	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	return &sqliteTx{tx: tx}, nil
}

// sqliteStmt wraps the SQLite statement
type sqliteStmt struct {
	stmt *sql.Stmt
}

func (s *sqliteStmt) Close() error {
	return s.stmt.Close()
}

func (s *sqliteStmt) NumInput() int {
	return -1
}

func (s *sqliteStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.stmt.ExecContext(context.Background(), libsql.ToNamedValues(args)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *sqliteStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.stmt.QueryContext(context.Background(), libsql.ToNamedValues(args)...)
	if err != nil {
		return nil, err
	}
	return &sqliteRows{rows: rows}, nil
}

// sqliteTx wraps the SQLite transaction
type sqliteTx struct {
	tx *sql.Tx
}

func (t *sqliteTx) Commit() error {
	return t.tx.Commit()
}

func (t *sqliteTx) Rollback() error {
	return t.tx.Rollback()
}

// sqliteRows wraps the SQLite rows
type sqliteRows struct {
	rows *sql.Rows
}

func (r *sqliteRows) Columns() []string {
	cols, _ := r.rows.Columns()
	return cols
}

func (r *sqliteRows) Close() error {
	return r.rows.Close()
}

func (r *sqliteRows) Next(dest []driver.Value) error {
	// Convert []driver.Value to []any
	args := make([]any, len(dest))
	for i := range dest {
		args[i] = &dest[i]
	}
	return r.rows.Scan(args...)
}
