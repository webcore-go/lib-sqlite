package mysql

import (
	"github.com/semanggilab/webcore-go/app/config"
	"github.com/semanggilab/webcore-go/app/loader"
	"github.com/semanggilab/webcore-go/lib/sql"
	"gorm.io/driver/sqlite"
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

func (l *SqliteLoader) Init(args ...any) (loader.Library, error) {
	config := args[1].(config.DatabaseConfig)
	dsn := sql.BuildDSN(config)

	db := &sql.SQLDatabase{}
	db.SetDialect(sqlite.Open(dsn))
	err := db.Install(args...)
	if err != nil {
		return nil, err
	}

	db.Connect()

	// l.DB = db
	return db, nil
}
