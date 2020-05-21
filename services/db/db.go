// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"time"

	"github.com/DATA-DOG/go-txdb"
	"github.com/gocraft/dbr"
	"github.com/gocraft/dbr/dialect"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"

	_ "github.com/go-sql-driver/mysql"
)

const (
	txDBDriverName = "txdb"
)

func New(stream *health.Stream, conf cfg.DB) (*dbr.Connection, error) {
	var (
		err error

		dsn                    = conf.DSN
		driver                 = conf.Driver
		dbrDialect dbr.Dialect = dialect.PostgreSQL
	)

	// If we want a transactional db then register that driver instead
	if conf.TXDB {
		driver = txDBDriverName
		registerTxDB(conf)
	}

	// If we're using MySQL we need to ensure to set the parseTime option
	if conf.Driver == "mysql" {
		dbrDialect = dialect.MySQL
		dsn, err = forceParseTimeParam(dsn)
		if err != nil {
			return nil, err
		}
	}

	// Create the underlying connection and ping it to ensure liveness
	rawDBConn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelFn()
	if err := rawDBConn.PingContext(ctx); err != nil {
		return nil, err
	}

	// Return a dbr connection from our raw db connection
	return &dbr.Connection{
		DB:            rawDBConn,
		EventReceiver: stream,
		Dialect:       dbrDialect,
	}, nil
}

var registerTxDBOnce = sync.Once{}

func registerTxDB(c cfg.DB) {
	registerTxDBOnce.Do(func() {
		txdb.Register(txDBDriverName, c.Driver, c.DSN)
	})
}

func forceParseTimeParam(dsn string) (string, error) {
	// Parse dsn into a url
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}

	// Add parseTime query param
	q := u.Query()
	q.Add("parseTime", "true")
	u.RawQuery = q.Encode()

	// Re-encode as a string
	return u.String(), nil
}
