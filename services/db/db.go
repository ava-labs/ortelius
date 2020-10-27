// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
)

const (
	DriverMysql = "mysql"
	DriverNone  = ""
	driverTXDB  = "txdb"
)

// Conn is a wrapper around a dbr connection and a health stream
type Conn struct {
	stream *health.Stream
	conn   *dbr.Connection
}

// New creates a new DB for the given config
func New(stream *health.Stream, conf cfg.DB) (*Conn, error) {
	conn, err := newDBRConnection(stream, conf)
	if err != nil {
		return nil, err
	}
	return &Conn{
		conn:   conn,
		stream: stream,
	}, nil
}

func (c *Conn) Close(context.Context) error {
	c.stream.Event("close")
	return c.conn.Close()
}

func (c *Conn) NewSession(name string, timeout time.Duration) (*dbr.Session, error) {
	session := c.conn.NewSession(c.stream.NewJob(name))
	if _, err := session.Exec(fmt.Sprintf("SET SESSION MAX_EXECUTION_TIME=%d", timeout.Milliseconds())); err != nil {
		return nil, err
	}
	return session, nil
}

func (c *Conn) NewSessionForEventReceiver(er health.EventReceiver) *dbr.Session {
	return c.conn.NewSession(er)
}

func newDBRConnection(stream *health.Stream, conf cfg.DB) (*dbr.Connection, error) {
	var (
		err error

		dsn                    = conf.DSN
		driver                 = conf.Driver
		dbrDialect dbr.Dialect = dialect.PostgreSQL
	)

	// If we want a transactional db then register that driver instead
	if conf.TXDB {
		driver = driverTXDB
		registerTxDB(conf)
	}

	// If we're using MySQL we need to ensure to set the parseTime option
	if conf.Driver == DriverMysql {
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
