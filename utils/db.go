// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
)

const (
	DriverMysql = "mysql"
	DriverNone  = ""
)

// Conn is a wrapper around a dbr connection and a health stream
type Conn struct {
	conn    *dbr.Connection
	eventer *EventRcvr
}

// New creates a new DB for the given config
func New(eventer *EventRcvr, conf cfg.DB, ro bool) (*Conn, error) {
	conn, err := newDBRConnection(conf, ro)
	if err != nil {
		return nil, err
	}
	return &Conn{
		conn:    conn,
		eventer: eventer,
	}, nil
}

func (c *Conn) Close(context.Context) error {
	return c.conn.Close()
}

func (c *Conn) NewSession(name string, timeout time.Duration) (*dbr.Session, error) {
	session := c.NewSessionForEventReceiver(c.eventer.NewJob(name))
	if _, err := session.Exec(fmt.Sprintf("SET SESSION MAX_EXECUTION_TIME=%d", timeout.Milliseconds())); err != nil {
		return nil, err
	}
	return session, nil
}

func (c *Conn) NewSessionForEventReceiver(er dbr.EventReceiver) *dbr.Session {
	return c.conn.NewSession(er)
}

func (c *Conn) SetMaxOpenConns(n int) {
	c.conn.SetMaxOpenConns(n)
}
func (c *Conn) SetMaxIdleConns(n int) {
	c.conn.SetMaxIdleConns(n)
}
func (c *Conn) SetConnMaxIdleTime(d time.Duration) {
	c.conn.SetConnMaxIdleTime(d)
}
func (c *Conn) SetConnMaxLifetime(d time.Duration) {
	c.conn.SetConnMaxLifetime(d)
}

func newDBRConnection(conf cfg.DB, ro bool) (*dbr.Connection, error) {
	var (
		err error

		driver                 = conf.Driver
		dbrDialect dbr.Dialect = dialect.MySQL
	)

	dsn := conf.DSN
	if ro {
		dsn = conf.RODSN
	}

	// If we're using MySQL we need to ensure to set the parseTime option
	if conf.Driver == DriverMysql {
		dbrDialect = dialect.MySQL
		dsn, err = ForceParseTimeParam(dsn)
		if err != nil {
			return nil, err
		}
	}

	// Create the underlying connection and ping it to ensure liveness
	rawDBConn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFn()
	if err := rawDBConn.PingContext(ctx); err != nil {
		rawDBConn.Close()
		return nil, err
	}

	// Return a dbr connection from our raw db connection
	return &dbr.Connection{
		DB:            rawDBConn,
		EventReceiver: &dbr.NullEventReceiver{},
		Dialect:       dbrDialect,
	}, nil
}
