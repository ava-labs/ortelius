// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-txdb"
	"github.com/gocraft/dbr"
	"github.com/gocraft/dbr/dialect"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
)

func init() {
	txdb.Register(testTXDBDriverName, testConfig.DB.Driver, testConfig.DB.DSN)
}

var (
	testConfig = &cfg.ServiceConfig{
		DB: &cfg.DBConfig{
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test",
		},
	}
	testTXDBDriverName = txdbName(*testConfig.DB)
)

func txdbName(conf cfg.DBConfig) string {
	return strings.Join([]string{"txdb", conf.Driver, conf.DSN}, "-")
}

func NewTestStream() *health.Stream {
	return health.NewStream()
}

func NewTestDB(t *testing.T, stream *health.Stream) *dbr.Connection {
	rawDBConn, err := sql.Open(testTXDBDriverName, "txdb")
	if err != nil {
		t.Fatal("Failed to create raw DB connection:", err.Error())
	}

	c := &dbr.Connection{
		DB:            rawDBConn,
		EventReceiver: stream,
		Dialect:       dialect.MySQL,
	}

	if err := c.Ping(); err != nil {
		t.Fatal("Failed to ping DB:", err.Error())
	}

	return c
}
