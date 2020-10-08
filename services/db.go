// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"context"
	"strings"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"

	// Import MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

// DB is a services.Accumulator backed by redis
type DB struct {
	stream *health.Stream
	db     *dbr.Connection
}

// NewDB creates a new DB for the given config
func NewDB(stream *health.Stream, db *dbr.Connection) *DB {
	return &DB{
		db:     db,
		stream: stream,
	}
}

func (w *DB) Close(context.Context) error {
	w.stream.Event("close")
	return w.db.Close()
}

func (w *DB) NewSession(name string) *dbr.Session {
	return w.NewSessionForEventReceiver(w.stream.NewJob(name))
}

func (w *DB) NewSessionForEventReceiver(er health.EventReceiver) *dbr.Session {
	return w.db.NewSession(er)
}

func ErrIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}
