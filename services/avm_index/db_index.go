// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"errors"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/vms/components/codec"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"
)

const (
	// MaxSerializationLen is the maximum number of bytes a canonically
	// serialized tx can be stored as in the database.
	MaxSerializationLen = 16_384
)

var (
	// ErrSerializationTooLong is returned when trying to ingest data with a
	// serialization larger than our max
	ErrSerializationTooLong = errors.New("serialization is too long")
)

// DBIndex is a services.Accumulator backed by redis
type DBIndex struct {
	chainID ids.ID
	codec   codec.Codec
	stream  *health.Stream
	db      *dbr.Connection
}

// NewDBIndex creates a new DBIndex for the given config
func NewDBIndex(stream *health.Stream, db *dbr.Connection, chainID ids.ID, codec codec.Codec) *DBIndex {
	return &DBIndex{
		chainID: chainID,
		codec:   codec,
		stream:  stream,
		db:      db,
	}
}

func (r *DBIndex) newDBSession(name string) *dbr.Session {
	return r.db.NewSession(r.stream.NewJob(name))
}
