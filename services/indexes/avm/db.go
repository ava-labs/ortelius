// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"github.com/ava-labs/gecko/utils/crypto"
	"github.com/ava-labs/gecko/vms/components/codec"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"
)

// DB is a services.Accumulator backed by redis
type DB struct {
	chainID string
	codec   codec.Codec
	stream  *health.Stream
	db      *dbr.Connection

	ecdsaRecoveryFactory crypto.FactorySECP256K1R
}

// NewDB creates a new DB for the given config
func NewDB(stream *health.Stream, db *dbr.Connection, chainID string, codec codec.Codec) *DB {
	return &DB{
		db:      db,
		codec:   codec,
		stream:  stream,
		chainID: chainID,

		ecdsaRecoveryFactory: crypto.FactorySECP256K1R{},
	}
}

func (r *DB) newSession(name string) *dbr.Session {
	return r.db.NewSession(r.stream.NewJob(name))
}
