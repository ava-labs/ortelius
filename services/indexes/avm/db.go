// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"

	"github.com/ava-labs/gecko/utils/crypto"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"

	// Import MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

// DB is a services.Accumulator backed by redis
type DB struct {
	chainID string
	vm      *avm.VM
	stream  *health.Stream
	db      *dbr.Connection

	ecdsaRecoveryFactory crypto.FactorySECP256K1R
}

// NewDB creates a new DB for the given config
func NewDB(stream *health.Stream, db *dbr.Connection, chainID string, vm *avm.VM) *DB {
	return &DB{
		db:      db,
		vm:      vm,
		stream:  stream,
		chainID: chainID,

		ecdsaRecoveryFactory: crypto.FactorySECP256K1R{},
	}
}

func (db *DB) Close(context.Context) error {
	db.stream.Event("close")
	return db.db.Close()
}

func (db *DB) newSession(name string) *dbr.Session {
	return db.db.NewSession(db.stream.NewJob(name))
}
