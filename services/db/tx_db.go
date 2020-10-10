// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"sync"

	"github.com/DATA-DOG/go-txdb"

	"github.com/ava-labs/ortelius/cfg"
)

var registerTxDBOnce = sync.Once{}

func registerTxDB(c cfg.DB) {
	registerTxDBOnce.Do(func() {
		txdb.Register(driverTXDB, c.Driver, c.DSN)
	})
}
