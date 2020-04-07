// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"fmt"

	"github.com/ava-labs/gecko/database"
	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/secp256k1fx"
)

type txParser func(b []byte) (snowstorm.Tx, error)

// newTXParser returns a function that parse []byte's into snowstorm.Tx
func newTXParser() (txParser, error) {
	avm, err := newAVM()
	if err != nil {
		return nil, err
	}
	return avm.ParseTx, nil
}

// newAVM creates an producer instance that we can use to parse txs
func newAVM() (*avm.VM, error) {
	genesisTX := genesis.VMGenesis(12345, avm.ID)

	fxIDs := genesisTX.FxIDs
	fxs := make([]*common.Fx, 0, len(fxIDs))
	for _, fxID := range fxIDs {
		switch {
		case fxID.Equals(secp256k1fx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		default:
			return nil, fmt.Errorf("Unknown FxID: %s", secp256k1fx.ID)
		}
	}

	ctx := &snow.Context{
		NetworkID: genesisTX.NetworkID,
		ChainID:   ids.Empty,
		Log:       logging.NoLog{},
	}

	// Initialize an producer to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err := vm.Initialize(ctx, &nodb.Database{}, genesisTX.GenesisData, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, err
	}
	return vm, nil
}
