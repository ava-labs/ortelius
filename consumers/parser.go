// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"errors"
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

var (
	// ErrNoParserForVM is returned when trying to create a parser for a VM that
	// doesn't exist or has no defined parsers
	ErrNoParserForVM = errors.New("no parser for VM")
)

// txParser parses a byte slice into a snowstorm.Tx
type txParser func(b []byte) (snowstorm.Tx, error)

// newParser returns a tx parser for the given vmType
func newParser(vmType string, chainID ids.ID, networkID uint32) (txParser, error) {
	switch vmType {
	case "avm":
		return newAVMParser(chainID, networkID)
	}
	return nil, ErrNoParserForVM
}

// newAVMParser creates a new ParseFn that parses AVM txs
func newAVMParser(chainID ids.ID, networkID uint32) (txParser, error) {
	avm, err := newAVM(chainID, networkID)
	if err != nil {
		return nil, err
	}

	return avm.ParseTx, nil
}

// newAVM creates an AVM instance that we can use to parse txs
func newAVM(chainID ids.ID, networkID uint32) (*avm.VM, error) {
	genesisTX := genesis.VMGenesis(networkID, avm.ID)

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
		NetworkID: networkID,
		ChainID:   chainID,
		Log:       logging.NoLog{},
	}

	// Initialize an AVM to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err := vm.Initialize(ctx, &nodb.Database{}, genesisTX.GenesisData, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, err
	}
	return vm, nil
}
