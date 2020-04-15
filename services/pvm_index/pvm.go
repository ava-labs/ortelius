// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

import (
	"github.com/ava-labs/gecko/database"
	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/platformvm"
)

// newPVM creates a platformvm instance we can use for parsing blocks and txs
func newPVM(chainID ids.ID, networkID uint32) (*platformvm.VM, error) {
	// g, err := genesis.VMGenesis(AVANetworkID, platformvm.ID)
	// if err != nil {
	// 	return nil, err
	// }

	gBytes, err := genesis.Genesis(AVANetworkID)
	if err != nil {
		return nil, err
	}

	var (
		// 	genesisTX = g
		// 	fxIDs     = genesisTX.FxIDs
		// 	fxs       = make([]*common.Fx, 0, len(fxIDs))
		ctx = &snow.Context{
			NetworkID: networkID,
			ChainID:   chainID,
			Log:       logging.NoLog{},
		}
	)
	// for _, fxID := range fxIDs {
	// 	switch {
	// 	case fxID.Equals(secp256k1fx.ID):
	// 		fxs = append(fxs, &common.Fx{
	// 			Fx: &secp256k1fx.Fx{},
	// 			ID: fxID,
	// 		})
	// 	case fxID.Equals(nftfx.ID):
	// 		fxs = append(fxs, &common.Fx{
	// 			Fx: &nftfx.Fx{},
	// 			ID: fxID,
	// 		})
	// 	default:
	// 		// return nil, fmt.Errorf("Unknown FxID: %s", fxID)
	// 	}
	// }

	vm := &platformvm.VM{}
	err = vm.Initialize(ctx, &nodb.Database{}, gBytes, make(chan common.Message, 1), nil)
	// err = vm.Initialize(ctx, &nodb.Database{}, genesisTX.GenesisData, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, err
	}

	return vm, nil
}
