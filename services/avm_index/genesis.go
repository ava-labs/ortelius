// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/vms/platformvm"
)

func platformGenesis() (*platformvm.Genesis, error) {
	genesisBytes, err := genesis.Genesis(AVANetworkID)
	if err != nil {
		return nil, err
	}
	genesis := &platformvm.Genesis{}
	err = platformvm.Codec.Unmarshal(genesisBytes, genesis)
	if err != nil {
		return nil, err
	}
	return genesis, genesis.Initialize()
}

func platformGenesisTimestamp() (uint64, error) {
	g, err := platformGenesis()
	if err != nil {
		return 0, err
	}
	return g.Timestamp, nil
}
