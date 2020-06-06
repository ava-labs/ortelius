// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"errors"
	"github.com/ava-labs/gecko/database"
	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/nftfx"
	"github.com/ava-labs/gecko/vms/secp256k1fx"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

func init() {
	api.RegisterRouter(VMName, NewAPIRouter, APIContext{})
}

type Index struct {
	networkID uint32
	chainID   ids.ID
	vm        *avm.VM
	db        *DB
	cache     *Redis
}

func New(conf cfg.ServiceConfig, networkID uint32, chainID ids.ID) (*Index, error) {

	conns, err := services.NewConnectionsFromConfig(conf)

	if err != nil {
		return nil, err
	}
	return newForConnections(conns, networkID, chainID)
}

func newForConnections(conns *services.Connections, networkID uint32, chainID ids.ID) (*Index, error) {
	vm, err := newAVM(chainID, networkID)
	if err != nil {
		return nil, err
	}

	return &Index{
		vm:        vm,
		networkID: networkID,
		chainID:   chainID,
		db:        NewDB(conns.Stream(), conns.DB(), chainID, vm.Codec()),
	}, nil
}

func (i *Index) Add(ingestable services.Ingestable) error {
	// Parse into a tx
	snowstormTx, err := i.vm.ParseTx(ingestable.Body())
	if err != nil {
		return err
	}

	tx, ok := snowstormTx.(*avm.UniqueTx)
	if !ok {
		return errors.New("tx must be an UniqueTx")
	}

	if err := i.db.AddTx(tx, ingestable.Timestamp(), ingestable.Body()); err != nil {
		return err
	}

	if i.cache != nil {
		if err := i.cache.AddTx(ingestable.ID(), ingestable.Body()); err != nil {
			return err
		}
	}

	return nil

}

func (i *Index) GetChainInfo(alias string, networkID uint32) (*ChainInfo, error) {
	return &ChainInfo{
		ID:        i.chainID,
		Alias:     alias,
		NetworkID: networkID,
		VM:        VMName,
	}, nil
}

func (i *Index) Search(params SearchParams) (*SearchResults, error) {
	return i.db.Search(params)
}

func (i *Index) Aggregate(params AggregateParams) (*AggregatesHistogram, error) {
	return i.db.Aggregate(params)
}

func (i *Index) ListTransactions(params *ListTransactionsParams) (*TransactionList, error) {
	return i.db.ListTransactions(params)
}

func (i *Index) GetTransaction(id ids.ID) (*Transaction, error) {
	txList, err := i.db.ListTransactions(&ListTransactionsParams{ID: &id})
	if err != nil {
		return nil, err
	}
	if len(txList.Transactions) > 0 {
		return txList.Transactions[0], nil
	}
	return nil, nil
}

func (i *Index) ListAssets(params *ListAssetsParams) (*AssetList, error) {
	return i.db.ListAssets(params)
}

func (i *Index) GetAsset(idStrOrAlias string) (*Asset, error) {
	params := &ListAssetsParams{}

	id, err := ids.FromString(idStrOrAlias)
	if err == nil {
		params.ID = &id
	} else {
		params.Alias = idStrOrAlias
	}

	assetList, err := i.db.ListAssets(params)
	if err != nil {
		return nil, err
	}
	if len(assetList.Assets) > 0 {
		return assetList.Assets[0], nil
	}
	return nil, err
}

func (i *Index) ListAddresses(params *ListAddressesParams) (*AddressList, error) {
	return i.db.ListAddresses(params)
}

func (i *Index) GetAddress(id ids.ShortID) (*Address, error) {
	addressList, err := i.db.ListAddresses(&ListAddressesParams{Address: &id})
	if err != nil {
		return nil, err
	}
	if len(addressList.Addresses) > 0 {
		return addressList.Addresses[0], nil
	}
	return nil, err
}

func (i *Index) ListOutputs(params *ListOutputsParams) (*OutputList, error) {
	return i.db.ListOutputs(params)
}

func (i *Index) GetOutput(id ids.ID) (*Output, error) {
	outputList, err := i.db.ListOutputs(&ListOutputsParams{ID: &id})
	if err != nil {
		return nil, err
	}
	if len(outputList.Outputs) > 0 {
		return outputList.Outputs[0], nil
	}
	return nil, err
}

// newAVM creates an producer instance that we can use to parse txs
func newAVM(chainID ids.ID, networkID uint32) (*avm.VM, error) {
	g, err := genesis.VMGenesis(networkID, avm.ID)
	if err != nil {
		return nil, err
	}

	var (
		genesisTX = g
		fxIDs     = genesisTX.FxIDs
		fxs       = make([]*common.Fx, 0, len(fxIDs))
		ctx       = &snow.Context{
			NetworkID: networkID,
			ChainID:   chainID,
			Log:       logging.NoLog{},
		}
	)
	for _, fxID := range fxIDs {
		switch {
		case fxID.Equals(secp256k1fx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		case fxID.Equals(nftfx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &nftfx.Fx{},
				ID: fxID,
			})
		default:
			// return nil, fmt.Errorf("Unknown FxID: %s", fxID)
		}
	}

	// Initialize an producer to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err = vm.Initialize(ctx, &nodb.Database{}, genesisTX.GenesisData, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, err
	}

	return vm, nil
}
