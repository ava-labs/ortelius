// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"

	"github.com/ava-labs/gecko/database"
	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/nftfx"
	"github.com/ava-labs/gecko/vms/platformvm"
	"github.com/ava-labs/gecko/vms/secp256k1fx"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/models"
)

func init() {
	api.RegisterRouter(VMName, NewAPIRouter, APIContext{})
}

type Index struct {
	networkID uint32
	chainID   string
	db        *DB
}

func New(conf cfg.Services, networkID uint32, chainID string) (*Index, error) {
	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		return nil, err
	}
	return newForConnections(conns, networkID, chainID)
}

func newForConnections(conns *services.Connections, networkID uint32, chainID string) (*Index, error) {
	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, err
	}

	vm, err := newAVM(id, networkID)
	if err != nil {
		return nil, err
	}

	return &Index{
		networkID: networkID,
		chainID:   chainID,
		db:        NewDB(conns.Stream(), conns.DB(), chainID, vm.Codec()),
	}, nil
}

func (i *Index) Name() string { return "avm-index" }

func (i *Index) Bootstrap(ctx context.Context) error {
	platformGenesisBytes, err := genesis.Genesis(i.networkID)
	if err != nil {
		return err
	}

	platformGenesis := &platformvm.Genesis{}
	if err = platformvm.Codec.Unmarshal(platformGenesisBytes, platformGenesis); err != nil {
		return err
	}
	if err = platformGenesis.Initialize(); err != nil {
		return err
	}

	for _, chain := range platformGenesis.Chains {
		if chain.VMID.Equals(avm.ID) {
			return i.bootstrap(ctx, chain.GenesisData, int64(platformGenesis.Timestamp))
		}
	}
	return nil
}

func (i *Index) Consume(ctx context.Context, ingestable services.Consumable) error {
	if err := i.db.Index(ctx, ingestable); err != nil {
		return err
	}

	return nil
}

func (i *Index) GetChainInfo(alias string, networkID uint32) (*models.ChainInfo, error) {
	return &models.ChainInfo{
		ID:        models.StringID(i.chainID),
		Alias:     alias,
		NetworkID: networkID,
		VM:        VMName,
	}, nil
}

func (i *Index) Search(ctx context.Context, params *SearchParams) (*SearchResults, error) {
	return i.db.Search(ctx, params)
}

func (i *Index) Aggregate(ctx context.Context, params *AggregateParams) (*AggregatesHistogram, error) {
	return i.db.Aggregate(ctx, params)
}

func (i *Index) ListTransactions(ctx context.Context, params *ListTransactionsParams) (*TransactionList, error) {
	return i.db.ListTransactions(ctx, params)
}

func (i *Index) GetTransaction(ctx context.Context, id ids.ID) (*Transaction, error) {
	txList, err := i.db.ListTransactions(ctx, &ListTransactionsParams{ID: &id})
	if err != nil {
		return nil, err
	}
	if len(txList.Transactions) > 0 {
		return txList.Transactions[0], nil
	}
	return nil, nil
}

func (i *Index) ListAssets(ctx context.Context, params *ListAssetsParams) (*AssetList, error) {
	return i.db.ListAssets(ctx, params)
}

func (i *Index) GetAsset(ctx context.Context, idStrOrAlias string) (*Asset, error) {
	params := &ListAssetsParams{}

	id, err := ids.FromString(idStrOrAlias)
	if err == nil {
		params.ID = &id
	} else {
		params.Alias = idStrOrAlias
	}

	assetList, err := i.db.ListAssets(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(assetList.Assets) > 0 {
		return assetList.Assets[0], nil
	}
	return nil, err
}

func (i *Index) ListAddresses(ctx context.Context, params *ListAddressesParams) (*AddressList, error) {
	return i.db.ListAddresses(ctx, params)
}

func (i *Index) GetAddress(ctx context.Context, id ids.ShortID) (*Address, error) {
	addressList, err := i.db.ListAddresses(ctx, &ListAddressesParams{Address: &id})
	if err != nil {
		return nil, err
	}
	if len(addressList.Addresses) > 0 {
		return addressList.Addresses[0], nil
	}
	return nil, err
}

func (i *Index) ListOutputs(ctx context.Context, params *ListOutputsParams) (*OutputList, error) {
	return i.db.ListOutputs(ctx, params)
}

func (i *Index) GetOutput(ctx context.Context, id ids.ID) (*Output, error) {
	outputList, err := i.db.ListOutputs(ctx, &ListOutputsParams{ID: &id})
	if err != nil {
		return nil, err
	}
	if len(outputList.Outputs) > 0 {
		return outputList.Outputs[0], nil
	}
	return nil, err
}

func (i *Index) bootstrap(ctx context.Context, genesisBytes []byte, timestamp int64) error {
	return i.db.bootstrap(ctx, genesisBytes, timestamp)
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
