// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/vms/avm"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

const (
	AVANetworkID = 12345
)

type Index struct {
	chainID ids.ID
	vm      *avm.VM
	db      *DBIndex
	cache   *RedisIndex
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
		vm:      vm,
		chainID: chainID,
		db:      NewDBIndex(conns.Stream(), conns.DB(), chainID, vm.Codec()),
		cache:   NewRedisIndex(conns.Redis(), chainID),
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
	if err := i.cache.AddTx(ingestable.ID(), ingestable.Body()); err != nil {
		return err
	}
	return nil

}

func (i *Index) GetChainInfo(alias string, networkID uint32) (*chainInfo, error) {
	dayAggregates, err := i.db.GetTransactionAggregates(GetTransactionAggregatesParams{
		StartTime:    time.Now().Add(-24 * time.Hour),
		IntervalSize: IntervalAll,
	})
	if err != nil {
		return nil, err
	}

	allTimeAggregates, err := i.db.GetTransactionAggregates(GetTransactionAggregatesParams{
		IntervalSize: IntervalAll,
	})
	if err != nil {
		return nil, err
	}

	return &chainInfo{
		ID:        i.chainID,
		Alias:     alias,
		NetworkID: networkID,
		VM:        VMName,

		Aggregates: chainInfoAggregates{
			Day: dayAggregates.Intervals[0].Aggregates,
			All: allTimeAggregates.Intervals[0].Aggregates,
		},
	}, nil
}

func (i *Index) GetRecentTxs(count int64) ([]ids.ID, error) {
	return i.cache.GetRecentTxs(count)
}

func (i *Index) GetTxCount() (int64, error) {
	return i.db.GetTxCount()
}

func (i *Index) GetTx(txID ids.ID) (*displayTx, error) {
	return i.db.GetTx(txID)
}

func (i *Index) GetTxs(params *ListTxParams) ([]*displayTx, error) {
	return i.db.GetTxs(params)
}

func (i *Index) GetTxsForAddr(addr ids.ShortID, params *ListTxParams) ([]*displayTx, error) {
	return i.db.GetTxsForAddr(addr, params)
}

func (i *Index) GetTxsForAsset(assetID ids.ID, params *ListTxParams) ([]json.RawMessage, error) {
	return i.db.GetTxsForAsset(assetID, params)
}

func (i *Index) GetTXOsForAddr(addr ids.ShortID, params *ListTXOParams) ([]output, error) {
	return i.db.GetTXOsForAddr(addr, params)
}

func (i *Index) GetAssets(params *ListParams) ([]asset, error) {
	return i.db.GetAssets(params)
}

func (i *Index) GetAsset(aliasOrID string) (asset, error) {
	return i.db.GetAsset(aliasOrID)
}

func (i *Index) Search(params SearchParams) (*searchResults, error) {
	return i.db.Search(params)
}

func (i *Index) GetTransactionAggregatesHistogram(params GetTransactionAggregatesParams) (*TransactionAggregatesHistogram, error) {
	return i.db.GetTransactionAggregates(params)
}
