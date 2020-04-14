// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"errors"

	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/go-redis/redis"

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

func New(conf cfg.ServiceConfig, chainID ids.ID) (*Index, error) {
	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		return nil, err
	}
	return newForConnections(conns, chainID)
}

func newForConnections(conns *services.Connections, chainID ids.ID) (*Index, error) {
	vm, err := newAVM(chainID, 12345)
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
	txCount, err := i.db.GetTxCount()
	if err != nil {
		return nil, err
	}

	assetCount, err := i.db.GetAssetCount()
	if err != nil {
		return nil, err
	}

	return &chainInfo{
		ID:        i.chainID,
		Alias:     alias,
		NetworkID: networkID,
		VM:        VMName,

		TransactionCount: txCount,
		AssetCount:       assetCount,
	}, nil
}

func (i *Index) GetRecentTxs(count int64) ([]ids.ID, error) {
	return i.cache.GetRecentTxs(count)
}

func (i *Index) GetTxCount() (int64, error) {
	return i.db.GetTxCount()
}

func (i *Index) GetTxs() ([]timestampedTx, error) {
	return i.db.GetTxs()
}

func (i *Index) GetTxsForAddr(addr ids.ShortID) ([]timestampedTx, error) {
	return i.db.GetTxsForAddr(addr)
}

func (i *Index) GetTx(txID ids.ID) ([]byte, error) {
	tx, err := i.cache.GetTx(txID)
	if err == nil {
		return tx, nil
	}
	if err != redis.Nil {
		return nil, err
	}

	return i.db.GetTx(txID)
}

func (i *Index) GetTXOsForAddr(addr ids.ShortID, spent *bool) ([]output, error) {
	return i.db.GetTXOsForAddr(addr, spent)
}

func (i *Index) GetAssets() ([]asset, error) {
	return i.db.GetAssets()
}

func (i *Index) GetAsset(aliasOrID string) (asset, error) {
	return i.db.GetAsset(aliasOrID)
}

func (i *Index) GetTxsForAsset(assetID ids.ID) ([]json.RawMessage, error) {
	return i.db.GetTxsForAsset(assetID)
}

func (i *Index) Bootstrap() error {
	g, err := genesis.VMGenesis(12345, avm.ID)
	if err != nil {
		return err
	}

	avmGenesis := &avm.Genesis{}
	if err := i.vm.Codec().Unmarshal(g.GenesisData, avmGenesis); err != nil {
		return err
	}

	timestamp, err := platformGenesisTimestamp()
	if err != nil {
		return err
	}

	for _, tx := range avmGenesis.Txs {
		txBytes, err := i.vm.Codec().Marshal(tx)
		if err != nil {
			return err
		}
		utx := &avm.UniqueTx{TxState: &avm.TxState{Tx: &avm.Tx{UnsignedTx: tx, Creds: nil}}}
		if err := i.db.AddTx(utx, timestamp, txBytes); err != nil {
			return err
		}
	}

	return nil
}
