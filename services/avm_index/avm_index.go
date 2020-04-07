// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"

	"github.com/ava-labs/gecko/ids"
	"github.com/go-redis/redis"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

type Index struct {
	txParser txParser
	db       *DB
	cache    *RedisIndex
}

func New(conf cfg.ServiceConfig) (*Index, error) {
	connections, err := services.NewConnections(conf)
	if err != nil {
		return nil, err
	}

	txParser, err := newTXParser()
	if err != nil {
		return nil, err
	}

	return &Index{
		txParser: txParser,
		db:       NewDB(connections.Stream(), connections.DB()),
		cache:    NewRedisIndex(connections.Redis()),
	}, nil
}

func (i *Index) AddTx(ingestable services.Ingestable) error {
	// Parse into a tx
	snowstormTx, err := i.txParser(ingestable.Body())
	if err != nil {
		return err
	}

	// Get indexable bytes and send to the backends
	body, err := toIndexableBytes(snowstormTx)
	if err != nil {
		return err
	}

	// func (i *Index) AddTx(chainID ids.ID, txID ids.ID, body []byte) error {
	if err := i.db.AddTx(ingestable.ChainID(), ingestable.ID(), body); err != nil {
		return err
	}
	if err := i.cache.AddTx(ingestable.ChainID(), ingestable.ID(), body); err != nil {
		return err
	}
	return nil
}

func (i *Index) GetTx(chainID ids.ID, txID ids.ID) ([]byte, error) {
	tx, err := i.cache.GetTx(chainID, txID)
	if err == nil {
		return tx, nil
	}
	if err != redis.Nil {
		return nil, err
	}
	return i.db.GetTx(chainID, txID)
}

func (i *Index) GetRecentTxs(chaindID ids.ID, count int64) ([]ids.ID, error) {
	return i.cache.GetRecentTxs(chaindID, count)
}

func (i *Index) GetTxCount(chaindID ids.ID) (int64, error) {
	return i.cache.GetTxCount(chaindID)
}

func (i *Index) GetTxsForAddr(chainID ids.ID, addr ids.ShortID) ([]json.RawMessage, error) {
	return i.db.GetTxsForAddr(chainID, addr)
}

func (i *Index) GetTXOsForAddr(chainID ids.ID, addr ids.ShortID, spent *bool) ([]output, error) {
	return i.db.GetTXOsForAddr(chainID, addr, spent)
}
