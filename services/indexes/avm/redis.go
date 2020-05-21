// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"strings"

	"github.com/ava-labs/gecko/ids"
	"github.com/go-redis/redis"

	"github.com/ava-labs/ortelius/services"
)

// RecentTxsSize is the number of transactions we want to keep cached in the
// recent txs list in redis
const (
	redisRecentTxsSize = 100_000

	redisKeyPrefixesTxByID    = "txs_by_id"
	redisKeyPrefixesTxCount   = "tx_count"
	redisKeyPrefixesRecentTxs = "recent_txs"
)

// Redis is an Accumulator and Consume backed by redis
type Redis struct {
	chainID ids.ID
	client  *redis.Client
}

// NewRedisIndex creates a new Redis for the given config
func NewRedisIndex(client *redis.Client, chainID ids.ID) *Redis {
	return &Redis{chainID: chainID, client: client}
}

// AddTx ingests a Transaction and adds it to the services
func (r *Redis) Index(i services.Consumable) error {
	var (
		pipe = r.client.TxPipeline()

		txByIDKey    = redisIndexKeysTxByID(r.chainID.String(), i.ID())
		txCountKey   = redisIndexKeysTxCount(r.chainID.String())
		recentTxsKey = redisIndexKeysRecentTxs(r.chainID.String())
	)

	if err := pipe.Set(txByIDKey, i.Body(), 0).Err(); err != nil {
		return err
	}

	if err := pipe.Incr(txCountKey).Err(); err != nil {
		return err
	}

	if err := pipe.LPush(recentTxsKey, i.ID()).Err(); err != nil {
		return err
	}

	if err := pipe.LTrim(recentTxsKey, 0, redisRecentTxsSize-1).Err(); err != nil {
		return err
	}

	_, err := pipe.Exec()
	return err
}

// GetTransaction returns the bytes for the Transaction with the given ID
func (r *Redis) GetTx(txID ids.ID) ([]byte, error) {
	cmd := r.client.Get(redisIndexKeysTxByID(r.chainID.String(), txID.String()))
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	return cmd.Bytes()
}

// GetTransactionCount returns the number of transactions this Server as seen
func (r *Redis) GetTxCount() (uint64, error) {
	cmd := r.client.Get(redisIndexKeysTxCount(r.chainID.String()))
	if err := cmd.Err(); err != nil {
		return 0, err
	}
	return cmd.Uint64()
}

// GetRecentTransactions returns a list of the N most recent transactions
func (r *Redis) GetRecentTransactions(n int64) ([]ids.ID, error) {
	cmd := r.client.LRange(redisIndexKeysRecentTxs(r.chainID.String()), 0, n-1)
	if err := cmd.Err(); err != nil {
		return nil, err
	}

	idStrs := cmd.Val()
	idObjs := make([]ids.ID, len(idStrs))

	for i, idStr := range idStrs {
		id, err := ids.FromString(idStr)
		if err != nil {
			return nil, err
		}
		idObjs[i] = id
	}
	return idObjs, nil
}

func redisIndexKeysTxByID(chainID string, txID string) string {
	return redisIndexKey(chainID, redisKeyPrefixesTxByID, txID)
}

func redisIndexKeysTxCount(chainID string) string {
	return redisIndexKey(chainID, redisKeyPrefixesTxCount)
}

func redisIndexKeysRecentTxs(chainID string) string {
	return redisIndexKey(chainID, redisKeyPrefixesRecentTxs)
}

func redisIndexKey(chainID string, keyName string, extra ...string) string {
	return strings.Join(append([]string{
		chainID,
		keyName,
	}, extra...), "|")
}
