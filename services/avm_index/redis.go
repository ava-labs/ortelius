// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"strings"

	"github.com/ava-labs/gecko/ids"
	"github.com/go-redis/redis"
)

// RecentTxsSize is the number of transactions we want to keep cached in the
// recent txs list in redis
const (
	redisRecentTxsSize = 100_000

	redisKeyPrefixesTxByID    = "txs_by_id"
	redisKeyPrefixesTxCount   = "tx_count"
	redisKeyPrefixesRecentTxs = "recent_txs"
)

// Redis is an Accumulator and Index backed by redis
type Redis struct {
	chainID ids.ID
	client  *redis.Client
}

// NewRedisIndex creates a new Redis for the given config
func NewRedisIndex(client *redis.Client, chainID ids.ID) *Redis {
	return &Redis{chainID: chainID, client: client}
}

// AddTx ingests a Transaction and adds it to the services
func (r *Redis) AddTx(txID ids.ID, body []byte) error {
	var (
		pipe = r.client.TxPipeline()

		txByIDKey    = redisIndexKeysTxByID(r.chainID, txID)
		txCountKey   = redisIndexKeysTxCount(r.chainID)
		recentTxsKey = redisIndexKeysRecentTxs(r.chainID)
	)

	if err := pipe.Set(r.client.Context(), txByIDKey, body, 0).Err(); err != nil {
		return err
	}

	if err := pipe.Incr(r.client.Context(), txCountKey).Err(); err != nil {
		return err
	}

	if err := pipe.LPush(r.client.Context(), recentTxsKey, txID.String()).Err(); err != nil {
		return err
	}

	if err := pipe.LTrim(r.client.Context(), recentTxsKey, 0, redisRecentTxsSize-1).Err(); err != nil {
		return err
	}

	_, err := pipe.Exec(r.client.Context())
	return err
}

// GetTransaction returns the bytes for the Transaction with the given ID
func (r *Redis) GetTx(txID ids.ID) ([]byte, error) {
	cmd := r.client.Get(r.client.Context(), redisIndexKeysTxByID(r.chainID, txID))
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	return cmd.Bytes()
}

// GetTransactionCount returns the number of transactions this Server as seen
func (r *Redis) GetTxCount() (uint64, error) {
	cmd := r.client.Get(r.client.Context(), redisIndexKeysTxCount(r.chainID))
	if err := cmd.Err(); err != nil {
		return 0, err
	}
	return cmd.Uint64()
}

// GetRecentTransactions returns a list of the N most recent transactions
func (r *Redis) GetRecentTransactions(n int64) ([]ids.ID, error) {
	cmd := r.client.LRange(r.client.Context(), redisIndexKeysRecentTxs(r.chainID), 0, n-1)
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

func redisIndexKeysTxByID(chainID ids.ID, txID ids.ID) string {
	return redisIndexKey(chainID, redisKeyPrefixesTxByID, txID.String())
}

func redisIndexKeysTxCount(chainID ids.ID) string {
	return redisIndexKey(chainID, redisKeyPrefixesTxCount)
}

func redisIndexKeysRecentTxs(chainID ids.ID) string {
	return redisIndexKey(chainID, redisKeyPrefixesRecentTxs)
}

func redisIndexKey(chainID ids.ID, keyName string, extra ...string) string {
	return strings.Join(append([]string{
		chainID.String(),
		keyName,
	}, extra...), "|")
}
