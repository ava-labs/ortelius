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

// RedisIndex is an Accumulator and Index backed by redis
type RedisIndex struct {
	client *redis.Client
}

// NewRedisIndex creates a new RedisIndex for the given config
func NewRedisIndex(client *redis.Client) *RedisIndex {
	return &RedisIndex{client}
}

// AddTx ingests a transaction and adds it to the services
func (r *RedisIndex) AddTx(chainID ids.ID, txID ids.ID, body []byte) error {
	var (
		pipe = r.client.TxPipeline()

		txByIDKey    = redisIndexKeysTxByID(chainID, txID)
		txCountKey   = redisIndexKeysTxCount(chainID)
		recentTxsKey = redisIndexKeysRecentTxs(chainID)
	)

	if err := pipe.Set(txByIDKey, body, 0).Err(); err != nil {
		return err
	}

	if err := pipe.Incr(txCountKey).Err(); err != nil {
		return err
	}

	if err := pipe.LPush(recentTxsKey, txID.String()).Err(); err != nil {
		return err
	}

	if err := pipe.LTrim(recentTxsKey, 0, redisRecentTxsSize-1).Err(); err != nil {
		return err
	}

	_, err := pipe.Exec()
	return err
}

// GetTx returns the bytes for the transaction with the given ID
func (r *RedisIndex) GetTx(chainID ids.ID, txID ids.ID) ([]byte, error) {
	cmd := r.client.Get(redisIndexKeysTxByID(chainID, txID))
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	return cmd.Bytes()
}

// GetTxCount returns the number of transactions this Server as seen
func (r *RedisIndex) GetTxCount(chainID ids.ID) (int64, error) {
	cmd := r.client.Get(redisIndexKeysTxCount(chainID))
	if err := cmd.Err(); err != nil {
		return 0, err
	}
	return cmd.Int64()
}

// GetRecentTxs returns a list of the N most recent transactions
func (r *RedisIndex) GetRecentTxs(chainID ids.ID, n int64) ([]ids.ID, error) {
	cmd := r.client.LRange(redisIndexKeysRecentTxs(chainID), 0, n-1)
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
