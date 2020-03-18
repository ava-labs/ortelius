package services

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/go-redis/redis"
)

// RecentTxsSize is the number of transactions we want to keep cached in the
// recent txs list in redis
const RedisRecentTxsSize = 100_000

// RedisIndex is an Accumulator and Index backed by redis
type RedisIndex struct {
	client *redis.Client
}

// NewRedisIndex creates a new RedisIndex for the given config
func NewRedisIndex(opts *redis.Options) (*RedisIndex, error) {
	client := redis.NewClient(opts)

	// Perform a liveness check on the backend service
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &RedisIndex{client: client}, nil
}

// AddTx ingests a transaction and adds it to the services
func (r *RedisIndex) AddTx(id ids.ID, body []byte) error {
	idStr := id.String()
	pipe := r.client.TxPipeline()

	if err := pipe.Set(idStr, body, 0).Err(); err != nil {
		return err
	}

	if err := pipe.Incr("tx_count").Err(); err != nil {
		return err
	}

	if err := pipe.LPush("recent_txs", idStr).Err(); err != nil {
		return err
	}

	if err := pipe.LTrim("recent_txs", 0, RedisRecentTxsSize-1).Err(); err != nil {
		return err
	}

	_, err := pipe.Exec()
	return err
}

// GetTx returns the bytes for the transaction with the given ID
func (r *RedisIndex) GetTx(id ids.ID) ([]byte, error) {
	cmd := r.client.Get(id.String())
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	return cmd.Bytes()
}

// GetTxCount returns the number of transactions this Server as seen
func (r *RedisIndex) GetTxCount() (int64, error) {
	cmd := r.client.Get("tx_count")
	if err := cmd.Err(); err != nil {
		return 0, err
	}
	return cmd.Int64()
}

// GetRecentTxs returns a list of the N most recent transactions
func (r *RedisIndex) GetRecentTxs(n int64) ([]ids.ID, error) {
	cmd := r.client.LRange("recent_txs", 0, n-1)
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
