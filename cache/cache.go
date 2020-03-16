package cache

import (
	"github.com/ava-labs/gecko/ids"
)

// Ensure backends conform the the interfaces
var (
	_ Accumulator = &RedisBackend{}
	_ Server      = &RedisBackend{}
)

// Accumulator takes in txs and adds them to the cache backend
type Accumulator interface {
	AddTx(ids.ID, []byte) error
}

// Server makes contents of the cache backend available for simple querying
type Server interface {
	GetTx(ids.ID) ([]byte, error)
	GetTxCount() (int64, error)
	GetRecentTxs(int64) ([]ids.ID, error)
}
