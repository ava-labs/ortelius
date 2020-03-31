package services

import (
	"github.com/ava-labs/gecko/ids"
)

// Accumulator takes in txs and adds them to the services backend
type Accumulator interface {
	AddTx(ids.ID, ids.ID, []byte) error
}

// Index makes data available for simple querying
type Index interface {
	GetTx(ids.ID) ([]byte, error)
	GetTxCount() (int64, error)
	GetRecentTxs(int64) ([]ids.ID, error)
}
