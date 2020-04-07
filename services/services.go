package services

import (
	"github.com/ava-labs/gecko/ids"
)

type Ingestable interface {
	ID() ids.ID
	ChainID() ids.ID
	Body() []byte
}

// Accumulator takes in txs and adds them to the services backend
type Accumulator interface {
	AddTx(Ingestable) error
}
