package cblock

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ava-labs/coreth/core/types"
)

type Block struct {
	Header         types.Header        `json:"header"`
	Uncles         []types.Header      `json:"uncles"`
	TxsBytes       [][]byte            `json:"txs"`
	Version        uint32              `json:"version"`
	ReceivedAt     time.Time           `json:"received_at"`
	BlockExtraData []byte              `json:"blockExtraData"`
	Txs            []types.Transaction `json:"transactions,omitempty"`
}

func New(bl *types.Block) (*Block, error) {
	var cblock Block
	cblock.Version = bl.Version()
	cblock.ReceivedAt = bl.ReceivedAt
	cblock.BlockExtraData = bl.ExtraData()
	var h *types.Header = bl.Header()
	if h != nil {
		cblock.Header = *h
	}
	for _, u := range bl.Uncles() {
		if u == nil {
			continue
		}
		cblock.Uncles = append(cblock.Uncles, *u)
	}
	for _, t := range bl.Transactions() {
		bdata, err := t.MarshalJSON()
		if err != nil {
			return nil, err
		}
		cblock.TxsBytes = append(cblock.TxsBytes, bdata)
	}
	return &cblock, nil
}

func Marshal(bl *types.Block) ([]byte, error) {
	b, err := New(bl)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("invalid block")
	}
	return json.Marshal(b)
}

func Unmarshal(data []byte) (*Block, error) {
	var block Block
	err := json.Unmarshal(data, &block)
	if err != nil {
		return nil, err
	}

	// convert the tx bytes into transactions.
	for _, t := range block.TxsBytes {
		var tr types.Transaction
		err := tr.UnmarshalJSON(t)
		if err != nil {
			return nil, err
		}
		block.Txs = append(block.Txs, tr)
	}
	return &block, err
}

type TransactionTrace struct {
	Hash  string `json:"hash"`
	Idx   uint32 `json:"idx"`
	Trace []byte `json:"trace"`
}
