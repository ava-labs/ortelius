package cblock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/coreth/ethclient"
	"github.com/ava-labs/coreth/rpc"

	"github.com/ava-labs/coreth/core/types"
)

var ErrNotFound = errors.New("block not found")

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

type Client struct {
	rpcClient *rpc.Client
	ethClient *ethclient.Client
	lock      sync.Mutex
}

func NewClient(url string) (*Client, error) {
	rc, err := rpc.Dial(url)
	if err != nil {
		return nil, err
	}
	cl := &Client{}
	cl.rpcClient = rc
	cl.ethClient = ethclient.NewClient(rc)
	return cl, nil
}

func (c *Client) Latest(rpcTimeout time.Duration) (*big.Int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()
	bl, err := c.ethClient.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
	return big.NewInt(0).SetUint64(bl), nil
}

func (c *Client) Close() {
	c.rpcClient.Close()
}

type TracerParam struct {
	Tracer string `json:"tracer"`
}

func ReadBlockFromRPC(client *Client, blockNumber *big.Int, rpcTimeout time.Duration) (*types.Block, []*TransactionTrace, error) {
	client.lock.Lock()
	defer client.lock.Unlock()

	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()

	bl, err := client.ethClient.BlockByNumber(ctx, blockNumber)
	if err != nil {
		return nil, nil, err
	}

	txTraces := make([]*TransactionTrace, 0, len(bl.Transactions()))
	for _, tx := range bl.Transactions() {
		txh := tx.Hash().Hex()
		if !strings.HasPrefix(txh, "0x") {
			txh = "0x" + txh
		}
		var results []interface{}
		err = client.rpcClient.CallContext(ctx, &results, "debug_traceTransaction", txh, TracerParam{Tracer: Tracer})
		if err != nil {
			return nil, nil, err
		}
		for ipos, result := range results {
			traceBits, err := json.Marshal(result)
			if err != nil {
				return nil, nil, err
			}
			txTraces = append(txTraces,
				&TransactionTrace{
					Hash:  txh,
					Idx:   uint32(ipos),
					Trace: traceBits,
				},
			)
		}
	}

	return bl, txTraces, nil
}
