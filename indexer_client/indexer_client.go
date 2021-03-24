package indexer

import (
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/formatting"
	"github.com/ava-labs/avalanchego/utils/json"

	"github.com/ava-labs/avalanchego/utils/rpc"
)

type IndexType byte

const (
	IndexTypeTransactions IndexType = iota
	IndexTypeVertices
	IndexTypeBlocks

	typeUnknown = "unknown"
)

func (t IndexType) String() string {
	switch t {
	case IndexTypeTransactions:
		return "tx"
	case IndexTypeVertices:
		return "vtx"
	case IndexTypeBlocks:
		return "block"
	}
	return typeUnknown
}

type IndexedChain byte

const (
	XChain IndexedChain = iota
	PChain
	CChain
)

func (t IndexedChain) String() string {
	switch t {
	case XChain:
		return "X"
	case PChain:
		return "P"
	case CChain:
		return "C"
	}
	// Should never happen
	return typeUnknown
}

type Client struct {
	rpc.EndpointRequester
}

// NewClient creates a client.
func NewClient(uri string, chain IndexedChain, indexType IndexType, requestTimeout time.Duration) (*Client, error) {
	switch {
	case chain == XChain && indexType == IndexTypeBlocks:
		return nil, errors.New("X-Chain doesn't have blocks")
	case (chain == PChain || chain == CChain) && indexType != IndexTypeBlocks:
		return nil, errors.New("P-Chain and C-Chain only blocks")
	case chain != XChain && chain != PChain && chain != CChain:
		return nil, errors.New("invalid chain given")
	case indexType != IndexTypeTransactions && indexType != IndexTypeVertices && indexType != IndexTypeBlocks:
		return nil, errors.New("invalid chain given")
	}

	return &Client{
		EndpointRequester: rpc.NewEndpointRequester(uri, fmt.Sprintf("ext/index/%s/%s", chain, indexType), "index", requestTimeout),
	}, nil
}

type FormattedContainer struct {
	ID        string              `json:"id"`
	Bytes     string              `json:"bytes"`
	Timestamp time.Time           `json:"timestamp"`
	Encoding  formatting.Encoding `json:"encoding"`
}
type GetContainerRange struct {
	StartIndex json.Uint64         `json:"startIndex"`
	NumToFetch json.Uint64         `json:"numToFetch"`
	Encoding   formatting.Encoding `json:"encoding"`
}
type GetContainer struct {
	Index    json.Uint64         `json:"index"`
	Encoding formatting.Encoding `json:"encoding"`
}
type GetLastAcceptedArgs struct {
	Encoding formatting.Encoding `json:"encoding"`
}
type GetIndexArgs struct {
	ContainerID ids.ID              `json:"containerID"`
	Encoding    formatting.Encoding `json:"encoding"`
}

type GetIndexResponse struct {
	Index json.Uint64 `json:"index"`
}

func (c *Client) GetContainerRange(args *GetContainerRange) ([]FormattedContainer, error) {
	var response []FormattedContainer
	err := c.SendRequest("getContainerRange", args, &response)
	return response, err
}

func (c *Client) GetContainerByIndex(args *GetContainer) (FormattedContainer, error) {
	var response FormattedContainer
	err := c.SendRequest("getContainerByIndex", args, &response)
	return response, err
}

func (c *Client) GetLastAccepted(args *GetLastAcceptedArgs) (FormattedContainer, error) {
	var response FormattedContainer
	err := c.SendRequest("getLastAccepted", args, &response)
	return response, err
}

func (c *Client) GetIndex(args *GetIndexArgs) (GetIndexResponse, error) {
	var response GetIndexResponse
	err := c.SendRequest("getIndex", args, &response)
	return response, err
}

func (c *Client) IsAccepted(args *GetIndexArgs) (bool, error) {
	var response bool
	err := c.SendRequest("isAccepted", args, &response)
	return response, err
}
