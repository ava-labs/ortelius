package indexer

import (
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/formatting"
	"github.com/ava-labs/avalanchego/utils/json"

	"github.com/ava-labs/avalanchego/utils/rpc"
)

type Client struct {
	rpc.EndpointRequester
}

type FormattedContainer struct {
	ID        ids.ID              `json:"id"`
	Bytes     string              `json:"bytes"`
	Timestamp time.Time           `json:"timestamp"`
	Encoding  formatting.Encoding `json:"encoding"`
	Index     json.Uint64         `json:"index"`
}

type GetContainerRangeArgs struct {
	StartIndex json.Uint64         `json:"startIndex"`
	NumToFetch json.Uint64         `json:"numToFetch"`
	Encoding   formatting.Encoding `json:"encoding"`
}

type GetContainerRangeResponse struct {
	Containers []FormattedContainer `json:"containers"`
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

// NewClient creates a client that can interact with an index via HTTP API calls.
// [host] is the host to make API calls to (e.g. http://1.2.3.4:9650).
// [endpoint] is the path to the index endpoint (e.g. /ext/index/C/block or /ext/index/X/tx).
func NewClient(host, endpoint string, requestTimeout time.Duration) *Client {
	return &Client{
		EndpointRequester: rpc.NewEndpointRequester(host, endpoint, "index", requestTimeout),
	}
}

func (c *Client) GetContainerRange(args *GetContainerRangeArgs) ([]FormattedContainer, error) {
	var response *GetContainerRangeResponse
	err := c.SendRequest("getContainerRange", args, &response)
	return response.Containers, err
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
