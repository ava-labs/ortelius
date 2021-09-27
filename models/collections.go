// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"time"

	"github.com/ava-labs/avalanchego/ids"
)

type ListMetadata struct {
	Count *uint64 `json:"count,omitempty"`
}

type TransactionList struct {
	ListMetadata

	Transactions []*Transaction `json:"transactions"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`

	Next *string `json:"next,omitempty"`
}

type CvmTransactionsTxDataTrace struct {
	Hash                       *string `json:"hash,omitempty"`
	Idx                        *uint32 `json:"idx,omitempty"`
	CallType                   string  `json:"callType"`
	ToAddr                     string  `json:"to"`
	FromAddr                   string  `json:"from"`
	Type                       string  `json:"type"`
	GasUsed                    string  `json:"gasUsed"`
	Gas                        string  `json:"gas"`
	Input                      *string `json:"input,omitempty"`
	Output                     *string `json:"output,omitempty"`
	Value                      string  `json:"value"`
	CreatedContractAddressHash *string `json:"createdContractAddressHash,omitempty"`
	Init                       *string `json:"init,omitempty"`
	CreatedContractCode        *string `json:"createdContractCode,omitempty"`
	Error                      *string `json:"error,omitempty"`
	RevertReason               *string `json:"revertReason,omitempty"`
	RevertReasonUnpacked       *string `json:"revertReasonUnpacked,omitempty"`
	TraceAddress               []int   `json:"traceAddress,omitempty"`
}

type CTransactionData struct {
	Type          int       `json:"type"`
	Block         string    `json:"block"`
	Hash          string    `json:"hash"`
	CreatedAt     time.Time `json:"createdAt"`
	Nonce         uint64    `json:"nonce"`
	GasPrice      *string   `json:"gasPrice,omitempty"`
	GasFeeCap     *string   `json:"maxFeePerGas,omitempty"`
	GasTipCap     *string   `json:"maxPriorityFeePerGas,omitempty"`
	GasLimit      uint64    `json:"gasLimit"`
	BlockGasUsed  uint64    `json:"blockGasUsed"`
	BlockGasLimit uint64    `json:"blockGasLimit"`
	BlockNonce    uint64    `json:"blockNonce"`
	BlockHash     string    `json:"blockHash"`
	Recipient     *string   `json:"recipient,omitempty"`
	Amount        *string   `json:"value,omitempty"`
	Payload       *string   `json:"input,omitempty"`
	ToAddr        string    `json:"toAddr"`
	FromAddr      string    `json:"fromAddr"`

	// Signature values
	V *string `json:"v,omitempty"`
	R *string `json:"r,omitempty"`
	S *string `json:"s,omitempty"`

	Traces    []*CvmTransactionsTxDataTrace          `json:"traces"`
	TracesMax uint32                                 `json:"-"`
	TracesMap map[uint32]*CvmTransactionsTxDataTrace `json:"-"`
}

type CTransactionList struct {
	Transactions []*CTransactionData
	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`
}

type AssetList struct {
	ListMetadata
	Assets []*Asset `json:"assets"`
}

type AddressList struct {
	ListMetadata
	Addresses []*AddressInfo `json:"addresses"`
}

// SearchResults represents a set of items returned for a search query.
type SearchResults struct {
	// Count is the total number of matching results
	Count uint64 `json:"count"`

	// Results is a list of SearchResult
	Results SearchResultSet `json:"results"`
}

type SearchResultSet []SearchResult

func (s SearchResultSet) Len() int           { return len(s) }
func (s SearchResultSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SearchResultSet) Less(i, j int) bool { return s[i].Score < s[j].Score }

// SearchResult represents a single item matching a search query.
type SearchResult struct {
	// SearchResultType is the type of object found
	SearchResultType `json:"type"`

	// Data is the object itself
	Data interface{} `json:"data"`

	// Score is a rank of how well this result matches the query
	Score uint64 `json:"score"`
}

type TxfeeAggregatesHistogram struct {
	TxfeeAggregates TxfeeAggregates   `json:"aggregates"`
	IntervalSize    time.Duration     `json:"intervalSize,omitempty"`
	Intervals       []TxfeeAggregates `json:"intervals,omitempty"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`
}

type TxfeeAggregates struct {
	// Idx is used internally when creating a histogram of Aggregates.
	// It is exported only so it can be written to by dbr.
	Idx int `json:"-"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`

	Txfee TokenAmount `json:"txfee"`
}

type AggregatesHistogram struct {
	Aggregates   Aggregates    `json:"aggregates"`
	IntervalSize time.Duration `json:"intervalSize,omitempty"`
	Intervals    []Aggregates  `json:"intervals,omitempty"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`
}

type Aggregates struct {
	// Idx is used internally when creating a histogram of Aggregates.
	// It is exported only so it can be written to by dbr.
	Idx int `json:"-"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`

	TransactionVolume TokenAmount `json:"transactionVolume"`
	TransactionCount  uint64      `json:"transactionCount"`
	AddressCount      uint64      `json:"addressCount"`
	OutputCount       uint64      `json:"outputCount"`
	AssetCount        uint64      `json:"assetCount"`
}

type AddressChains struct {
	AddressChains map[string][]StringID `json:"addressChains"`
}

type AssetAggregate struct {
	Asset     ids.ID               `json:"asset"`
	Aggregate *AggregatesHistogram `json:"aggregate"`
}
