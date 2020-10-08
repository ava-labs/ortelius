// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"time"
)

type ListMetadata struct {
	Count uint64 `json:"count"`
}

type TransactionList struct {
	ListMetadata
	Transactions []*Transaction `json:"transactions"`
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

type AggregatesHistogram struct {
	Aggregates   Aggregates    `json:"aggregates"`
	IntervalSize time.Duration `json:"intervalSize,omitempty"`
	Intervals    []Aggregates  `json:"intervals,omitempty"`
}

type Aggregates struct {
	// Idx is used internally when creating a histogram of Aggregates.
	// It is exported only so it can be written to by dbr.
	Idx int `json:"-"`

	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	TransactionVolume TokenAmount `json:"transactionVolume"`

	TransactionCount uint64 `json:"transactionCount"`
	AddressCount     uint64 `json:"addressCount"`
	OutputCount      uint64 `json:"outputCount"`
	AssetCount       uint64 `json:"assetCount"`
}
