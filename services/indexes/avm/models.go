// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"time"

	"github.com/ava-labs/ortelius/services/models"
)

var (
	VMName = "avm"

	OutputTypesSECP2556K1Transfer OutputType = 0x000000ff

	TXTypeBase        TransactionType = "base"
	TXTypeCreateAsset TransactionType = "create_asset"
	TXTypeImport      TransactionType = "import"
	TXTypeExport      TransactionType = "export"

	ResultTypeTransaction SearchResultType = "transaction"
	ResultTypeAsset       SearchResultType = "asset"
	ResultTypeAddress     SearchResultType = "address"
	ResultTypeOutput      SearchResultType = "output"
)

//
// Base models
//

type Transaction struct {
	ID      models.StringID `json:"id"`
	ChainID models.StringID `json:"chainID"`
	Type    string          `json:"type"`

	Inputs  []*Input  `json:"inputs"`
	Outputs []*Output `json:"outputs"`

	InputTotals         AssetTokenCounts `json:"inputTotals"`
	OutputTotals        AssetTokenCounts `json:"outputTotals"`
	ReusedAddressTotals AssetTokenCounts `json:"reusedAddressTotals"`

	CanonicalSerialization []byte    `json:"canonicalSerialization,omitempty"`
	CreatedAt              time.Time `json:"timestamp"`

	Score uint64 `json:"-"`
}

type Input struct {
	Output *Output            `json:"output"`
	Creds  []InputCredentials `json:"credentials"`
}

type Output struct {
	ID            models.StringID        `json:"id"`
	TransactionID models.StringID        `json:"transactionID"`
	OutputIndex   uint64                 `json:"outputIndex"`
	AssetID       models.StringID        `json:"assetID"`
	OutputType    OutputType             `json:"outputType"`
	Amount        TokenAmount            `json:"amount"`
	Locktime      uint64                 `json:"locktime"`
	Threshold     uint64                 `json:"threshold"`
	Addresses     []models.StringShortID `json:"addresses"`
	CreatedAt     time.Time              `json:"timestamp"`

	RedeemingTransactionID models.StringID `json:"redeemingTransactionID"`

	Score uint64 `json:"-"`
}

type InputCredentials struct {
	Address   models.StringShortID `json:"address"`
	PublicKey []byte               `json:"public_key"`
	Signature []byte               `json:"signature"`
}

type OutputAddress struct {
	OutputID  models.StringID      `json:"output_id"`
	Address   models.StringShortID `json:"address"`
	Signature []byte               `json:"signature"`
	CreatedAt time.Time            `json:"timestamp"`
	PublicKey []byte               `json:"-"`
}

type Asset struct {
	ID      models.StringID `json:"id"`
	ChainID models.StringID `json:"chainID"`

	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Alias        string `json:"alias"`
	Denomination uint8  `json:"denomination"`

	CurrentSupply TokenAmount `json:"currentSupply"`
	CreatedAt     time.Time   `json:"timestamp"`

	Score uint64 `json:"-"`
}

type Address struct {
	Address models.StringShortID `json:"address"`
	ChainID models.StringID      `json:"chainID"`

	PublicKey        []byte `json:"publicKey"`
	TransactionCount uint64 `json:"transactionCount"`

	Balance       TokenAmount `json:"balance"`
	TotalReceived TokenAmount `json:"totalReceived"`
	TotalSent     TokenAmount `json:"totalSent"`
	UTXOCount     uint64      `json:"utxoCount"`

	Score uint64 `json:"-"`
}

//
// Lists
//

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
	Addresses []*Address `json:"addresses"`
}

type OutputList struct {
	ListMetadata
	Outputs []*Output `json:"outputs"`
}

//
// Search
//

// SearchResults represents a set of items returned for a search query.
type SearchResults struct {
	// Count is the total number of matching results
	Count uint64 `json:"count"`

	// Results is a list of SearchResult
	Results SearchResultSet `json:"results"`
}

type SearchResultSet []SearchResult

func (srs SearchResultSet) Len() int         { return len(srs) }
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

//
// Aggregates
//

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
