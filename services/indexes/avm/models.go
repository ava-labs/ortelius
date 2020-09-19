// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"
)

var (
	VMName = "avm"

	// Create types for transactions and outputs as defined by the spec, which
	// is documented at:
	// https://docs.avax.network/v1.0/en/references/avm-transaction-serialization

	OutputTypesSECP2556K1Transfer OutputType = 7
	OutputTypesSECP2556K1Mint     OutputType = 6
	OutputTypesNFTMint            OutputType = 10
	OutputTypesNFTTransferOutput  OutputType = 11

	TXTypeBase        models.TransactionType = "base"
	TXTypeCreateAsset models.TransactionType = "create_asset"
	TXTypeImport      models.TransactionType = "import"
	TXTypeExport      models.TransactionType = "export"

	ResultTypeTransaction models.SearchResultType = "transaction"
	ResultTypeAsset       models.SearchResultType = "asset"
	ResultTypeAddress     models.SearchResultType = "address"
	ResultTypeOutput      models.SearchResultType = "output"
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

	Memo []byte `json:"memo"`

	InputTotals         models.AssetTokenCounts `json:"inputTotals"`
	OutputTotals        models.AssetTokenCounts `json:"outputTotals"`
	ReusedAddressTotals models.AssetTokenCounts `json:"reusedAddressTotals"`

	CanonicalSerialization []byte    `json:"canonicalSerialization,omitempty"`
	CreatedAt              time.Time `json:"timestamp"`

	Score uint64 `json:"-"`
}

type Input struct {
	Output *Output            `json:"output"`
	Creds  []InputCredentials `json:"credentials"`
}

type Output struct {
	ID            models.StringID    `json:"id"`
	TransactionID models.StringID    `json:"transactionID"`
	OutputIndex   uint64             `json:"outputIndex"`
	AssetID       models.StringID    `json:"assetID"`
	OutputType    models.OutputType  `json:"outputType"`
	Amount        models.TokenAmount `json:"amount"`
	Locktime      uint64             `json:"locktime"`
	Threshold     uint64             `json:"threshold"`
	Addresses     []models.Address   `json:"addresses"`
	CreatedAt     time.Time          `json:"timestamp"`

	RedeemingTransactionID models.StringID `json:"redeemingTransactionID"`

	GroupID uint32 `json:"groupID"`

	Payload []byte `json:"payload"`

	Score uint64 `json:"-"`
}

type InputCredentials struct {
	Address   models.Address `json:"address"`
	PublicKey []byte         `json:"public_key"`
	Signature []byte         `json:"signature"`
}

type OutputAddress struct {
	OutputID  models.StringID `json:"output_id"`
	Address   models.Address  `json:"address"`
	Signature []byte          `json:"signature"`
	PublicKey []byte          `json:"-"`
}

type Asset struct {
	ID      models.StringID `json:"id"`
	ChainID models.StringID `json:"chainID"`

	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Alias        string `json:"alias"`
	Denomination uint8  `json:"denomination"`

	CurrentSupply models.TokenAmount `json:"currentSupply"`
	CreatedAt     time.Time          `json:"timestamp"`

	Score uint64 `json:"-"`
}

type AddressInfo struct {
	Address   models.Address `json:"address"`
	PublicKey []byte         `json:"publicKey"`

	Assets map[models.StringID]AssetInfo `json:"assets"`

	Score uint64 `json:"-"`
}

type AssetInfo struct {
	AssetID models.StringID `json:"id"`

	TransactionCount uint64             `json:"transactionCount"`
	UTXOCount        uint64             `json:"utxoCount"`
	Balance          models.TokenAmount `json:"balance"`
	TotalReceived    models.TokenAmount `json:"totalReceived"`
	TotalSent        models.TokenAmount `json:"totalSent"`
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
	Addresses []*AddressInfo `json:"addresses"`
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

func (s SearchResultSet) Len() int           { return len(s) }
func (s SearchResultSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SearchResultSet) Less(i, j int) bool { return s[i].Score < s[j].Score }

// SearchResult represents a single item matching a search query.
type SearchResult struct {
	// SearchResultType is the type of object found
	models.SearchResultType `json:"type"`

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

	TransactionVolume models.TokenAmount `json:"transactionVolume"`

	TransactionCount uint64 `json:"transactionCount"`
	AddressCount     uint64 `json:"addressCount"`
	OutputCount      uint64 `json:"outputCount"`
	AssetCount       uint64 `json:"assetCount"`
}
