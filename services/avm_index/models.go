// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"time"

	"github.com/ava-labs/gecko/ids"
)

var (
	VMName = "avm"

	OutputTypesSECP2556K1Transfer OutputType = 0x000000ff

	TXTypeBase        TxType = "base"
	TXTypeCreateAsset TxType = "create_asset"
	TXTypeImport      TxType = "import"
	TXTypeExport      TxType = "export"

	ResultTypeTx      ResultType = "transaction"
	ResultTypeAsset   ResultType = "asset"
	ResultTypeAddress ResultType = "address"
	ResultTypeOutput  ResultType = "output"

	IntervalMinute = 1 * time.Minute
	IntervalHour   = 60 * time.Minute
	IntervalDay    = 1440 * time.Minute
	IntervalWeek   = 7 * IntervalDay
	IntervalMonth  = 30 * IntervalDay
	IntervalYear   = 365 * IntervalDay
	IntervalAll    = time.Duration(0)

	IntervalNames = map[string]time.Duration{
		"minute": IntervalMinute,
		"hour":   IntervalHour,
		"day":    IntervalDay,
		"week":   IntervalWeek,
		"month":  IntervalMonth,
		"year":   IntervalYear,
		"all":    IntervalAll,
	}

	// // TimeframeTicks maps a total duration to the duration of ticks we want to
	// // show for that total duration.
	// TimeframeTicks = map[time.Duration]time.Duration{
	// 	IntervalMinute: time.Minute,      // 60 ticks
	// 	IntervalHour:   15 * time.Second, // 240 ticks
	// 	IntervalDay:    5 * time.Minute,  // 288 ticks
	// 	IntervalWeek:   time.Hour,        // 168 ticks
	// 	IntervalMonth:  3 * time.Hour,    // 240 ticks
	// 	IntervalYear:   IntervalDay,      // 365 ticks
	//
	// 	// The "all" timeframe is not used for histograms
	// 	IntervalAll: 0,
	// }
)

func parseInterval(s string) (time.Duration, error) {
	interval, ok := IntervalNames[s]
	if !ok {
		var err error
		interval, err = time.ParseDuration(s)
		if err != nil {
			return 0, err
		}
	}
	return interval, nil
}

type TxType string
type OutputType uint32

type ResultType string

type Output struct {
	AssetID string                   `json:"assetID"`
	Output  SECP2556K1TransferOutput `json:"output"`
}

type SECP2556K1TransferOutput struct {
	Amount    uint64   `json:"amount"`
	Locktime  uint32   `json:"locktime"`
	Threshold uint32   `json:"threshold"`
	Addresses []string `json:"addresses"`
}

type BaseTx struct {
	UnsignedTx struct {
		NetworkID    uint32   `json:"networkID"`
		BlockchainID string   `json:"blockchainID"`
		Outputs      []Output `json:"outputs"`
		Inputs       []struct {
			TxID        string `json:"txID"`
			OutputIndex int    `json:"outputIndex"`
			AssetID     string `json:"assetID"`
			Input       struct {
				Amount           uint64 `json:"amount"`
				SignatureIndices []int  `json:"signatureIndices"`
			} `json:"input"`
		} `json:"inputs"`
	} `json:"unsignedTx"`
	Credentials []struct {
		Signatures [][]int `json:"signatures"`
	} `json:"credentials"`
}

// stringID represents an ids.ID in the database
type stringID string

func toStringID(id ids.ID) stringID {
	return stringID(id.String())
}

func stringIDFromBytes(b [32]byte) stringID {
	return toStringID(ids.NewID(b))
}

// Equals returns true if and only if the two rawIDs represent the same ids.ID
func (rid stringID) Equals(oRID stringID) bool {
	return string(rid) == string(oRID)
}

// MarshalJSON returns a json-marshaled string representing the ID
func (rid stringID) MarshalJSON() ([]byte, error) {
	if string(rid) == "" {
		return json.Marshal(nil)
	}
	id, err := ids.FromString(string(rid))
	if err != nil {
		return nil, err
	}
	return json.Marshal(id.String())
}

// stringShortID represents an ids.ShortID in the database
type stringShortID string

func toShortStringID(id ids.ShortID) stringShortID {
	return stringShortID(id.String())
}

func stringShortIDFromBytes(b [20]byte) stringShortID {
	return toShortStringID(ids.NewShortID(b))
}

// Equals returns true if and only if the two stringShortID represent the same
// ids.stringShortID
func (rid stringShortID) Equals(oRID stringShortID) bool {
	return string(rid) == string(oRID)
}

// MarshalJSON returns a json-marshaled string representing the ID
func (rid stringShortID) MarshalJSON() ([]byte, error) {
	id, err := ids.ShortFromString(string(rid))
	if err != nil {
		return nil, err
	}
	return json.Marshal(id.String())
}

// transaction represents a tx in the db
type transaction struct {
	ID      stringID `json:"id"`
	ChainID stringID `json:"chainID"`
	Type    string   `json:"type"`

	Inputs  []input  `json:"inputs"`
	Outputs []output `json:"outputs"`

	CanonicalSerialization []byte    `json:"canonicalSerialization,omitempty"`
	CreatedAt              time.Time `json:"timestamp"`
}

type input struct {
	Output *output     `json:"output"`
	Creds  []inputCred `json:"credentials"`
}

// output represents a tx output in the db
type output struct {
	ID            stringID        `json:"id"`
	TransactionID stringID        `json:"transactionID"`
	OutputIndex   uint64          `json:"outputIndex"`
	AssetID       stringID        `json:"assetID"`
	OutputType    OutputType      `json:"outputType"`
	Amount        uint64          `json:"amount"`
	Locktime      uint64          `json:"locktime"`
	Threshold     uint64          `json:"threshold"`
	Addresses     []stringShortID `json:"addresses"`
	CreatedAt     time.Time       `json:"timestamp"`

	RedeemingTransactionID stringID `json:"redeemingTransactionID"`
}

func (o output) CalculateID() (ids.ID, error) {
	txID, err := ids.FromString(string(o.TransactionID))
	if err != nil {
		return ids.ID{}, err
	}

	return txID.Prefix(o.OutputIndex), nil
}

type inputCred struct {
	Address   stringShortID `json:"address"`
	PublicKey []byte        `json:"public_key"`
	Signature []byte        `json:"signature"`
}

// output represents an address that controls a tx output in the db
type outputAddress struct {
	OutputID  stringID      `json:"output_id"`
	Address   stringShortID `json:"address"`
	Signature []byte        `json:"signature"`
	CreatedAt time.Time     `json:"timestamp"`
	PublicKey []byte        `json:"-"`
	// Signature          []byte         `json:"signature"`
}

type asset struct {
	ID      stringID `json:"id"`
	ChainID stringID `json:"chainID"`

	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Alias        string `json:"alias"`
	Denomination uint8  `json:"denomination"`

	CurrentSupply uint64    `json:"currentSupply"`
	CreatedAt     time.Time `json:"timestamp"`
}

type address struct {
	ID ids.ShortID `json:"id"`

	Pubkey           []byte `json:"pubkey"`
	TransactionCount uint64 `json:"transactionCount"`

	Balance uint64 `json:"balance"`
	LTV     uint64 `json:"lifetimeValue"`

	UTXOCount uint64   `json:"utxoCount"`
	UTXOs     []output `json:"utxos"`
}

type searchResults struct {
	Count   uint64         `json:"count"`
	Results []searchResult `json:"results"`
}

type searchResult struct {
	ResultType `json:"type"`
	Data       interface{} `json"data"`
}

type chainInfo struct {
	ID        ids.ID `json:"chainID"`
	Alias     string `json:"chainAlias"`
	VM        string `json:"vm"`
	NetworkID uint32 `json:"networkID"`

	Aggregates chainInfoAggregates `json:"aggregates"`
}

type chainInfoAggregates struct {
	Day TransactionAggregates `json:"day"`
	All TransactionAggregates `json:"all"`
}

type TransactionAggregates struct {
	TXCount     uint64 `json:"transactionCount",db:"tx_count"`
	TXVolume    uint64 `json:"transactionVolume",db:"tx_volume"`
	OutputCount uint64 `json:"outputCount",db:"output_count"`
	AddrCount   uint64 `json:"addressCount"`
	AssetCount  uint64 `json:"assetCount"`
}

type TransactionAggregatesHistogram struct {
	StartTime    time.Time                       `json:"startTime"`
	EndTime      time.Time                       `json:"endTime"`
	Aggregates   TransactionAggregates           `json:"aggregates"`
	IntervalSize time.Duration                   `json:"intervalSize,omitempty"`
	Intervals    []TransactionAggregatesInterval `json:"intervals,omitempty"`
}

type TransactionAggregatesInterval struct {
	StartTime  time.Time             `json:"startTime"`
	EndTime    time.Time             `json:"endTime"`
	Idx        int                   `json:"index"`
	Aggregates TransactionAggregates `json:"aggregates"`
}

type displayTx struct {
	transaction
	json.RawMessage
}

func (dt *displayTx) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	if err := json.Unmarshal(dt.RawMessage, &m); err != nil {
		return nil, err
	}

	txJson, err := json.Marshal(&dt.transaction)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(txJson, &m); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}
