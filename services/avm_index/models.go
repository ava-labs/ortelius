// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"
)

var (
	VMName = "avm"

	OutputTypesSECP2556K1Transfer OutputType = 0x000000ff

	TXTypeBase        TxType = "base"
	TXTypeCreateAsset        = "create_asset"
	TXTypeImport             = "import"
	TXTypeExport             = "export"
)

type TxType string
type OutputType uint32

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

// rawID represents an ids.ID in the database
type rawID []byte

// Equals returns true if and only if the two rawIDs represent the same ids.ID
func (rid rawID) Equals(oRID rawID) bool {
	return string(rid) == string(oRID)
}

// MarshalJSON returns a json-marshaled string representing the ID
func (rid rawID) MarshalJSON() ([]byte, error) {
	id, err := ids.ToID(rid)
	if err != nil {
		return nil, err
	}
	return json.Marshal(id.String())
}

// rawShortID represents an ids.ShortID in the database
type rawShortID []byte

// Equals returns true if and only if the two rawShortID represent the same
// ids.rawShortID
func (rid rawShortID) Equals(oRID rawShortID) bool {
	return string(rid) == string(oRID)
}

// MarshalJSON returns a json-marshaled string representing the ID
func (rid rawShortID) MarshalJSON() ([]byte, error) {
	id, err := ids.ToShortID(rid)
	if err != nil {
		return nil, err
	}
	return json.Marshal(id.String())
}

// transaction represents a tx in the db
type transaction struct {
	ID      rawID  `json:"id"`
	ChainID rawID  `json:"chainID"`
	Type    string `json:"type"`

	CanonicalSerialization []byte
	JSONSerialization      []byte `dbr:"json_serialization"`

	InputCount  uint16
	OutputCount uint16
	Amount      uint64
	IngestedAt  time.Time
}

// output represents a tx output in the db
type output struct {
	TransactionID rawID        `json:"transactionID"`
	OutputIndex   uint16       `json:"outputIndex"`
	AssetID       rawID        `json:"assetOD"`
	OutputType    OutputType   `json:"outputType"`
	Amount        uint64       `json:"amount"`
	Locktime      uint64       `json:"locktime"`
	Threshold     uint64       `json:"threshold"`
	Addresses     []rawShortID `json:"addresses"`

	RedeemingTransactionID []byte `json:"redeemingTransactionID"`
	RedeemingSignature     []byte `json:"redeemingSignature"`
}

// output represents an address that controls a tx output in the db
type outputAddress struct {
	TransactionID      rawID
	OutputIndex        uint16
	Address            rawShortID
	RedeemingSignature dbr.NullString
}

type asset struct {
	ID      rawID `json:"id"`
	ChainID rawID `json:"chainID"`

	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Alias        string `json:"alias"`
	Denomination uint8  `json:"denomination"`

	CurrentSupply uint64 `json:"currentSupply"`
}

type chainInfo struct {
	ID        ids.ID `json:"chainID"`
	Alias     string `json:"chainAlias"`
	VM        string `json:"vm"`
	NetworkID uint32 `json:"networkID"`

	TransactionCount int64 `json:"transactionCount"`
	AssetCount       int64 `json:"assetCount"`
}

type timestampedTx struct {
	json.RawMessage `db:"json_serialization"`
	Timestamp       time.Time `db:"ingested_at" json:"timestamp"`
}

func (tt *timestampedTx) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	err := json.Unmarshal(tt.RawMessage, &m)
	if err != nil {
		return nil, err
	}
	m["timestamp"] = tt.Timestamp
	return json.Marshal(m)
}
