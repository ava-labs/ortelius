// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"
)

var (
	AVMOutputTypesSECP2556K1Transfer AVMOutputType = 0x000000ff
)

type AVMOutputType uint32

func (ot AVMOutputType) Equals(otherOT AVMOutputType) bool {
	return uint32(ot) == uint32(otherOT)
}

type AVMOutput struct {
	AssetID string                      `json:"assetID"`
	Output  AVMSECP2556K1TransferOutput `json:"output"`
}

type AVMSECP2556K1TransferOutput struct {
	Amount    uint64   `json:"amount"`
	Locktime  uint32   `json:"locktime"`
	Threshold uint32   `json:"threshold"`
	Addresses []string `json:"addresses"`
}

type AVMTransferTx struct {
	UnsignedTx struct {
		NetworkID    uint32      `json:"networkID"`
		BlockchainID string      `json:"blockchainID"`
		Outputs      []AVMOutput `json:"outputs"`
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

// ID returns the ids.ID represented by the rawID
func (rid rawID) ID() (ids.ID, error) {
	return ids.ToID(rid)
}

// String returns the string representation of the ids.ID represented by the
// rawID
func (rid rawID) String() (string, error) {
	id, err := rid.ID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

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

// ID returns the ids.ShortID represented by the rawShortID
func (rid rawShortID) ID() (ids.ShortID, error) {
	return ids.ToShortID(rid)
}

// String returns the string representation of the ids.ShortID represented by
// the rawShortID
func (rid rawShortID) String() (string, error) {
	id, err := rid.ID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

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
	ID        rawID
	ChainID   rawID
	NetworkID uint16

	CanonicalSerialization []byte

	InputCount  uint16
	OutputCount uint16
	Amount      uint64
}

// output represents a tx output in the db
type output struct {
	TransactionID rawID         `json:"transaction_id"`
	OutputIndex   uint16        `json:"output_index"`
	AssetID       rawID         `json:"asset_id"`
	OutputType    AVMOutputType `json:"output_type"`
	Amount        uint64        `json:"amount"`
	Locktime      uint64        `json:"locktime"`
	Threshold     uint64        `json:"threshold"`
	Addresses     []rawShortID  `json:"addresses"`

	RedeemingTransactionID []byte `json:"redeeming_transaction_id"`
	RedeemingSignature     []byte `json:"redeeming_signature"`
}

// output represents an address that controls a tx output in the db
type outputAddress struct {
	TransactionID      rawID
	OutputIndex        uint16
	Address            rawShortID
	RedeemingSignature dbr.NullString
}
