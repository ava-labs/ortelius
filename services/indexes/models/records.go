// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"time"
)

// ChainInfo represents an overview of data about a given chain
type ChainInfo struct {
	ID          StringID `json:"chainID"`
	Alias       string   `json:"chainAlias"`
	VM          string   `json:"vm"`
	AVAXAssetID StringID `json:"avaxAssetID"`
	NetworkID   uint32   `json:"networkID"`
}

type Transaction struct {
	ID      StringID `json:"id"`
	ChainID StringID `json:"chainID"`
	Type    string   `json:"type"`

	Inputs  []*Input  `json:"inputs"`
	Outputs []*Output `json:"outputs"`

	Memo []byte `json:"memo"`

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
	ID            StringID    `json:"id"`
	TransactionID StringID    `json:"transactionID"`
	OutputIndex   uint64      `json:"outputIndex"`
	AssetID       StringID    `json:"assetID"`
	OutputType    OutputType  `json:"outputType"`
	Amount        TokenAmount `json:"amount"`
	Locktime      uint64      `json:"locktime"`
	Threshold     uint64      `json:"threshold"`
	Addresses     []Address   `json:"addresses"`
	CreatedAt     time.Time   `json:"timestamp"`

	RedeemingTransactionID StringID `json:"redeemingTransactionID"`

	Score uint64 `json:"-"`
}

type InputCredentials struct {
	Address   Address `json:"address"`
	PublicKey []byte  `json:"public_key"`
	Signature []byte  `json:"signature"`
}

type OutputAddress struct {
	OutputID  StringID `json:"output_id"`
	Address   Address  `json:"address"`
	Signature []byte   `json:"signature"`
	PublicKey []byte   `json:"-"`
}

type Asset struct {
	ID      StringID `json:"id"`
	ChainID StringID `json:"chainID"`

	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Alias        string `json:"alias"`
	Denomination uint8  `json:"denomination"`

	CurrentSupply TokenAmount `json:"currentSupply"`
	CreatedAt     time.Time   `json:"timestamp"`

	Score uint64 `json:"-"`
}

type AssetInfo struct {
	AssetID StringID `json:"id"`

	TransactionCount uint64      `json:"transactionCount"`
	UTXOCount        uint64      `json:"utxoCount"`
	Balance          TokenAmount `json:"balance"`
	TotalReceived    TokenAmount `json:"totalReceived"`
	TotalSent        TokenAmount `json:"totalSent"`
}

type AddressInfo struct {
	Address   Address `json:"address"`
	PublicKey []byte  `json:"publicKey"`

	Assets map[StringID]AssetInfo `json:"assets"`

	Score uint64 `json:"-"`
}

type OutputList struct {
	ListMetadata
	Outputs []*Output `json:"outputs"`
}
