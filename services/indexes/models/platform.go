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

type Block struct {
	ID        StringID  `json:"id"`
	ParentID  StringID  `json:"parentID"`
	ChainID   StringID  `json:"chainID"`
	Type      BlockType `json:"type"`
	CreatedAt time.Time `json:"createdAt"`
}

type Subnet struct {
	ID          StringID     `json:"id"`
	Threshold   uint64       `json:"threshold"`
	ControlKeys []ControlKey `json:"controlKeys"`
	CreatedAt   time.Time    `json:"createdAt"`
}

type Validator struct {
	TransactionID StringID `json:"transactionID"`

	NodeID StringShortID `json:"nodeID"`
	Weight string        `json:"weight"`

	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	Destination StringShortID `json:"destination"`
	Shares      uint32        `json:"shares"`

	SubnetID StringID `json:"subnetID"`
}

type Chain struct {
	ID                StringID           `json:"id"`
	SubnetID          StringID           `json:"subnetID"`
	Name              string             `json:"name"`
	VMID              StringID           `json:"vmID" db:"vm_id"`
	ControlSignatures []ControlSignature `json:"controlSignatures"`
	FxIDs             []StringID         `json:"fxIDs"`
	GenesisData       []byte             `json:"genesisData"`
}

type ControlKey struct {
	Address   StringShortID `json:"address"`
	PublicKey []byte        `json:"publicKey"`
}

type ControlSignature []byte

type BlockList struct {
	ListMetadata
	Blocks []*Block `json:"blocks"`
}

type SubnetList struct {
	ListMetadata
	Subnets []*Subnet `json:"subnets"`
}

type ValidatorList struct {
	ListMetadata
	Validators []*Validator `json:"validators"`
}

type ChainList struct {
	ListMetadata
	Chains []*Chain `json:"chains"`
}
