// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

import (
	"time"

	"github.com/ava-labs/ortelius/services/models"
)

const (
	VMName = "pvm"

	BlockTypeProposal BlockType = iota
	BlockTypeStandard
	BlockTypeAtomic
)

const (
	// Timed
	TransactionTypeAddDefaultSubnetValidator TransactionType = (iota * 2) + 11
	TransactionTypeAddNonDefaultSubnetValidator
	TransactionTypeAddDefaultSubnetDelegator

	// Decision
	TransactionTypeCreateChain
	TransactionTypeCreateSubnet

	// Atomic
	TransactionTypeImport
	TransactionTypeExport
)

type BlockType uint8
type TransactionType uint8

type Block struct {
	ID        models.StringID `json:"id"`
	ParentID  models.StringID `json:"parentID"`
	ChainID   models.StringID `json:"chainID"`
	Type      BlockType       `json:"type"`
	CreatedAt time.Time       `json:"createdAt"`
}

type BlockList struct {
	Blocks []*Block `json:"blocks"`
}

type Transaction struct{}
type TransactionList struct{}

type Subnet struct {
	ID          models.StringID `json:"id"`
	Threshold   uint64          `json:"threshold"`
	ControlKeys []ControlKey    `json:"controlKeys"`
	CreatedAt   time.Time       `json:"createdAt"`
}

type SubnetList struct {
	Subnets []*Subnet `json:"subnets"`
}

type Validator struct {
	TransactionID models.StringID `json:"transactionID"`

	NodeID models.StringShortID `json:"nodeID"`
	Weight string               `json:"weight"`

	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	Destination models.StringShortID `json:"destination"`
	Shares      uint32               `json:"shares"`

	SubnetID models.StringID `json:"subnetID"`
}

type ValidatorList struct {
	Validators []*Validator `json:"validators"`
}

type Chain struct {
	ID                models.StringID    `json:"id"`
	SubnetID          models.StringID    `json:"subnetID"`
	Name              string             `json:"name"`
	VMID              models.StringID    `json:"vmID" db:"vm_id"`
	ControlSignatures []ControlSignature `json:"controlSignatures"`
	FxIDs             []models.StringID  `json:"fxIDs"`
	GenesisData       []byte             `json:"genesisData"`
}

type ChainList struct {
	Chains []*Chain `json:"chains"`
}

type ControlKey struct {
	Address   models.StringShortID `json:"address"`
	PublicKey []byte               `json:"publicKey"`
}

type ControlSignature []byte
