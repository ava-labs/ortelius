// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

const (
	TxTypeAddDefaultSubnetValidator TxType = iota
	TxTypeAddNonDefaultSubnetValidator
	TxTypeAddDefaultSubnetDelegator
	TxTypeCreateChainTx
	TxTypeCreateSubnetTx
	TxTypeImportTx
	TxTypeExportTx
	TxTypeAdvanceTimeTx
)

type TxType uint
