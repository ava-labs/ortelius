// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
)

//
// The formatting provided here is a stand-in until Gecko implements a canonical
// JSON serialization for transactions.
//

type formattedTx struct {
	ID           ids.ID         `json:"id"`
	Dependencies []snowstorm.Tx `json:"dependencies"`
	InputIDs     []ids.ID       `json:"input_ids"`
	Bytes        []byte         `json:"bytes"`
}

func formatTx(tx snowstorm.Tx) *formattedTx {
	return &formattedTx{
		ID:           tx.ID(),
		Dependencies: tx.Dependencies(),
		InputIDs:     tx.InputIDs().List(),
		Bytes:        tx.Bytes(),
	}
}
