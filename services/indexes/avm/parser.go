// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/vms/avm"
)

func parseTx(c codec.Manager, bytes []byte) (*avm.Tx, error) {
	tx := &avm.Tx{}
	ver, err := c.Unmarshal(bytes, tx)
	if err != nil {
		return nil, err
	}
	unsignedBytes, err := c.Marshal(ver, &tx.UnsignedTx)
	if err != nil {
		return nil, err
	}

	tx.Initialize(unsignedBytes, bytes)
	return tx, nil
}
