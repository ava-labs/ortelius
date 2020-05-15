// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"github.com/ava-labs/gecko/ids"
)

type ChainInfo struct {
	ID        ids.ID `json:"chainID"`
	Alias     string `json:"chainAlias"`
	VM        string `json:"vm"`
	NetworkID uint32 `json:"networkID"`
}
