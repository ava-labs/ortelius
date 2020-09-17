// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"time"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/ortelius/services/indexes/params"
)

type BlockSort string

type SearchParams struct {
	params.ListParams
}

type ListBlocksParams struct {
	params.ListParams

	ID        *ids.ID
	Types     []BlockType
	StartTime time.Time
	EndTime   time.Time
	Sort      BlockSort
}

type ListValidatorsParams struct {
	params.ListParams

	ID           *ids.ID
	Subnets      []ids.ID
	Destinations []ids.ShortID
	StartTime    time.Time
	EndTime      time.Time
}

type ListChainsParams struct {
	params.ListParams

	ID      *ids.ID
	Subnets []ids.ID
	VMID    *ids.ID
}

type ListSubnetsParams struct {
	params.ListParams

	ID *ids.ID
}
