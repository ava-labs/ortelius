// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

import (
	"time"

	"github.com/ava-labs/gecko/ids"

	"github.com/ava-labs/ortelius/services/params"
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
