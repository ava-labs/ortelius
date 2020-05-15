// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"github.com/ava-labs/ortelius/services/models"
)

// TransactionType represents a sub class of Transaction
type TransactionType string

// OutputType represents a sub class of Output
type OutputType uint32

// SearchResultType is the type for an object found from a search query.
type SearchResultType string

// AssetTokenCounts maps asset IDs to a TokenAmount for that asset
type AssetTokenCounts map[models.StringID]TokenAmount

type TokenAmount string
