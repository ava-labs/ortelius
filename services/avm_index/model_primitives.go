// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import "github.com/ava-labs/gecko/ids"

// TransactionType represents a sub class of Transaction
type TransactionType string

// OutputType represents a sub class of Output
type OutputType uint32

// SearchResultType is the type for an object found from a search query.
type SearchResultType string

// stringID represents a 256bit hash encoded as a base58 string
type stringID string

// toStringID converts an ids.ID into a stringID
func toStringID(id ids.ID) stringID { return stringID(id.String()) }

// Equals returns true if and only if the two stringIDs represent the same ID
func (rid stringID) Equals(oRID stringID) bool { return string(rid) == string(oRID) }

// stringShortID represents a 160bit hash encoded as a base58 string
type stringShortID string

// toShortStringID converts an ids.ShortID into a stringShortID
func toShortStringID(id ids.ShortID) stringShortID {
	return stringShortID(id.String())
}

// Equals returns true if and only if the two stringShortIDs represent the same ID
func (rid stringShortID) Equals(oRID stringShortID) bool {
	return string(rid) == string(oRID)
}

// AssetTokenCounts maps asset IDs to an amount of tokens of that asset
type AssetTokenCounts map[stringID]uint64
