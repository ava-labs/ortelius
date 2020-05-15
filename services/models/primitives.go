// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"github.com/ava-labs/gecko/ids"
)

// StringID represents a 256bit hash encoded as a base58 string
type StringID string

// ToStringID converts an ids.ID into a StringID
func ToStringID(id ids.ID) StringID { return StringID(id.String()) }

// Equals returns true if and only if the two stringIDs represent the same ID
func (rid StringID) Equals(oRID StringID) bool { return string(rid) == string(oRID) }

// StringShortID represents a 160bit hash encoded as a base58 string
type StringShortID string

// ToShortStringID converts an ids.ShortID into a StringShortID
func ToShortStringID(id ids.ShortID) StringShortID {
	return StringShortID(id.String())
}

// Equals returns true if and only if the two stringShortIDs represent the same ID
func (rid StringShortID) Equals(oRID StringShortID) bool {
	return string(rid) == string(oRID)
}
