// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"encoding/json"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/formatting"
)

// bech32HRP is the human-readable part of bech32 addresses. It needs to be
// available to Address.MarshalJSON is there is no other way to give it this
// data
var bech32HRP = constants.GetHRP(constants.EverestID)

// SetBech32HRP sets the package-wide beck32HRP to use for Address marshaling
func SetBech32HRP(networkID uint32) {
	bech32HRP = constants.GetHRP(networkID)
}

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

type Address StringShortID

func (addr Address) MarshalJSON() ([]byte, error) {
	id, err := ids.ShortFromString(string(addr))
	if err != nil {
		return nil, err
	}

	bech32Addr, err := formatting.FormatBech32(bech32HRP, id.Bytes())
	if err != nil {
		return nil, err
	}

	return json.Marshal(bech32Addr)
}
