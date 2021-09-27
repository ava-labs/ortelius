// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"strconv"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/formatting"
)

// Bech32HRP is the human-readable part of bech32 addresses. It needs to be
// available to Address.MarshalJSON is there is no other way to give it this
// data
var Bech32HRP = constants.GetHRP(constants.EverestID)

// SetBech32HRP sets the package-wide beck32HRP to use for Address marshaling.
func SetBech32HRP(networkID uint32) {
	Bech32HRP = constants.GetHRP(networkID)
}

// StringID represents a 256bit hash encoded as a base58 string.
type StringID string

// ToStringID converts an ids.ID into a StringID.
func ToStringID(id ids.ID) StringID { return StringID(id.String()) }

// Equals returns true if and only if the two stringIDs represent the same ID.
func (rid StringID) Equals(oRID StringID) bool { return string(rid) == string(oRID) }

// StringShortID represents a 160bit hash encoded as a base58 string.
type StringShortID string

// ToShortStringID converts an ids.ShortID into a StringShortID.
func ToShortStringID(id ids.ShortID) StringShortID {
	return StringShortID(id.String())
}

// Equals returns true if and only if the two stringShortIDs represent the same
// ID.
func (rid StringShortID) Equals(oRID StringShortID) bool {
	return string(rid) == string(oRID)
}

// Address represents an address; a short ID that is shown to users as bech32.
type Address StringShortID

// ToAddress converts an ids.ShortID into an Address.
func ToAddress(id ids.ShortID) Address {
	return Address(id.String())
}

// Equals returns true if and only if the two Addresses represent the same.
func (addr Address) Equals(oAddr2 Address) bool {
	return string(addr) == string(oAddr2)
}

// MarshalJSON encodes an Address to JSON by converting it to bech32.
func (addr Address) MarshalJSON() ([]byte, error) {
	id, err := ids.ShortFromString(string(addr))
	if err != nil {
		return nil, err
	}

	bech32Addr, err := formatting.FormatBech32(Bech32HRP, id.Bytes())
	if err != nil {
		return nil, err
	}

	return json.Marshal(bech32Addr)
}

func (addr Address) MarshalString() ([]byte, error) {
	id, err := ids.ShortFromString(string(addr))
	if err != nil {
		return nil, err
	}

	bech32Addr, err := formatting.FormatBech32(Bech32HRP, id.Bytes())
	if err != nil {
		return nil, err
	}

	return []byte(bech32Addr), nil
}

// AssetTokenCounts maps asset IDs to a TokenAmount for that asset.
type AssetTokenCounts map[StringID]TokenAmount

// TokenAmount represents some number of tokens as a string.
type TokenAmount string

// TokenAmountForUint64 returns an TokenAmount for the given uint64.
func TokenAmountForUint64(i uint64) TokenAmount {
	return TokenAmount(strconv.Itoa(int(i)))
}
