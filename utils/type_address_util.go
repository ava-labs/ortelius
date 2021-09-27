// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"encoding/hex"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

func CommonAddressHexRepair(address *common.Address) string {
	if address == nil {
		return ""
	}
	str := strings.TrimPrefix(address.Hex(), "0x")
	// decode to all lower case
	hb, err := hex.DecodeString(str)
	if err == nil {
		str = hex.EncodeToString(hb)
	}
	if !strings.HasPrefix(str, "0x") {
		str = "0x" + str
	}
	return str
}
