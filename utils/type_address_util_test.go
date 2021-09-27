// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestCommonAddressHexRepair(t *testing.T) {
	var addr *common.Address = nil
	if CommonAddressHexRepair(addr) != "" {
		t.Fatal("nil failed")
	}
	origStr := "D7059d104aA67748e78CC0c60F7255EB5593eC47"
	b, err := hex.DecodeString(origStr)
	if err != nil {
		t.Fatal("failed decode", err)
	}
	addr = &common.Address{}
	copy(addr[:], b)
	if origStr != strings.TrimPrefix(addr.Hex(), "0x") {
		t.Fatal("hex match failed", addr.Hex())
	}
	res := CommonAddressHexRepair(addr)
	if res != "0x"+strings.ToLower(origStr) {
		t.Fatal("repair failed", res)
	}
}
