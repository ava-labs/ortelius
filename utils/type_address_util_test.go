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
	res := CommonAddressHexRepair(addr)
	if res != "0x"+strings.ToLower(origStr) {
		t.Fatal("repair failed", res)
	}
}
