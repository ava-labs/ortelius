package params

import (
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
)

func TestForValueChainID(t *testing.T) {
	res := ForValueChainID(nil, nil)
	if res != nil {
		t.Error("ForValueChainID failed")
	}
	tempChain, _ := ids.ToID(hashing.ComputeHash256([]byte("tid1")))
	res = ForValueChainID(&tempChain, nil)
	if len(res) != 1 || res[0] != tempChain.String() {
		t.Error("ForValueChainID failed")
	}
	res = ForValueChainID(&tempChain, []string{})
	if len(res) != 1 || res[0] != tempChain.String() {
		t.Error("ForValueChainID failed")
	}
	res = ForValueChainID(&tempChain, []string{tempChain.String()})
	if len(res) != 1 || res[0] != tempChain.String() {
		t.Error("ForValueChainID failed")
	}
	tempChain2, _ := ids.ToID(hashing.ComputeHash256([]byte("tid2")))
	if tempChain.String() == tempChain2.String() {
		t.Error("toId failed")
	}
	res = ForValueChainID(&tempChain, []string{tempChain2.String()})
	if len(res) != 2 || res[0] != tempChain.String() || res[1] != tempChain2.String() {
		t.Error("ForValueChainID failed")
	}
}
