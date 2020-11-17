package params

import (
	"testing"

	"github.com/ava-labs/avalanchego/utils/hashing"

	"github.com/ava-labs/avalanchego/ids"
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
	res = ForValueChainID(&tempChain, []string{tempChain2.String()})
	if len(res) != 2 || res[0] != tempChain2.String() || res[1] != tempChain.String() {
		t.Error("ForValueChainID failed")
	}
}