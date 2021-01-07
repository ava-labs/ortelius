package services

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
)

const TestDB = "mysql"
const TestDSN = "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"

func TestTransaction(t *testing.T) {
	p := New()
	ctx := context.Background()
	tm := time.Now().UTC()

	v := &Transaction{}
	v.ID = "id"
	v.ChainID = "cid1"
	v.TxType = "txtype"
	v.Memo = []byte("memo")
	v.CanonicalSerialization = []byte("cs")
	v.Txfee = 1
	v.Genesis = true
	v.CreatedAt = tm

	stream := health.NewStream()
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	err = p.InsertTransaction(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransaction(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputsRedeeming(t *testing.T) {
	p := New()
	ctx := context.Background()
	tm := time.Now().UTC()

	v := &OutputsRedeeming{}
	v.ID = "id1"
	v.RedeemedAt = tm
	v.RedeemingTransactionID = "rtxid"
	v.Amount = 100
	v.OutputIndex = 1
	v.InTX = "intx"
	v.AssetID = "aid1"
	v.ChainID = "cid1"
	v.CreatedAt = tm

	stream := health.NewStream()
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	err = p.InsertOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputsRedeeming(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputs(t *testing.T) {
	p := New()
	ctx := context.Background()
	tm := time.Now().UTC()

	v := &Outputs{}
	v.ID = "id1"
	v.ChainID = "cid1"
	v.TransactionID = "txid1"
	v.OutputIndex = 1
	v.AssetID = "aid1"
	v.OutputType = models.OutputTypesSECP2556K1Transfer
	v.Amount = 2
	v.Locktime = 3
	v.Threshold = 4
	v.GroupID = 5
	v.Payload = []byte("payload")
	v.StakeLocktime = 6
	v.Stake = true
	v.CreatedAt = tm

	stream := health.NewStream()
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	err = p.InsertOutputs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputs(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}
