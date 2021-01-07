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
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Transaction{}
	v.ID = "id"
	v.ChainID = "cid1"
	v.Type = "txtype"
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
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()

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

	v.ChainID = "cid2"
	v.Type = "txtype1"
	v.Memo = []byte("memo1")
	v.CanonicalSerialization = []byte("cs1")
	v.Txfee = 2
	v.Genesis = false
	err = p.InsertTransaction(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransaction(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatal("query fail", err)
	}

	if fv.Txfee != 2 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputsRedeeming(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &OutputsRedeeming{}
	v.ID = "id1"
	v.RedeemedAt = tm
	v.RedeemingTransactionID = "rtxid"
	v.Amount = 100
	v.OutputIndex = 1
	v.Intx = "intx1"
	v.AssetID = "aid1"
	v.ChainID = "cid1"
	v.CreatedAt = tm

	stream := health.NewStream()
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputsRedeeming).Exec()

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

	v.RedeemingTransactionID = "rtxid1"
	v.Amount = 102
	v.OutputIndex = 3
	v.Intx = "intx2"
	v.AssetID = "aid2"
	v.ChainID = "cid2"

	err = p.InsertOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputsRedeeming(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Intx != "intx2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputs(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

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
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputs).Exec()

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

	v.ChainID = "cid2"
	v.TransactionID = "txid2"
	v.OutputIndex = 2
	v.AssetID = "aid2"
	v.OutputType = models.OutputTypesSECP2556K1Mint
	v.Amount = 3
	v.Locktime = 4
	v.Threshold = 5
	v.GroupID = 6
	v.Payload = []byte("payload2")
	v.StakeLocktime = 7
	v.Stake = false

	err = p.InsertOutputs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputs(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Amount != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}
