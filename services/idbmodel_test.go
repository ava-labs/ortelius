package services

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
)

const TestDB = "mysql"
const TestDSN = "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"

func TestTransaction(t *testing.T) {
	p := New()
	ctx := context.Background()
	tm := time.Now().UTC()

	tran := &Transaction{}
	tran.ChainID = "cid1"
	tran.TxID = "tid1"
	tran.Memo = []byte("memo")
	tran.Txfee = 1

	stream := health.NewStream()
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatalf("db fail", err)
	}
	err = p.InsertTransaction(ctx, rawDBConn.NewSession(stream), tm, tran, true)
	if err != nil {
		t.Fatalf("insert transaction fail", err)
	}
	tranf, err := p.QueryTransaction(ctx, rawDBConn.NewSession(stream))
	if err != nil {
		t.Fatalf("query transaction fail", err)
	}
	if !reflect.DeepEqual(*tran, *tranf) {
		t.Fatalf("transaction compare fail")
	}
}
