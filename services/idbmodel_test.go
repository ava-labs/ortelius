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
	fv, err := p.QueryTransaction(ctx, rawDBConn.NewSession(stream), v)
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
	fv, err = p.QueryTransaction(ctx, rawDBConn.NewSession(stream), v)
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
	fv, err := p.QueryOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v)
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
	fv, err = p.QueryOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v)
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
	v.Frozen = true
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
	fv, err := p.QueryOutputs(ctx, rawDBConn.NewSession(stream), v)
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
	v.Frozen = false

	err = p.InsertOutputs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputs(ctx, rawDBConn.NewSession(stream), v)
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

func TestAssets(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Assets{}
	v.ID = "id1"
	v.ChainID = "cid1"
	v.Name = "name1"
	v.Symbol = "symbol1"
	v.Denomination = 0x1
	v.Alias = "alias1"
	v.CurrentSupply = 1
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAssets).Exec()

	err = p.InsertAssets(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAssets(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.Name = "name2"
	v.Symbol = "symbol2"
	v.Denomination = 0x2
	v.Alias = "alias2"
	v.CurrentSupply = 2
	v.CreatedAt = tm

	err = p.InsertAssets(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAssets(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Name != "name2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAddresses(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	basebin := [33]byte{}
	for cnt := 0; cnt < len(basebin); cnt++ {
		basebin[cnt] = byte(cnt + 1)
	}

	v := &Addresses{}
	v.Address = "id1"
	v.PublicKey = make([]byte, len(basebin))
	copy(v.PublicKey, basebin[:])
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAddresses).Exec()

	err = p.InsertAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	basebin[0] = 0xF
	basebin[5] = 0xE
	copy(v.PublicKey, basebin[:])
	v.CreatedAt = tm

	err = p.InsertAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.PublicKey[0] != 0xF {
		t.Fatal("compare fail")
	}
	if fv.PublicKey[5] != 0xE {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAddressChain(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &AddressChain{}
	v.Address = "id1"
	v.ChainID = "ch1"
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAddressChain).Exec()

	err = p.InsertAddressChain(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAddressChain(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "ch2"
	v.CreatedAt = tm

	err = p.InsertAddressChain(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAddressChain(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.ChainID != "ch2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputAddresses(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddresses).Exec()

	v := &OutputAddresses{}
	v.OutputID = "oid1"
	v.Address = "id1"
	v.CreatedAt = tm

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.RedeemingSignature != nil {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputID = "oid1"
	v.Address = "id1"
	v.RedeemingSignature = []byte("rd1")
	v.CreatedAt = tm

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingSignature = []byte("rd2")
	v.CreatedAt = tm

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(v.RedeemingSignature) != "rd2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingSignature = []byte("rd3")
	v.CreatedAt = tm

	err = p.UpdateOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("update fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(v.RedeemingSignature) != "rd3" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsEpoch(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsEpoch{}
	v.ID = "id1"
	v.Epoch = 10
	v.VertexID = "vid1"
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsEpochs).Exec()

	err = p.InsertTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Epoch = 11
	v.VertexID = "vid2"
	v.CreatedAt = tm

	err = p.InsertTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.VertexID != "vid2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmAddresses(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmAddresses{}
	v.ID = "id1"
	v.Type = models.CChainIn
	v.Idx = 1
	v.TransactionID = "tid1"
	v.Address = "addr1"
	v.AssetID = "assid1"
	v.Amount = 2
	v.Nonce = 3
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmAddresses).Exec()

	err = p.InsertCvmAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Type = models.CchainOut
	v.Idx = 2
	v.TransactionID = "tid2"
	v.Address = "addr2"
	v.AssetID = "assid2"
	v.Amount = 3
	v.Nonce = 4
	v.CreatedAt = tm

	err = p.InsertCvmAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Idx != 2 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmTransactions(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmTransactions{}
	v.ID = "id1"
	v.Type = models.CChainIn
	v.BlockchainID = "bid1"
	v.Block = "1"
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmTransactions).Exec()

	err = p.InsertCvmTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Type = models.CchainOut
	v.BlockchainID = "bid2"
	v.Block = "2"
	v.CreatedAt = tm

	err = p.InsertCvmTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Block != "2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestPvmBlocks(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &PvmBlocks{}
	v.ID = "id1"
	v.ChainID = "cid1"
	v.Type = models.BlockTypeAbort
	v.ParentID = "pid1"
	v.Serialization = []byte("ser1")
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TablePvmBlocks).Exec()

	err = p.InsertPvmBlocks(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryPvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.Type = models.BlockTypeCommit
	v.ParentID = "pid2"
	v.Serialization = []byte("ser2")
	v.CreatedAt = tm

	err = p.InsertPvmBlocks(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryPvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(fv.Serialization) != "ser2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestRewards(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Rewards{}
	v.ID = "id1"
	v.BlockID = "bid1"
	v.Txid = "txid1"
	v.Shouldprefercommit = true
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableRewards).Exec()

	err = p.InsertRewards(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryRewards(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.BlockID = "bid2"
	v.Txid = "txid2"
	v.Shouldprefercommit = false
	v.CreatedAt = tm

	err = p.InsertRewards(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryRewards(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.Txid != "txid2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsValidator(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsValidator{}
	v.ID = "id1"
	v.NodeID = "nid1"
	v.Start = 1
	v.End = 2
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsValidator).Exec()

	err = p.InsertTransactionsValidator(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsValidator(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.NodeID = "nid2"
	v.Start = 2
	v.End = 3
	v.CreatedAt = tm

	err = p.InsertTransactionsValidator(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsValidator(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.NodeID != "nid2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionssBlock(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsBlock{}
	v.ID = "id1"
	v.TxBlockID = "txb1"
	v.CreatedAt = tm

	stream := health.NewStream()

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsBlock).Exec()

	err = p.InsertTransactionsBlock(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsBlock(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.TxBlockID = "txb2"
	v.CreatedAt = tm

	err = p.InsertTransactionsBlock(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsBlock(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.TxBlockID != "txb2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}
