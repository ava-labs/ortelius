// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ava-labs/ortelius/models"
	"github.com/gocraft/dbr/v2"
)

const TestDB = "mysql"
const TestDSN = "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"

func TestTransaction(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Transactions{}
	v.ID = "id"
	v.ChainID = "cid1"
	v.Type = "txtype"
	v.Memo = []byte("memo")
	v.CanonicalSerialization = []byte("cs")
	v.Txfee = 1
	v.Genesis = true
	v.CreatedAt = tm
	v.NetworkID = 1

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()

	err = p.InsertTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactions(ctx, rawDBConn.NewSession(stream), v)
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
	v.NetworkID = 2
	err = p.InsertTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}

	if fv.NetworkID != 2 {
		t.Fatal("compare fail")
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

	stream := &dbr.NullEventReceiver{}

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
	v.Stakeableout = true
	v.Genesisutxo = true
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

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
	v.Stakeableout = false
	v.Genesisutxo = false

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

	stream := &dbr.NullEventReceiver{}

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
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	basebin := [33]byte{}
	for cnt := 0; cnt < len(basebin); cnt++ {
		basebin[cnt] = byte(cnt + 1)
	}

	v := &Addresses{}
	v.Address = "id1"
	v.PublicKey = make([]byte, len(basebin))
	copy(v.PublicKey, basebin[:])
	v.CreatedAt = tm
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

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
	v.UpdatedAt = tmu.Add(1 * time.Minute)

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
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	v := &AddressChain{}
	v.Address = "id1"
	v.ChainID = "ch1"
	v.CreatedAt = tm
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

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
	v.UpdatedAt = tmu.Add(1 * time.Minute)

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
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddresses).Exec()

	v := &OutputAddresses{}
	v.OutputID = "oid1"
	v.Address = "id1"
	v.CreatedAt = tm
	v.UpdatedAt = tmu

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
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.UpdatedAt != tmu.Add(1*time.Minute) {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingSignature = []byte("rd2")
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(2 * time.Minute)

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
	if fv.UpdatedAt != tmu.Add(2*time.Minute) {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingSignature = []byte("rd3")
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(3 * time.Minute)

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

	stream := &dbr.NullEventReceiver{}

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

func TestCvmBlocks(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmBlocks{}
	v.Block = "1"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmBlocks).Exec()

	err = p.InsertCvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
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

	stream := &dbr.NullEventReceiver{}

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
	txtime := time.Now().UTC().Truncate(1 * time.Second).Add(-1 * time.Hour)

	v := &CvmTransactions{}
	v.ID = "id1"
	v.TransactionID = "trid1"
	v.Type = models.CChainIn
	v.BlockchainID = "bid1"
	v.Block = "1"
	v.CreatedAt = tm
	v.Serialization = []byte("test123")
	v.TxTime = txtime
	v.Nonce = 10
	v.Hash = "h1"
	v.ParentHash = "ph1"

	stream := &dbr.NullEventReceiver{}

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

	txtime2 := time.Now().UTC().Truncate(1 * time.Second).Add(-1 * time.Hour)

	v.Type = models.CchainOut
	v.TransactionID = "trid2"
	v.BlockchainID = "bid2"
	v.Block = "2"
	v.CreatedAt = tm
	v.Serialization = []byte("test456")
	v.TxTime = txtime2
	v.Nonce = 11
	v.Hash = "h2"
	v.ParentHash = "ph2"

	err = p.InsertCvmTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(fv.Serialization) != "test456" {
		t.Fatal("compare fail")
	}
	if !fv.TxTime.Equal(txtime2) {
		t.Fatal("compare fail")
	}
	if fv.TransactionID != "trid2" {
		t.Fatal("compare fail")
	}
	if fv.Block != "2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmTransactionsTxdata(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmTransactionsTxdata{}
	v.Hash = "h1"
	v.Block = "1"
	v.Idx = 1
	v.CreatedAt = tm
	v.Serialization = []byte("test123")

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmTransactionsTxdata).Exec()

	err = p.InsertCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Idx = 7
	v.CreatedAt = tm
	v.Serialization = []byte("test456")

	err = p.InsertCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(fv.Serialization) != "test456" {
		t.Fatal("compare fail")
	}
	if fv.Hash != "h1" {
		t.Fatal("compare fail")
	}
	if fv.Idx != 7 {
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

	stream := &dbr.NullEventReceiver{}

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

	stream := &dbr.NullEventReceiver{}

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

	stream := &dbr.NullEventReceiver{}

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

func TestTransactionsBlock(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsBlock{}
	v.ID = "id1"
	v.TxBlockID = "txb1"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

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

func TestAddressBech32(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	v := &AddressBech32{}
	v.Address = "adr1"
	v.Bech32Address = "badr1"
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAddressBech32).Exec()

	err = p.InsertAddressBech32(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAddressBech32(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Bech32Address = "badr2"
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertAddressBech32(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAddressBech32(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.Bech32Address != "badr2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputAddressAccumulateOut(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &OutputAddressAccumulate{}
	v.OutputID = "out1"
	v.Address = "adr1"
	v.TransactionID = "txid1"
	v.OutputIndex = 1
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("db fail", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddressAccumulateOut).Exec()

	err = p.InsertOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputIndex = 3
	v.TransactionID = "tr3"

	err = p.InsertOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.OutputIndex != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputAddressAccumulateIn(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &OutputAddressAccumulate{}
	v.OutputID = "out1"
	v.Address = "adr1"
	v.TransactionID = "txid1"
	v.OutputIndex = 1
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("compute id failed", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddressAccumulateIn).Exec()

	err = p.InsertOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputIndex = 3
	v.TransactionID = "tr3"

	err = p.InsertOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.OutputIndex != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputTxsAccumulate(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &OutputTxsAccumulate{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TransactionID = "tr1"
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("compute id failed", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputTxsAccumulate).Exec()

	err = p.InsertOutputTxsAccumulate(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputTxsAccumulate(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAccumulateBalancesReceived(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &AccumulateBalancesAmount{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TotalAmount = "0"
	v.UtxoCount = "0"
	v.UpdatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("compute id failed", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAccumulateBalancesReceived).Exec()

	err = p.InsertAccumulateBalancesReceived(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAccumulateBalancesReceived(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAccumulateBalancesSent(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &AccumulateBalancesAmount{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TotalAmount = "0"
	v.UtxoCount = "0"
	v.UpdatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("compute id failed", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAccumulateBalancesSent).Exec()

	err = p.InsertAccumulateBalancesSent(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAccumulateBalancesSent(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAccumulateBalancesTransactions(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &AccumulateBalancesTransactions{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TransactionCount = "0"
	v.UpdatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("compute id failed", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAccumulateBalancesTransactions).Exec()

	err = p.InsertAccumulateBalancesTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAccumulateBalancesTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsRewardsOwnersOutputs(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &TransactionsRewardsOwnersOutputs{}
	v.ID = "txid1"
	v.TransactionID = "addr1"
	v.OutputIndex = 1
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsRewardsOwnersOutputs).Exec()

	err = p.InsertTransactionsRewardsOwnersOutputs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsRewardsOwnersOutputs(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsRewardsOwnersAddress(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsRewardsOwnersAddress{}
	v.ID = "txid1"
	v.Address = "addr1"
	v.OutputIndex = 1
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsRewardsOwnersAddress).Exec()

	err = p.InsertTransactionsRewardsOwnersAddress(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsRewardsOwnersAddress(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputIndex = 4
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertTransactionsRewardsOwnersAddress(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsRewardsOwnersAddress(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.OutputIndex != 4 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsRewardsOwners(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsRewardsOwners{}
	v.ID = "txid1"
	v.ChainID = "cid1"
	v.Locktime = 3
	v.Threshold = 4
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsRewardsOwners).Exec()

	err = p.InsertTransactionsRewardsOwners(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsRewardsOwners(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.Locktime = 4
	v.Threshold = 5

	err = p.InsertTransactionsRewardsOwners(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsRewardsOwners(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Threshold != 5 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTxPool(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &TxPool{}
	v.NetworkID = 1
	v.ChainID = "ch1"
	v.Serialization = []byte("hello")
	v.Topic = "topic1"
	v.MsgKey = "key1"
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err := v.ComputeID()
	if err != nil {
		t.Fatal("compute id failed", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTxPool).Exec()

	err = p.InsertTxPool(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTxPool(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestKeyValueStore(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &KeyValueStore{}
	v.K = "k"
	v.V = "v"

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableKeyValueStore).Exec()

	err = p.InsertKeyValueStore(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryKeyValueStore(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmTransactionsTxdataTrace(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmTransactionsTxdataTrace{}
	v.Hash = "h1"
	v.Idx = 1
	v.ToAddr = "to1"
	v.FromAddr = "fr1"
	v.CallType = "ct1"
	v.Type = "t1"
	v.Serialization = []byte("bits1")
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmTransactionsTxdataTrace).Exec()

	err = p.InsertCvmTransactionsTxdataTrace(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmTransactionsTxdataTrace(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ToAddr = "to2"
	v.FromAddr = "fr2"
	v.Serialization = []byte("bits2")

	err = p.InsertCvmTransactionsTxdataTrace(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmTransactionsTxdataTrace(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.ToAddr != "to2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestNodeIndex(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &NodeIndex{}
	v.Instance = "def"
	v.Topic = "top"
	v.Idx = 1

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableNodeIndex).Exec()

	err = p.InsertNodeIndex(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Idx = 2

	err = p.InsertNodeIndex(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Idx != 2 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Idx = 3
	err = p.UpdateNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Idx != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmLogs(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmLogs{}
	v.BlockHash = "bh1"
	v.TxHash = "txh1"
	v.LogIndex = 1
	v.FirstTopic = "ft1"
	v.Block = "123"
	v.Removed = false
	v.CreatedAt = tm
	v.Serialization = []byte("bits1")

	err := v.ComputeID()
	if err != nil {
		t.Fatal("db fail", err)
	}

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmLogs).Exec()

	err = p.InsertCvmLogs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmLogs(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.FirstTopic = "ft3"
	v.Block = "124"
	v.Removed = false
	v.Serialization = []byte("bits2")

	err = p.InsertCvmLogs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmLogs(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Block != "124" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestPvmProposer(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &PvmProposer{}
	v.ID = "id"
	v.ParentID = "pid"
	v.BlkID = "blk"
	v.ProposerBlkID = "oblk"
	v.PChainHeight = 1
	v.Proposer = "proposer"
	v.TimeStamp = tm
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TablePvmProposer).Exec()

	err = p.InsertPvmProposer(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryPvmProposer(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ParentID = "pid2"
	v.BlkID = "blk2"
	v.ProposerBlkID = "oblk2"
	v.PChainHeight = 2
	v.Proposer = "proposer2"
	v.TimeStamp = tm
	v.CreatedAt = tm

	err = p.InsertPvmProposer(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryPvmProposer(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.ParentID != "pid2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}
