// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/avm"
	avalancheGoAvax "github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
)

var (
	testXChainID = ids.ID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})
)

func TestIndexBootstrap(t *testing.T) {
	conns, writer, reader, closeFn := newTestIndex(t, testXChainID)
	defer closeFn()

	persist := db.NewPersist()
	err := writer.Bootstrap(newTestContext(), conns, persist)
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	txList, err := reader.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs: []string{testXChainID.String()},
	}, ids.Empty)
	if err != nil {
		t.Fatal("Failed to list transactions:", err.Error())
	}

	if txList.Count == nil || *txList.Count < 1 {
		if txList.Count == nil {
			t.Fatal("Incorrect number of transactions:", txList.Count)
		} else {
			t.Fatal("Incorrect number of transactions:", *txList.Count)
		}
	}

	if !txList.Transactions[0].Genesis {
		t.Fatal("Transaction is not genesis")
	}
	if txList.Transactions[0].Txfee != 0 {
		t.Fatal("Transaction fee is not 0")
	}

	// inject a txfee for testing
	session, _ := conns.DB().NewSession("test_tx", cfg.RequestTimeout)

	transaction := &db.Transactions{
		ID: string(txList.Transactions[0].ID),
	}
	transaction, _ = persist.QueryTransactions(context.Background(), session, transaction)
	transaction.Txfee = 101
	_ = persist.InsertTransactions(context.Background(), session, transaction, true)

	txList, _ = reader.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs: []string{string(txList.Transactions[0].ChainID)},
	}, ids.Empty)

	if txList.Transactions[0].Txfee != 101 {
		t.Fatal("Transaction fee is not 101")
	}

	addr, _ := ids.ToShortID([]byte("addr"))

	sess, _ := conns.DB().NewSession("address_chain", cfg.RequestTimeout)

	addressChain := &db.AddressChain{
		Address:   addr.String(),
		ChainID:   "ch1",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	_ = persist.InsertAddressChain(context.Background(), sess, addressChain, false)

	addressChains, err := reader.AddressChains(context.Background(), &params.AddressChainsParams{
		Addresses: []ids.ShortID{addr},
	})
	if err != nil {
		t.Fatal("Failed to get address chains:", err.Error())
	}
	if len(addressChains.AddressChains) != 1 {
		t.Fatal("Incorrect number of address chains:", len(addressChains.AddressChains))
	}
	addrf, _ := models.Address(addr.String()).MarshalString()
	if addressChains.AddressChains[string(addrf)][0] != "ch1" {
		t.Fatal("Incorrect chain id")
	}

	// invoke the address and asset logic to test the db.
	txList, err = reader.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs:  []string{testXChainID.String()},
		Addresses: []ids.ShortID{ids.ShortEmpty},
	}, ids.Empty)

	if err != nil {
		t.Fatal("Failed to list transactions:", err.Error())
	}

	if txList.Count == nil || *txList.Count < 1 {
		if txList.Count == nil {
			t.Fatal("Incorrect number of transactions:", txList.Count)
		} else {
			t.Fatal("Incorrect number of transactions:", *txList.Count)
		}
	}
}

func newTestIndex(t *testing.T, chainID ids.ID) (*utils.Connections, *Writer, *avax.Reader, func()) {
	networkID := uint32(5)

	logConf, err := logging.DefaultConfig()
	if err != nil {
		t.Fatal("Failed to create logging config:", err.Error())
	}

	conf := cfg.Services{
		Logging: logConf,
		DB: &cfg.DB{
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true",
		},
	}

	sc := &servicesctrl.Control{Log: logging.NoLog{}, Services: conf}
	conns, err := sc.Database()
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	// Create index
	writer, err := NewWriter(networkID, chainID.String())
	if err != nil {
		t.Fatal("Failed to create writer:", err.Error())
	}

	cmap := make(map[string]services.Consumer)
	reader, _ := avax.NewReader(networkID, conns, cmap, nil, sc)
	return conns, writer, reader, func() {
		_ = conns.Close()
	}
}

func newTestContext() context.Context {
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	time.AfterFunc(5*time.Second, cancelFn)
	return ctx
}

func TestInsertTxInternal(t *testing.T) {
	conns, writer, _, closeFn := newTestIndex(t, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := &avm.Tx{}
	baseTx := &avm.BaseTx{}

	transferableOut := &avalancheGoAvax.TransferableOutput{}
	transferableOut.Out = &secp256k1fx.TransferOutput{
		OutputOwners: secp256k1fx.OutputOwners{Addrs: []ids.ShortID{ids.ShortEmpty}},
	}
	baseTx.Outs = []*avalancheGoAvax.TransferableOutput{transferableOut}

	transferableIn := &avalancheGoAvax.TransferableInput{}
	transferableIn.In = &secp256k1fx.TransferInput{}
	baseTx.Ins = []*avalancheGoAvax.TransferableInput{transferableIn}

	f := crypto.FactorySECP256K1R{}
	pk, _ := f.NewPrivateKey()
	sb, _ := pk.Sign(baseTx.UnsignedBytes())
	cred := &secp256k1fx.Credential{}
	cred.Sigs = make([][crypto.SECP256K1RSigLen]byte, 0, 1)
	sig := [crypto.SECP256K1RSigLen]byte{}
	copy(sig[:], sb)
	cred.Sigs = append(cred.Sigs, sig)
	tx.Creds = []*avm.FxCredential{
		{Verifiable: cred},
	}

	tx.UnsignedTx = baseTx

	persist := db.NewPersistMock()
	session, _ := conns.DB().NewSession("test_tx", cfg.RequestTimeout)
	cCtx := services.NewConsumerContext(ctx, session, time.Now().Unix(), 0, persist)
	err := writer.insertTxInternal(cCtx, tx, tx.Bytes())
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.Transactions) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Outputs) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputsRedeeming) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Addresses) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.AddressChain) != 2 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputsRedeeming) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputAddressAccumulateIn) != 2 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputAddressAccumulateOut) != 2 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputTxsAccumulate) != 1 {
		t.Fatal("insert failed")
	}
}

func TestInsertTxInternalCreateAsset(t *testing.T) {
	conns, writer, _, closeFn := newTestIndex(t, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := &avm.Tx{}
	baseTx := &avm.CreateAssetTx{}

	transferableOut := &avalancheGoAvax.TransferableOutput{}
	transferableOut.Out = &secp256k1fx.TransferOutput{}
	baseTx.Outs = []*avalancheGoAvax.TransferableOutput{transferableOut}

	transferableIn := &avalancheGoAvax.TransferableInput{}
	transferableIn.In = &secp256k1fx.TransferInput{}
	baseTx.Ins = []*avalancheGoAvax.TransferableInput{transferableIn}

	tx.UnsignedTx = baseTx

	persist := db.NewPersistMock()
	session, _ := conns.DB().NewSession("test_tx", cfg.RequestTimeout)
	cCtx := services.NewConsumerContext(ctx, session, time.Now().Unix(), 0, persist)
	err := writer.insertTxInternal(cCtx, tx, tx.Bytes())
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.Transactions) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Outputs) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputsRedeeming) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Addresses) != 0 {
		t.Fatal("insert failed")
	}
	if len(persist.AddressChain) != 0 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputsRedeeming) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Assets) != 1 {
		t.Fatal("insert failed")
	}
}

func TestTransactionNext(t *testing.T) {
	conns, _, reader, closeFn := newTestIndex(t, testXChainID)
	defer closeFn()
	ctx := context.Background()

	session, _ := conns.DB().NewSession("test_tx", cfg.RequestTimeout)

	_, _ = session.DeleteFrom("avm_transactions").ExecContext(ctx)

	persist := db.NewPersist()

	tnow0 := time.Now().Truncate(time.Second)

	tnow1 := tnow0.Add(time.Second)
	tx1 := &db.Transactions{
		ID:        "1",
		ChainID:   "1",
		CreatedAt: tnow1,
	}
	_ = persist.InsertTransactions(ctx, session, tx1, false)

	tnow2 := tnow1.Add(time.Second)
	tx2 := &db.Transactions{
		ID:        "2",
		ChainID:   "1",
		CreatedAt: tnow2,
	}
	_ = persist.InsertTransactions(ctx, session, tx2, false)

	tnow3 := tnow2.Add(time.Second)
	tx3 := &db.Transactions{
		ID:        "3",
		ChainID:   "1",
		CreatedAt: tnow3,
	}
	_ = persist.InsertTransactions(ctx, session, tx3, false)

	tnow4 := tnow3.Add(time.Second)
	tx4 := &db.Transactions{
		ID:        "4",
		ChainID:   "1",
		CreatedAt: tnow4,
	}
	_ = persist.InsertTransactions(ctx, session, tx4, false)

	tp := params.ListTransactionsParams{}
	_ = tp.ForValues(0, url.Values{})
	tp.ListParams.Limit = 2

	tp.Sort = params.TransactionSortTimestampAsc
	tl, _ := reader.ListTransactions(ctx, &tp, ids.ID{})
	if len(tl.Transactions) != 2 {
		t.Fatal("invalid transactions")
	}
	if !(tl.Transactions[0].ID == "1" && tl.Transactions[1].ID == "2") {
		t.Fatal("invalid transactions")
	}

	n, _ := url.ParseQuery(*tl.Next)

	if n[params.KeyStartTime][0] != fmt.Sprintf("%d", tnow3.Unix()) {
		t.Fatal("invalid next starttime")
	}
	if n[params.KeySortBy][0] != params.TransactionSortTimestampAscStr {
		t.Fatal("invalid sort")
	}

	_ = tp.ForValues(0, n)
	tp.ListParams.Limit = 1
	tl, _ = reader.ListTransactions(ctx, &tp, ids.ID{})
	if len(tl.Transactions) != 1 {
		t.Fatal("invalid transactions")
	}
	if tl.Transactions[0].ID != "3" {
		t.Fatal("invalid transactions")
	}

	tp = params.ListTransactionsParams{}
	_ = tp.ForValues(0, url.Values{})
	tp.ListParams.Limit = 2
	tp.Sort = params.TransactionSortTimestampDesc
	tl, _ = reader.ListTransactions(ctx, &tp, ids.ID{})
	if len(tl.Transactions) != 2 {
		t.Fatal("invalid transactions")
	}
	if !(tl.Transactions[0].ID == "4" && tl.Transactions[1].ID == "3") {
		t.Fatal("invalid transactions")
	}

	n, _ = url.ParseQuery(*tl.Next)

	if n[params.KeyEndTime][0] != fmt.Sprintf("%d", tnow3.Unix()) {
		t.Fatal("invalid next endtime")
	}
	if n[params.KeySortBy][0] != params.TransactionSortTimestampDescStr {
		t.Fatal("invalid sort")
	}

	_ = tp.ForValues(0, n)
	tp.ListParams.Limit = 1
	tl, _ = reader.ListTransactions(ctx, &tp, ids.ID{})
	if len(tl.Transactions) != 1 {
		t.Fatal("invalid transactions")
	}
	if tl.Transactions[0].ID != "2" {
		t.Fatal("invalid transactions")
	}
}
