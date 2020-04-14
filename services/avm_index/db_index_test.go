// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/gocraft/dbr"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

type message struct {
	id        ids.ID
	chainID   ids.ID
	body      []byte
	timestamp uint64
}

func (m *message) ID() ids.ID        { return m.id }
func (m *message) ChainID() ids.ID   { return m.chainID }
func (m *message) Body() []byte      { return m.body }
func (m *message) Timestamp() uint64 { return m.timestamp }

func newTestIndex(t *testing.T) (*Index, func()) {
	// Get default config
	conf, err := cfg.NewAPIConfig("")
	if err != nil {
		t.Fatal("Failed to get config:", err.Error())
	}

	// Configure test db
	conf.DB.TXDB = true
	conf.DB.DSN = "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"

	// Configure test redis
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("Failed to create miniredis server:", err.Error())
	}
	conf.Redis.Addr = s.Addr()

	// Create index
	idx, err := New(conf.ServiceConfig, 12345, testXChainID)
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	return idx, func() {
		s.Close()
		idx.db.db.Close()
	}
}

func TestIndexBootstrap(t *testing.T) {
	idx, closeFn := newTestIndex(t)
	defer closeFn()

	err := idx.Bootstrap()
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}
	defer idx.db.db.Close()

	var (
		txID          = []byte{102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237}
		createAssetTx = []byte{0, 3, 65, 86, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 65, 86, 65, 0, 3, 65, 86, 65, 9, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7, 0, 159, 223, 66, 246, 228, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156}
	)

	db := idx.db.newDBSession("test_index_bootstrap")
	assertAllTransactionsCorrect(t, db, []transaction{{
		ID:                     txID,
		ChainID:                testXChainID.Bytes(),
		CanonicalSerialization: createAssetTx,

		Amount:      45000000000000000,
		InputCount:  0,
		OutputCount: 1,
		CreatedAt:   time.Unix(1572566400, 0),
	}})
}

func TestIndexVectors(t *testing.T) {
	idx, closeFn := newTestIndex(t)
	defer closeFn()

	db := idx.db.db.NewSession(nil)

	// Start with nothing
	assertAllTransactionsCorrect(t, db, nil)
	assertAllOutputsCorrect(t, db, nil)

	// Add each test vector tx
	acc := services.FanOutService{idx}
	for i, v := range createTestVectors() {
		err := acc.Add(&message{
			id:        ids.NewID(hashing.ComputeHash256Array(v.serializedTx)),
			chainID:   testXChainID,
			body:      v.serializedTx,
			timestamp: uint64(i + 1),
		})
		if err != nil {
			t.Fatal("Failed to add tx to index:", err.Error())
		}

		assertAllTransactionsCorrect(t, db, v.expecteds.txs)
		assertAllOutputsCorrect(t, db, v.expecteds.outs)
		assertAllOutputAddressesCorrect(t, db, v.expecteds.outAddrs)
	}
}

func assertAllTransactionsCorrect(t *testing.T, db dbr.SessionRunner, expecteds []transaction) {
	txs := []transaction{}
	if _, err := db.
		Select("*").
		From("avm_transactions").
		OrderAsc("internal_id").
		Load(&txs); err != nil {
		t.Fatal("Failed to get transactions:", err.Error())
	}
	assertCorrectTransactions(t, expecteds, txs)
}

func assertCorrectTransactions(t *testing.T, expecteds, actuals []transaction) {
	if len(actuals) != len(expecteds) {
		t.Fatal("Wrong transactions count:", len(actuals))
	}

	for i, actual := range actuals {
		assertCorrectTransaction(t, expecteds[i], actual)
	}
}

func assertCorrectTransaction(t *testing.T, expected, actual transaction) {
	if !actual.ID.Equals(expected.ID) {
		t.Fatal("Wrong id:", actual.ID)
	}

	if !actual.ChainID.Equals(testXChainID.Bytes()) {
		t.Fatal("Wrong chain id:", actual.ChainID)
	}

	if string(actual.CanonicalSerialization) != string(expected.CanonicalSerialization) {
		t.Fatal("Wrong canonical serialization:", actual.CanonicalSerialization)
	}

	if actual.InputCount != expected.InputCount {
		t.Fatal("Wrong input count:", actual.InputCount)
	}

	if actual.OutputCount != expected.OutputCount {
		t.Fatal("Wrong output count:", actual.OutputCount)
	}

	if actual.Amount != expected.Amount {
		t.Fatal("Wrong amount:", actual.Amount)
	}

	if !actual.CreatedAt.Equal(expected.CreatedAt) {
		t.Fatal("Wrong ingested at:", actual.CreatedAt)
	}
}

func assertAllOutputsCorrect(t *testing.T, db dbr.SessionRunner, expecteds []output) {
	outputs := []output{}
	if _, err := db.
		Select("*").
		From("avm_outputs").
		OrderAsc("internal_id").
		Load(&outputs); err != nil {
		t.Fatal("Failed to get outputs:", err.Error())
	}
	assertCorrectOutputs(t, expecteds, outputs)
}

func assertCorrectOutputs(t *testing.T, expecteds, actuals []output) {
	if len(actuals) != len(expecteds) {
		t.Fatal("Wrong output count:", len(actuals))
	}

	for i, actual := range actuals {
		assertCorrectOutput(t, expecteds[i], actual)
	}
}

func assertCorrectOutput(t *testing.T, expected, actual output) {
	if !actual.TransactionID.Equals(expected.TransactionID) {
		t.Fatal("Wrong output transaction id:", actual.TransactionID)
	}

	if actual.OutputIndex != expected.OutputIndex {
		t.Fatal("Wrong output index:", actual.OutputIndex)
	}

	if !actual.AssetID.Equals(testAVAAssetID.Bytes()) {
		t.Fatal("Wrong asset id:", actual.AssetID)
	}

	if actual.OutputType != expected.OutputType {
		t.Fatal("Wrong output type:", actual.OutputType)
	}

	if actual.Locktime != expected.Locktime {
		t.Fatal("Wrong output locktime:", actual.Locktime)
	}

	if actual.Threshold != expected.Threshold {
		t.Fatal("Wrong output threshold:", actual.Threshold)
	}

	if actual.Amount != expected.Amount {
		t.Fatal("Wrong output amount:", actual.Amount)
	}

	if string(actual.RedeemingTransactionID) != string(expected.RedeemingTransactionID) {
		t.Fatal("Wrong output redeeming tx id:", actual.RedeemingTransactionID)
	}
}

func assertAllOutputAddressesCorrect(t *testing.T, db dbr.SessionRunner, expecteds []outputAddress) {
	outputAddresses := []outputAddress{}
	if _, err := db.
		Select("*").
		From("avm_output_addresses").
		OrderAsc("internal_id").
		Load(&outputAddresses); err != nil {
		t.Fatal("Failed to get output addresses")
	}
	assertCorrectOutputAddresses(t, expecteds, outputAddresses)
}

func assertCorrectOutputAddresses(t *testing.T, expecteds, actuals []outputAddress) {
	if len(actuals) != len(expecteds) {
		t.Fatal("Wrong output addresses count:", len(actuals))
	}

	for i, actual := range actuals {
		assertCorrectOutputAddress(t, expecteds[i], actual)
	}
}

func assertCorrectOutputAddress(t *testing.T, expected, actual outputAddress) {
	if !actual.TransactionID.Equals(expected.TransactionID) {
		t.Fatal("Wrong transaction id:", actual.TransactionID)
	}

	if actual.OutputIndex != expected.OutputIndex {
		t.Fatal("Wrong output index:", actual.OutputIndex)
	}

	if !actual.Address.Equals(expected.Address) {
		t.Fatal("Wrong address:", actual.Address)
	}

	if actual.RedeemingSignature.String != expected.RedeemingSignature.String {
		t.Fatal("Wrong redeeming signature:", actual.RedeemingSignature)
	}
}
