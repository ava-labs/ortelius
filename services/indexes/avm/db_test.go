// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"encoding/hex"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

type message struct {
	id        ids.ID
	chainID   ids.ID
	body      []byte
	timestamp int64
}

func (m *message) ID() string       { return m.id.String() }
func (m *message) ChainID() string  { return m.chainID.String() }
func (m *message) Body() []byte     { return m.body }
func (m *message) Timestamp() int64 { return m.timestamp }

func newTestIndex(t *testing.T, networkID uint32, chainID ids.ID) (*Index, func()) {
	// Start test redis
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("Failed to create miniredis server:", err.Error())
	}

	conf := cfg.Services{
		DB: &cfg.DB{
			TXDB:   true,
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true",
		},
		Redis: &cfg.Redis{
			Addr: s.Addr(),
		},
	}

	// Create index
	idx, err := New(conf, networkID, chainID.String())
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	return idx, func() {
		s.Close()
		idx.db.db.Close()
	}
}

func TestIngestInputs(t *testing.T) {
	chainID, _ := ids.FromString("4ktRjsAKxgMr2aEzv9SWmrU7Xk5FniHUrVCX4P1TZSfTLZWFM")

	idx, closeFn := newTestIndex(t, 2, chainID)
	defer closeFn()

	tx1 := "00000000000000020887AC3054B78FC778790DF1224E3BC5B4DC16916FDAC23BB13B9AF17B4C06A900000002851C1619FC1F795385801F887D52998633D32FBAF3488A757D47B12AE8A595CA000000070000000000004E20000000000000000000000001000000018DB7D7AC082FA6AC6849543A33787DE24B1A4E3F851C1619FC1F795385801F887D52998633D32FBAF3488A757D47B12AE8A595CA00000007009FDF42F664056000000000000000000000000100000001010FD7648AA1C294C2A637FC19435BD9C8CBDDC5000000019A48B1FC5DC5562DACE4E7DD203825CE8C03A7B1082C82C2E4E80CF26F8806DC00000001851C1619FC1F795385801F887D52998633D32FBAF3488A757D47B12AE8A595CA00000005009FDF42F664538000000001000000000000000100000009000000013AADB2929575970742BCA6D28155E32C7B233B6A2E37C9CFC0A64078A07AE19A38178E4E0A65C32C8D122C21E6DC1983C0D4487191C7DCC9FE63607A2040F3DD01"
	tx2 := "00000000000000020887AC3054B78FC778790DF1224E3BC5B4DC16916FDAC23BB13B9AF17B4C06A900000002851C1619FC1F795385801F887D52998633D32FBAF3488A757D47B12AE8A595CA000000070000000000004E20000000000000000000000001000000018DB7D7AC082FA6AC6849543A33787DE24B1A4E3F851C1619FC1F795385801F887D52998633D32FBAF3488A757D47B12AE8A595CA00000007009FDF42F663B74000000000000000000000000100000001010FD7648AA1C294C2A637FC19435BD9C8CBDDC5000000010F47242454B7810B9C4F2B8D64F89534E5C20FB460D95E09F7C43FD79A0B599C00000001851C1619FC1F795385801F887D52998633D32FBAF3488A757D47B12AE8A595CA00000005009FDF42F66405600000000100000000000000010000000900000001956E1CBC915A60C00DC75DA7D6272A308013D4C7DEB2B2313709A4FB86895CB07F9F7E2B51A5926BACE8AA79C478CB03B607914797F6F28AF76727A8596AC61A01"

	tx1Bytes, _ := hex.DecodeString(tx1)
	tx2Bytes, _ := hex.DecodeString(tx2)

	ctx := newTestContext()

	err := idx.Consume(ctx, &message{
		id:        ids.NewID(hashing.ComputeHash256Array(tx1Bytes)),
		chainID:   chainID,
		body:      tx1Bytes,
		timestamp: 1,
	})
	if err != nil {
		t.Fatal("Failed to index:", err.Error())
	}

	err = idx.Consume(ctx, &message{
		id:        ids.NewID(hashing.ComputeHash256Array(tx2Bytes)),
		chainID:   chainID,
		body:      tx2Bytes,
		timestamp: 2,
	})
	if err != nil {
		t.Fatal("Failed to index:", err.Error())
	}

	outputs, err := idx.ListOutputs(ctx, &ListOutputsParams{})
	if err != nil {
		t.Fatal("Failed to list outputs:", err.Error())
	}

	if len(outputs.Outputs) != 5 {
		t.Fatal("Incorrect number of outputs:", len(outputs.Outputs))
	}

	expectedAddrs := []string{"6cesTteH62Y5mLoDBUASaBvCXuL2AthL", "DvLWeWgVRx914ewTVNjTAopXXHGHkatbD", "6cesTteH62Y5mLoDBUASaBvCXuL2AthL", "6cesTteH62Y5mLoDBUASaBvCXuL2AthL", "DvLWeWgVRx914ewTVNjTAopXXHGHkatbD"}
	for i, output := range outputs.Outputs {
		if len(output.Addresses) != 1 {
			t.Fatal("Incorrect number of Output addresses:", len(output.Addresses))
		}

		if string(output.Addresses[0]) != expectedAddrs[i] {
			t.Fatal("Incorrect Output Address:", output.Addresses[0])
		}
	}
}

func TestIndexBootstrap(t *testing.T) {
	idx, closeFn := newTestIndex(t, 12345, testXChainID)
	defer closeFn()

	err := idx.Bootstrap(newTestContext())
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	var (
		txID          = models.ToStringID(ids.NewID([32]byte{102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237}))
		createAssetTx = []byte{0, 3, 65, 86, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 65, 86, 65, 0, 3, 65, 86, 65, 9, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7, 0, 159, 223, 66, 246, 228, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156}
	)

	db := idx.db.newSession("test_index_bootstrap")
	assertAllTransactionsCorrect(t, db, []models.Transaction{{
		ID:                     txID,
		ChainID:                models.ToStringID(testXChainID),
		CanonicalSerialization: createAssetTx,
		CreatedAt:              time.Unix(1572566400, 0).UTC(),
	}})
}

func TestIndexVectors(t *testing.T) {
	idx, closeFn := newTestIndex(t, 12345, testXChainID)
	defer closeFn()

	db := idx.db.db.NewSession(nil)

	// Start with nothing
	assertAllTransactionsCorrect(t, db, nil)
	assertAllOutputsCorrect(t, db, nil)

	// Add each test vector tx
	ctx := newTestContext()
	for i, v := range createTestVectors() {
		err := idx.Consume(ctx, &message{
			id:        ids.NewID(hashing.ComputeHash256Array(v.serializedTx)),
			chainID:   testXChainID,
			body:      v.serializedTx,
			timestamp: int64(i + 1),
		})
		if err != nil {
			t.Fatal("Failed to add tx to index:", err.Error())
		}

		assertAllTransactionsCorrect(t, db, v.expecteds.txs)
		assertAllOutputsCorrect(t, db, v.expecteds.outs)
		assertAllOutputAddressesCorrect(t, db, v.expecteds.outAddrs)
	}
}

func assertAllTransactionsCorrect(t *testing.T, db dbr.SessionRunner, expecteds []models.Transaction) {
	txs := []models.Transaction{}
	if _, err := db.
		Select("*").
		From("avm_transactions").
		OrderAsc("created_at").
		Load(&txs); err != nil {
		t.Fatal("Failed to get transactions:", err.Error())
	}
	assertCorrectTransactions(t, expecteds, txs)
}

func assertCorrectTransactions(t *testing.T, expecteds, actuals []models.Transaction) {
	if len(actuals) != len(expecteds) {
		t.Fatal("Wrong transactions count:", len(actuals))
	}

	for i, actual := range actuals {
		assertCorrectTransaction(t, expecteds[i], actual)
	}
}

func assertCorrectTransaction(t *testing.T, expected, actual models.Transaction) {
	if !actual.ID.Equals(expected.ID) {
		t.Fatal("Wrong id:", actual.ID)
	}

	if !actual.ChainID.Equals(models.ToStringID(testXChainID)) {
		t.Fatal("Wrong chain id:", actual.ChainID)
	}

	if string(actual.CanonicalSerialization) != string(expected.CanonicalSerialization) {
		t.Fatal("Wrong canonical serialization:", actual.CanonicalSerialization)
	}

	if !actual.CreatedAt.Equal(expected.CreatedAt) {
		t.Fatal("Wrong timestamp:", actual.CreatedAt)
	}
}

func assertAllOutputsCorrect(t *testing.T, db dbr.SessionRunner, expecteds []models.Output) {
	outputs := []models.Output{}
	if _, err := db.
		Select("*").
		From("avm_outputs").
		Load(&outputs); err != nil {
		t.Fatal("Failed to get outputs:", err.Error())
	}
	assertCorrectOutputs(t, expecteds, outputs)
}

func assertCorrectOutputs(t *testing.T, expecteds, actuals []models.Output) {
	if len(actuals) != len(expecteds) {
		t.Fatal("Wrong Output count:", len(actuals))
	}

	sort.Sort(outputsLexically(actuals))
	sort.Sort(outputsLexically(expecteds))

	for i, actual := range actuals {
		assertCorrectOutput(t, expecteds[i], actual)
	}
}

func assertCorrectOutput(t *testing.T, expected, actual models.Output) {
	if !actual.TransactionID.Equals(expected.TransactionID) {
		t.Fatal("Wrong Output Transaction id:", actual.TransactionID)
	}

	if actual.OutputIndex != expected.OutputIndex {
		t.Fatal("Wrong Output index:", actual.OutputIndex)
	}

	if !actual.AssetID.Equals(models.ToStringID(testAVAAssetID)) {
		t.Fatal("Wrong Asset id:", actual.AssetID)
	}

	if actual.OutputType != expected.OutputType {
		t.Fatal("Wrong Output type:", actual.OutputType)
	}

	if actual.Locktime != expected.Locktime {
		t.Fatal("Wrong Output locktime:", actual.Locktime)
	}

	if actual.Threshold != expected.Threshold {
		t.Fatal("Wrong Output threshold:", actual.Threshold)
	}

	if actual.Amount != expected.Amount {
		t.Fatal("Wrong Output amount:", actual.Amount)
	}

	if string(actual.RedeemingTransactionID) != string(expected.RedeemingTransactionID) {
		t.Fatal("Wrong Output redeeming tx id:", actual.RedeemingTransactionID)
	}
}

func assertAllOutputAddressesCorrect(t *testing.T, db dbr.SessionRunner, expecteds []models.OutputAddress) {
	outputAddresses := []models.OutputAddress{}
	if _, err := db.
		Select("*").
		From("avm_output_addresses").
		Load(&outputAddresses); err != nil {
		t.Fatal("Failed to get Output addresses:", err.Error())
	}
	assertCorrectOutputAddresses(t, expecteds, outputAddresses)
}

func assertCorrectOutputAddresses(t *testing.T, expecteds, actuals []models.OutputAddress) {
	if len(actuals) != len(expecteds) {
		t.Fatal("Wrong Output addresses count:", len(actuals))
	}

	sort.Sort(outputAddrsLexically(actuals))
	sort.Sort(outputAddrsLexically(expecteds))

	for i, actual := range actuals {
		assertCorrectOutputAddress(t, expecteds[i], actual)
	}
}

func assertCorrectOutputAddress(t *testing.T, expected, actual models.OutputAddress) {
	if !actual.OutputID.Equals(expected.OutputID) {
		t.Fatal("Wrong Output id:", actual.OutputID)
	}

	if !actual.Address.Equals(expected.Address) {
		t.Fatal("Wrong Address:", actual.Address)
	}

	if string(actual.Signature) != string(expected.Signature) {
		t.Fatal("Wrong redeeming signature:", actual.Signature)
	}
}

func newTestContext() context.Context {
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	time.AfterFunc(5*time.Second, cancelFn)
	return ctx
}

type outputsLexically []models.Output

func (o outputsLexically) Len() int           { return len(o) }
func (o outputsLexically) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
func (o outputsLexically) Less(i, j int) bool { return o[i].ID < o[j].ID }

type outputAddrsLexically []models.OutputAddress

func (o outputAddrsLexically) Len() int      { return len(o) }
func (o outputAddrsLexically) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o outputAddrsLexically) Less(i, j int) bool {
	return string(o[i].OutputID)+string(o[i].Address) < string(o[j].OutputID)+string(o[j].Address)
}
