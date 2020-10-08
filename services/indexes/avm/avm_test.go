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
	"github.com/ava-labs/ortelius/services/indexes/params"
)

var (
	testXChainID   = ids.NewID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})
	testAVAAssetID = ids.NewID([32]byte{102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237})
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

	outputs, err := idx.ListOutputs(ctx, &params.ListOutputsParams{})
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

// Test vector management
type testVector struct {
	serializedTx []byte
	expecteds    testVectorExpecteds
}

type testVectorExpecteds struct {
	txs      []models.Transaction
	outs     []models.Output
	outAddrs []models.OutputAddress
}

func copyTestVectorExpecteds(e1 testVectorExpecteds) testVectorExpecteds {
	return testVectorExpecteds{
		txs:      copyTxSlice(e1.txs),
		outs:     copyOutputSlice(e1.outs),
		outAddrs: copyOutputAddrSlice(e1.outAddrs),
	}
}

func copyTxSlice(s1 []models.Transaction) []models.Transaction {
	s2 := make([]models.Transaction, len(s1))
	copy(s2, s1)
	return s2
}

func copyOutputSlice(s1 []models.Output) []models.Output {
	s2 := make([]models.Output, len(s1))
	copy(s2, s1)
	return s2
}

func copyOutputAddrSlice(s1 []models.OutputAddress) []models.OutputAddress {
	s2 := make([]models.OutputAddress, len(s1))
	copy(s2, s1)
	return s2
}

func createTestVectors() (testVectors []testVector) {
	// Test data
	testVectorSerializedTxs := [][]byte{
		{0, 0, 0, 0, 0, 0, 48, 57, 7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120, 0, 0, 0, 2, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 0, 0, 0, 0, 1, 134, 160, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 64, 177, 3, 184, 26, 190, 164, 125, 106, 5, 97, 48, 21, 75, 207, 96, 252, 106, 9, 169, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 159, 223, 66, 246, 226, 249, 96, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156, 0, 0, 0, 1, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 0, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 5, 0, 159, 223, 66, 246, 228, 128, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 9, 0, 0, 0, 1, 212, 95, 229, 79, 72, 47, 183, 165, 110, 75, 50, 154, 63, 190, 191, 118, 140, 237, 65, 72, 162, 23, 163, 247, 42, 250, 19, 55, 114, 196, 229, 220, 113, 101, 251, 102, 89, 196, 193, 129, 132, 132, 49, 241, 113, 185, 155, 57, 101, 94, 78, 72, 246, 69, 205, 55, 62, 21, 41, 140, 40, 96, 16, 92, 1},
		{0, 0, 0, 0, 0, 0, 48, 57, 7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120, 0, 0, 0, 2, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 39, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 179, 119, 16, 234, 139, 2, 253, 149, 92, 137, 22, 102, 229, 228, 138, 84, 235, 170, 118, 26, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 159, 223, 66, 246, 226, 210, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156, 0, 0, 0, 1, 50, 96, 126, 151, 94, 2, 115, 191, 120, 172, 106, 118, 195, 165, 202, 214, 0, 67, 248, 107, 138, 123, 212, 98, 132, 24, 249, 28, 22, 12, 153, 33, 0, 0, 0, 1, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 5, 0, 159, 223, 66, 246, 226, 249, 96, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 9, 0, 0, 0, 1, 91, 173, 127, 43, 220, 214, 174, 27, 153, 167, 112, 10, 12, 174, 109, 213, 211, 124, 97, 107, 29, 7, 249, 181, 102, 98, 11, 255, 84, 70, 185, 133, 108, 173, 54, 101, 177, 78, 140, 18, 120, 81, 110, 151, 245, 200, 104, 236, 24, 48, 251, 236, 16, 225, 194, 205, 93, 125, 251, 128, 170, 142, 176, 148, 1},
		{0, 0, 0, 0, 0, 0, 48, 57, 7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120, 0, 0, 0, 2, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 39, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 179, 119, 16, 234, 139, 2, 253, 149, 92, 137, 22, 102, 229, 228, 138, 84, 235, 170, 118, 26, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 0, 0, 0, 0, 1, 95, 143, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156, 0, 0, 0, 1, 50, 96, 126, 151, 94, 2, 115, 191, 120, 172, 106, 118, 195, 165, 202, 214, 0, 67, 248, 107, 138, 123, 212, 98, 132, 24, 249, 28, 22, 12, 153, 33, 0, 0, 0, 0, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 5, 0, 0, 0, 0, 0, 1, 134, 160, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 9, 0, 0, 0, 1, 221, 116, 200, 183, 174, 182, 181, 35, 176, 9, 20, 150, 78, 142, 18, 90, 30, 210, 95, 238, 239, 101, 249, 179, 58, 129, 141, 14, 32, 33, 148, 59, 100, 185, 245, 63, 23, 198, 231, 180, 92, 71, 228, 223, 103, 226, 215, 127, 81, 231, 235, 63, 236, 18, 145, 27, 81, 12, 178, 135, 205, 186, 62, 1, 1},
		{0, 0, 0, 0, 0, 0, 48, 57, 7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120, 0, 0, 0, 2, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 78, 34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 208, 187, 155, 197, 193, 255, 200, 81, 99, 172, 211, 188, 182, 225, 70, 109, 145, 253, 209, 84, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 7, 0, 0, 0, 0, 0, 1, 17, 109, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156, 0, 0, 0, 1, 7, 151, 72, 227, 47, 221, 46, 7, 44, 223, 120, 174, 147, 94, 133, 159, 83, 131, 254, 226, 253, 11, 247, 197, 81, 187, 79, 62, 122, 88, 72, 182, 0, 0, 0, 1, 102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237, 0, 0, 0, 5, 0, 0, 0, 0, 0, 1, 95, 143, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 9, 0, 0, 0, 1, 37, 196, 43, 82, 5, 28, 233, 240, 135, 66, 39, 20, 21, 6, 82, 243, 139, 215, 41, 133, 75, 41, 19, 238, 255, 43, 57, 204, 2, 33, 114, 223, 38, 0, 17, 85, 47, 74, 161, 147, 73, 194, 28, 6, 185, 156, 199, 225, 26, 177, 163, 156, 81, 199, 175, 225, 197, 212, 117, 232, 162, 97, 104, 88, 1},
	}
	addr1 := models.ToAddress(ids.NewShortID([20]byte{64, 177, 3, 184, 26, 190, 164, 125, 106, 5, 97, 48, 21, 75, 207, 96, 252, 106, 9, 169}))
	addr2 := models.ToAddress(ids.NewShortID([20]byte{179, 119, 16, 234, 139, 2, 253, 149, 92, 137, 22, 102, 229, 228, 138, 84, 235, 170, 118, 26}))
	addr3 := models.ToAddress(ids.NewShortID([20]byte{208, 187, 155, 197, 193, 255, 200, 81, 99, 172, 211, 188, 182, 225, 70, 109, 145, 253, 209, 84}))
	addrChange := models.ToAddress(ids.NewShortID([20]byte{60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156}))

	avaxGenesisOutput := models.Output{
		ID:                     "ehWBfjaqMPbtsRfjf3ZPjREFVeAyfiGs2Q52ZUy2pzNNumBRd",
		TransactionID:          "n8XH5JY1EX5VYqDeAhB4Zd4GKxi9UNQy6oPpMsCAj1Q6xkiiL",
		OutputIndex:            0,
		AssetID:                "n8XH5JY1EX5VYqDeAhB4Zd4GKxi9UNQy6oPpMsCAj1Q6xkiiL",
		OutputType:             models.OutputTypesSECP2556K1Transfer,
		Amount:                 "45000000000000000",
		CreatedAt:              time.Unix(1, 0),
		RedeemingTransactionID: "PBp3ZQyjnLNfKPLP1FM48GreKBKxrY5PRAL2PLtPsj9v71kca",
	}

	avaxGenesisOutputAddress := models.OutputAddress{
		OutputID: avaxGenesisOutput.ID,
		Address:  "6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV",
	}

	// Create helpers for building the set of expected objects
	var txID models.StringID
	var i int64 = 0

	expecteds := testVectorExpecteds{
		outs:     []models.Output{avaxGenesisOutput},
		outAddrs: []models.OutputAddress{avaxGenesisOutputAddress},
	}

	addVector := func(e testVectorExpecteds) {
		testVectors = append(testVectors, testVector{
			serializedTx: testVectorSerializedTxs[i],
			expecteds:    e,
		})
		i++
	}

	nextTx := func() models.Transaction {
		return models.Transaction{
			ID:                     txID,
			CanonicalSerialization: testVectorSerializedTxs[i],
			CreatedAt:              time.Unix(i+1, 0),
		}
	}

	calculateOutputID := func(txIDStr models.StringID, idx uint64) (ids.ID, error) {
		txID, err := ids.FromString(string(txIDStr))
		if err != nil {
			return ids.ID{}, err
		}

		return txID.Prefix(idx), nil
	}

	outsFor := func(amounts ...uint64) []models.Output {
		outs := make([]models.Output, len(amounts))
		for i, amount := range amounts {
			outs[i] = models.Output{
				TransactionID: txID,
				OutputIndex:   uint64(i),
				Amount:        models.TokenAmountForUint64(amount),
				AssetID:       models.ToStringID(testAVAAssetID),
				OutputType:    models.OutputTypesSECP2556K1Transfer,
				Threshold:     1,
			}

			outID, _ := calculateOutputID(outs[i].TransactionID, outs[i].OutputIndex)
			outs[i].ID = models.ToStringID(outID)
		}
		return outs
	}

	outAddrsFor := func(addr models.Address) []models.OutputAddress {
		outID1, _ := calculateOutputID(txID, 0)
		outID2, _ := calculateOutputID(txID, 1)

		return []models.OutputAddress{{
			OutputID: models.ToStringID(outID1),
			Address:  addr,
		}, {
			OutputID: models.ToStringID(outID2),
			Address:  addrChange,
		}}
	}

	stringIDOfBytes := func(b []byte) models.StringID {
		return models.ToStringID(ids.NewID(hashing.ComputeHash256Array(b)))
	}
	// Add tx 1
	txID = stringIDOfBytes(testVectorSerializedTxs[0])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.txs = append(expecteds.txs, nextTx())
	expecteds.outs = append(expecteds.outs, outsFor(100000, 44999999999900000)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr1)...)
	addVector(expecteds)

	// Add tx2
	txID = stringIDOfBytes(testVectorSerializedTxs[1])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.outs[2].RedeemingTransactionID = txID
	expecteds.txs = append(expecteds.txs, nextTx())
	expecteds.outs = append(expecteds.outs, outsFor(10000, 44999999999890000)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr2)...)
	addVector(expecteds)

	// Add tx3
	txID = stringIDOfBytes(testVectorSerializedTxs[2])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.outs[1].RedeemingTransactionID = txID
	expecteds.txs = append(expecteds.txs, nextTx())
	expecteds.outs = append(expecteds.outs, outsFor(10001, 89999)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr2)...)
	addVector(expecteds)

	// Add tx4
	txID = stringIDOfBytes(testVectorSerializedTxs[3])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.outs[6].RedeemingTransactionID = txID
	expecteds.txs = append(expecteds.txs, nextTx())
	expecteds.outs = append(expecteds.outs, outsFor(20002, 69997)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr3)...)
	addVector(expecteds)

	return testVectors
}
