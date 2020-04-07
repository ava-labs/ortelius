package avm_index

import (
	"fmt"
	"testing"

	"github.com/ava-labs/ortelius/services"
)

func TestDB(t *testing.T) {
	stream := services.NewTestStream()
	db := services.NewTestDB(t, stream)
	defer db.Close()
	idx := NewDB(stream, db)

	err := idx.AddTx(
		testVectorBasicAVMTx1ChainID,
		testVectorsBasicAVMTx1ID,
		testVectorsBasicAVMTx1,
	)
	if err != nil {
		t.Fatal("Failed to add tx to RDMBSIndex:", err.Error())
	}

	sess := db.NewSession(stream)

	// Check transactions
	txs := []transaction{}
	if _, err = sess.Select("*").From("avm_transactions").Load(&txs); err != nil {
		t.Fatal("Failed to get transactions")
	}
	assertCorrectTransactions(t, []transaction{{
		ID:                     testVectorsBasicAVMTx1ID.Bytes(),
		ChainID:                testVectorBasicAVMTx1ChainID.Bytes(),
		CanonicalSerialization: testVectorsBasicAVMTx1,

		Amount:      938,
		InputCount:  1,
		OutputCount: 2,
	}}, txs)

	// Check outputs
	outputs := []output{}
	if _, err = sess.Select("*").From("avm_outputs").Load(&outputs); err != nil {
		t.Fatal("Failed to get outputs:", err.Error())
	}
	assertCorrectOutputs(t, []output{{
		TransactionID: testVectorsBasicAVMTx1ID.Bytes(),
		OutputIndex:   0,
		AssetID:       testVectorBasicAVMTx1AssetID.Bytes(),
		OutputType:    AVMOutputTypesSECP2556K1Transfer,
		Amount:        1,
		Locktime:      0,
		Threshold:     1,
	}, {
		TransactionID: testVectorsBasicAVMTx1ID.Bytes(),
		OutputIndex:   1,
		AssetID:       testVectorBasicAVMTx1AssetID.Bytes(),
		OutputType:    AVMOutputTypesSECP2556K1Transfer,
		Amount:        937,
		Locktime:      0,
		Threshold:     1,
	}}, outputs)

	// Check output addresses
	outputAddresses := []outputAddress{}
	_, err = sess.Select("*").From("avm_output_addresses").Load(&outputAddresses)
	if err != nil {
		t.Fatal("Failed to get output addresses")
	}
	assertCorrectOutputAddresses(t, []outputAddress{{
		TransactionID: testVectorsBasicAVMTx1ID.Bytes(),
		OutputIndex:   0,
		Address:       testVectorBasicAVMTx1Addr1.Bytes(),
	}, {
		TransactionID: testVectorsBasicAVMTx1ID.Bytes(),
		OutputIndex:   1,
		Address:       testVectorBasicAVMTx1Addr2.Bytes(),
	}}, outputAddresses)
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

	if !actual.ChainID.Equals(expected.ChainID) {
		fmt.Println("expected chain id:", expected.ChainID)
		t.Fatal("Wrong chain id:", actual.ChainID)
	}

	if actual.NetworkID != 12345 {
		t.Fatal("Wrong network id:", actual.NetworkID)
	}

	if string(actual.CanonicalSerialization) != string(expected.CanonicalSerialization) {
		t.Fatal("Wrong canonical serialization:", actual.CanonicalSerialization)
	}

	if actual.InputCount != 1 {
		t.Fatal("Wrong input count:", actual.InputCount)
	}

	if actual.OutputCount != 2 {
		t.Fatal("Wrong output count:", actual.OutputCount)
	}

	if actual.Amount != 938 {
		t.Fatal("Wrong amount:", actual.Amount)
	}
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
		t.Fatal("Wrong id:", actual.TransactionID)
	}

	if actual.OutputIndex != expected.OutputIndex {
		t.Fatal("Wrong output index:", actual.OutputIndex)
	}

	if !actual.AssetID.Equals(testVectorBasicAVMTx1AssetID.Bytes()) {
		t.Fatal("Wrong asset id:", actual.AssetID)
	}

	if !actual.OutputType.Equals(expected.OutputType) {
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
		fmt.Println("expected:", expected.Address)
		fmt.Println("actual:", actual.Address)
		t.Fatal("Wrong address:", actual.Address)
	}

	if actual.RedeemingSignature.String != expected.RedeemingSignature.String {
		t.Fatal("Wrong redeeming signature:", actual.RedeemingSignature)
	}
}
