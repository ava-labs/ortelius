// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
)

var (
	testXChainID   = ids.NewID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})
	testAVAAssetID = ids.NewID([32]byte{102, 120, 244, 148, 78, 145, 97, 160, 180, 127, 210, 143, 194, 49, 223, 176, 3, 60, 202, 183, 27, 214, 191, 129, 132, 160, 171, 238, 108, 158, 146, 237})
)

type testVector struct {
	serializedTx []byte
	expecteds    testVectorExpecteds
}

type testVectorExpecteds struct {
	txs      []transaction
	outs     []output
	outAddrs []outputAddress
}

func copyTestVectorExpecteds(e1 testVectorExpecteds) testVectorExpecteds {
	return testVectorExpecteds{
		txs:      copyTxSlice(e1.txs),
		outs:     copyOutputSlice(e1.outs),
		outAddrs: copyOutputAddrSlice(e1.outAddrs),
	}
}

func copyTxSlice(s1 []transaction) []transaction {
	s2 := make([]transaction, len(s1))
	copy(s2, s1)
	return s2
}

func copyOutputSlice(s1 []output) []output {
	s2 := make([]output, len(s1))
	copy(s2, s1)
	return s2
}

func copyOutputAddrSlice(s1 []outputAddress) []outputAddress {
	s2 := make([]outputAddress, len(s1))
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
	addr1 := []byte{64, 177, 3, 184, 26, 190, 164, 125, 106, 5, 97, 48, 21, 75, 207, 96, 252, 106, 9, 169}
	addr2 := []byte{179, 119, 16, 234, 139, 2, 253, 149, 92, 137, 22, 102, 229, 228, 138, 84, 235, 170, 118, 26}
	addr3 := []byte{208, 187, 155, 197, 193, 255, 200, 81, 99, 172, 211, 188, 182, 225, 70, 109, 145, 253, 209, 84}
	addrChange := []byte{60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156}

	// Create helpers for building the set of expected objects
	var txID []byte
	var i int64 = 0

	expecteds := testVectorExpecteds{}
	addVector := func(e testVectorExpecteds) {
		testVectors = append(testVectors, testVector{
			serializedTx: testVectorSerializedTxs[i],
			expecteds:    e,
		})
		i += 1
	}

	nextTx := func(amount uint64) transaction {
		return transaction{
			ID:                     txID,
			CanonicalSerialization: testVectorSerializedTxs[i],
			InputCount:             1,
			OutputCount:            2,
			Amount:                 amount,
			IngestedAt:             time.Unix(i+1, 0),
		}
	}

	outsFor := func(amounts ...uint64) []output {
		outs := make([]output, len(amounts))
		for i, amount := range amounts {
			outs[i] = output{
				TransactionID: txID,
				OutputIndex:   uint16(i),
				Amount:        amount,
				AssetID:       testAVAAssetID.Bytes(),
				OutputType:    OutputTypesSECP2556K1Transfer,
			}
		}
		return outs
	}

	outAddrsFor := func(addr []byte) []outputAddress {
		return []outputAddress{{
			TransactionID: txID,
			OutputIndex:   0,
			Address:       addr,
		}, {
			TransactionID: txID,
			OutputIndex:   1,
			Address:       addrChange,
		}}
	}

	// Add tx 1
	txID = hashing.ComputeHash256(testVectorSerializedTxs[0])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.txs = append(expecteds.txs, nextTx(45000000000000000))
	expecteds.outs = append(expecteds.outs, outsFor(100000, 44999999999900000)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr1)...)
	addVector(expecteds)

	// Add tx2
	txID = hashing.ComputeHash256(testVectorSerializedTxs[1])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.outs[1].RedeemingTransactionID = txID
	expecteds.txs = append(expecteds.txs, nextTx(44999999999900000))
	expecteds.outs = append(expecteds.outs, outsFor(10000, 44999999999890000)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr2)...)
	addVector(expecteds)

	// Add tx3
	txID = hashing.ComputeHash256(testVectorSerializedTxs[2])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.outs[0].RedeemingTransactionID = txID
	expecteds.txs = append(expecteds.txs, nextTx(100000))
	expecteds.outs = append(expecteds.outs, outsFor(10001, 89999)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr2)...)
	addVector(expecteds)

	// Add tx4
	txID = hashing.ComputeHash256(testVectorSerializedTxs[3])
	expecteds = copyTestVectorExpecteds(expecteds)
	expecteds.outs[5].RedeemingTransactionID = txID
	expecteds.txs = append(expecteds.txs, nextTx(89999))
	expecteds.outs = append(expecteds.outs, outsFor(20002, 69997)...)
	expecteds.outAddrs = append(expecteds.outAddrs, outAddrsFor(addr3)...)
	addVector(expecteds)

	return testVectors
}
