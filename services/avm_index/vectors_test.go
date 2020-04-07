// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
)

var (
	testVectorsBasicAVMTx1ID = ids.NewID(hashing.ComputeHash256Array(testVectorsBasicAVMTx1))
	testVectorsBasicAVMTx1   = []byte(`{"unsignedTx":{"networkID":12345,"blockchainID":"i8KtK2KwLi1o7WaVBbEKpRLPtAEYayfoptqAFYxfQgrus1g6m","outputs":[{"assetID":"XPvpLaDGPK9L3C8Sm4xnxBPtspfaHKa2kY1oaVBhrJJnWvucC","output":{"amount":1,"locktime":0,"threshold":1,"addresses":["AmBV5DFz9rGVcuhXaQrB6uo4yrNBHqycS"]}},{"assetID":"XPvpLaDGPK9L3C8Sm4xnxBPtspfaHKa2kY1oaVBhrJJnWvucC","output":{"amount":937,"locktime":0,"threshold":1,"addresses":["6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV"]}}],"inputs":[{"txID":"2JXg1cJ8d8Njfa5zMd8qrR9eNxq9TYeMraeLxz5m7DvNt2EEMB","outputIndex":1,"assetID":"XPvpLaDGPK9L3C8Sm4xnxBPtspfaHKa2kY1oaVBhrJJnWvucC","input":{"amount":938,"signatureIndices":[0]}}]},"credentials":[{"signatures":[[240,103,248,210,39,96,144,200,59,104,217,199,14,102,231,132,0,244,59,163,50,158,53,64,253,160,147,228,24,156,198,247,15,170,158,76,161,198,156,114,247,111,6,16,6,223,132,238,90,73,112,142,88,220,208,250,106,238,203,159,248,3,61,57,0]]}]}`)

	testVectorBasicAVMTx1ChainID = ids.NewID([32]byte{93, 97, 222, 155, 240, 86, 158, 185, 53, 50, 151, 251, 186, 105, 143, 74, 121, 215, 205, 58, 85, 173, 197, 69, 151, 142, 202, 185, 2, 38, 245, 81})
	testVectorBasicAVMTx1AssetID = ids.NewID([32]byte{69, 4, 41, 244, 193, 71, 6, 235, 27, 151, 121, 116, 153, 95, 125, 37, 64, 232, 75, 159, 158, 252, 243, 119, 70, 47, 171, 221, 128, 229, 180, 23})

	testVectorBasicAVMTx1Addr1 = ids.NewShortID([20]byte{107, 20, 47, 156, 165, 65, 19, 45, 14, 50, 76, 189, 87, 210, 34, 11, 211, 20, 180, 83})
	testVectorBasicAVMTx1Addr2 = ids.NewShortID([20]byte{60, 183, 211, 132, 46, 140, 238, 106, 14, 189, 9, 241, 254, 136, 79, 104, 97, 225, 178, 156})
)
