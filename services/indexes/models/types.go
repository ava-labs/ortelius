// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

var (
	OutputTypesSECP2556K1Transfer OutputType = 7
	OutputTypesSECP2556K1Mint     OutputType = 6
	OutputTypesNFTMint            OutputType = 10
	OutputTypesNFTTransferOutput  OutputType = 11

	TXTypeBase        TransactionType = "base"
	TXTypeCreateAsset TransactionType = "create_asset"
	TXTypeImport      TransactionType = "import"
	TXTypeExport      TransactionType = "export"

	ResultTypeTransaction SearchResultType = "transaction"
	ResultTypeAsset       SearchResultType = "asset"
	ResultTypeAddress     SearchResultType = "address"
	ResultTypeOutput      SearchResultType = "output"
)
