// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"fmt"
	"reflect"
	"time"

	"github.com/palantir/stacktrace"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/formatting"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services"
)

var (
	MaxSerializationLen = (16 * 1024 * 1024) - 1

	// MaxMemoLen is the maximum number of bytes a memo can be in the database
	MaxMemoLen = 1024
)

var ecdsaRecoveryFactory = crypto.FactorySECP256K1R{}

type Writer struct {
	chainID     string
	avaxAssetID ids.ID
}

func NewWriter(chainID string, avaxAssetID ids.ID) *Writer {
	return &Writer{chainID: chainID, avaxAssetID: avaxAssetID}
}

type AddInsContainer struct {
	Ins     []*avax.TransferableInput
	ChainID string
}

type AddOutsContainer struct {
	Outs    []*avax.TransferableOutput
	Stake   bool
	ChainID string
}

func (w *Writer) InsertTransaction(
	ctx services.ConsumerCtx,
	txBytes []byte,
	unsignedBytes []byte,
	baseTx *avax.BaseTx,
	creds []verify.Verifiable,
	txType models.TransactionType,
	addIns *AddInsContainer,
	addOuts *AddOutsContainer,
	addlOutTxfee uint64,
	genesis bool,
) error {
	var (
		err      error
		totalin  uint64 = 0
		totalout uint64 = 0
	)

	inidx := 0
	for _, in := range baseTx.Ins {
		totalin, err = w.InsertTransactionIns(inidx, ctx, totalin, in, baseTx.ID(), creds, unsignedBytes, w.chainID)
		if err != nil {
			return err
		}
		inidx++
	}

	if addIns != nil {
		for _, in := range addIns.Ins {
			totalin, err = w.InsertTransactionIns(inidx, ctx, totalin, in, baseTx.ID(), creds, unsignedBytes, addIns.ChainID)
			if err != nil {
				return err
			}
			inidx++
		}
	}

	var idx uint32
	for _, out := range baseTx.Outs {
		totalout, err = w.InsertTransactionOuts(idx, ctx, totalout, out, baseTx.ID(), w.chainID, false, false)
		if err != nil {
			return err
		}
		idx++
	}

	if addOuts != nil {
		for _, out := range addOuts.Outs {
			totalout, err = w.InsertTransactionOuts(idx, ctx, totalout, out, baseTx.ID(), addOuts.ChainID, addOuts.Stake, false)
			if err != nil {
				return err
			}
			idx++
		}
	}

	txfee := totalin - (totalout + addlOutTxfee)
	if genesis {
		txfee = 0
	} else if totalin < (totalout + addlOutTxfee) {
		txfee = 0
	}

	// Add baseTx to the table
	return w.InsertTransactionBase(
		ctx,
		baseTx.ID(),
		w.chainID,
		txType.String(),
		baseTx.Memo,
		txBytes,
		txfee,
		genesis,
		baseTx.NetworkID,
	)
}

func (w *Writer) InsertTransactionBase(
	ctx services.ConsumerCtx,
	txID ids.ID,
	chainID string,
	txType string,
	memo []byte,
	txBytes []byte,
	txfee uint64,
	genesis bool,
	networkID uint32,
) error {
	if len(txBytes) > MaxSerializationLen {
		txBytes = []byte("")
	}
	if len(memo) > MaxMemoLen {
		memo = nil
	}

	t := &db.Transactions{
		ID:                     txID.String(),
		ChainID:                chainID,
		Type:                   txType,
		Memo:                   memo,
		CanonicalSerialization: txBytes,
		Txfee:                  txfee,
		Genesis:                genesis,
		CreatedAt:              ctx.Time(),
		NetworkID:              networkID,
	}

	return ctx.Persist().InsertTransactions(ctx.Ctx(), ctx.DB(), t, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionIns(
	idx int,
	ctx services.ConsumerCtx,
	totalin uint64,
	in *avax.TransferableInput,
	txID ids.ID,
	creds []verify.Verifiable,
	unsignedBytes []byte,
	chainID string,
) (uint64, error) {
	var err error
	if in.AssetID() == w.avaxAssetID {
		totalin, err = math.Add64(totalin, in.Input().Amount())
		if err != nil {
			return 0, err
		}
	}

	inputID := in.TxID.Prefix(uint64(in.OutputIndex))

	if idx < len(creds) {
		// For each signature we recover the public key and the data to the db
		cred, ok := creds[idx].(*secp256k1fx.Credential)
		if ok {
			for _, sig := range cred.Sigs {
				publicKey, err := ecdsaRecoveryFactory.RecoverPublicKey(unsignedBytes, sig[:])
				if err != nil {
					return 0, err
				}
				err = w.InsertAddressFromPublicKey(ctx, publicKey)
				if err != nil {
					return 0, err
				}

				err = w.InsertOutputAddress(ctx, inputID, publicKey.Address(), sig[:], in.TxID, in.OutputIndex, chainID)
				if err != nil {
					return 0, err
				}
			}
		}
	}

	outputsRedeeming := &db.OutputsRedeeming{
		ID:                     inputID.String(),
		RedeemedAt:             ctx.Time(),
		RedeemingTransactionID: txID.String(),
		Amount:                 in.Input().Amount(),
		OutputIndex:            in.OutputIndex,
		Intx:                   in.TxID.String(),
		AssetID:                in.AssetID().String(),
		ChainID:                chainID,
		CreatedAt:              ctx.Time(),
	}

	err = ctx.Persist().UpdateOutputAddressAccumulateInOutputsProcessed(ctx.Ctx(), ctx.DB(), inputID.String())
	if err != nil {
		return 0, err
	}

	return totalin, ctx.Persist().InsertOutputsRedeeming(ctx.Ctx(), ctx.DB(), outputsRedeeming, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionOuts(
	idx uint32,
	ctx services.ConsumerCtx,
	totalout uint64,
	out *avax.TransferableOutput,
	txID ids.ID,
	chainID string,
	stake bool,
	genesisutxo bool,
) (uint64, error) {
	var err error
	_, totalout, err = w.ProcessStateOut(ctx, out.Out, txID, idx, out.AssetID(), 0, totalout, chainID, stake, genesisutxo)
	if err != nil {
		return 0, err
	}
	return totalout, nil
}

func (w *Writer) InsertOutput(
	ctx services.ConsumerCtx,
	txID ids.ID,
	idx uint32,
	assetID ids.ID,
	out *secp256k1fx.TransferOutput,
	outputType models.OutputType,
	groupID uint32,
	payload []byte,
	stakeLocktime uint64,
	chainID string,
	stake bool,
	frozen bool,
	stakeableout bool,
	genesisutxo bool,
) error {
	outputID := txID.Prefix(uint64(idx))

	var err error

	// Ingest each Output Address
	for _, addr := range out.Addresses() {
		addrBytes := [20]byte{}
		copy(addrBytes[:], addr)
		addrid := ids.ShortID(addrBytes)
		err = w.InsertOutputAddress(ctx, outputID, addrid, nil, txID, idx, chainID)
		if err != nil {
			return err
		}

		outputTxsAccumulate := &db.OutputTxsAccumulate{
			ChainID:       chainID,
			AssetID:       assetID.String(),
			Address:       addrid.String(),
			TransactionID: txID.String(),
			CreatedAt:     time.Now(),
		}
		err = outputTxsAccumulate.ComputeID()
		if err != nil {
			return err
		}
		err = ctx.Persist().InsertOutputTxsAccumulate(ctx.Ctx(), ctx.DB(), outputTxsAccumulate)
		if err != nil {
			return err
		}
	}

	output := &db.Outputs{
		ID:            outputID.String(),
		ChainID:       chainID,
		TransactionID: txID.String(),
		OutputIndex:   idx,
		AssetID:       assetID.String(),
		OutputType:    outputType,
		Amount:        out.Amount(),
		Locktime:      out.Locktime,
		Threshold:     out.Threshold,
		GroupID:       groupID,
		Payload:       payload,
		StakeLocktime: stakeLocktime,
		Stake:         stake,
		Frozen:        frozen,
		Stakeableout:  stakeableout,
		Genesisutxo:   genesisutxo,
		CreatedAt:     ctx.Time(),
	}

	// ensure that addresses are created before the outputs
	return ctx.Persist().InsertOutputs(ctx.Ctx(), ctx.DB(), output, cfg.PerformUpdates)
}

func (w *Writer) InsertAddressFromPublicKey(
	ctx services.ConsumerCtx,
	publicKey crypto.PublicKey,
) error {
	addresses := &db.Addresses{
		Address:   publicKey.Address().String(),
		PublicKey: publicKey.Bytes(),
		CreatedAt: ctx.Time(),
		UpdatedAt: time.Now().UTC(),
	}
	return ctx.Persist().InsertAddresses(ctx.Ctx(), ctx.DB(), addresses, cfg.PerformUpdates)
}

func (w *Writer) InsertOutputAddress(
	ctx services.ConsumerCtx,
	outputID ids.ID,
	address ids.ShortID,
	sig []byte,
	txID ids.ID,
	idx uint32,
	chainID string,
) error {
	addressChain := &db.AddressChain{
		Address:   address.String(),
		ChainID:   chainID,
		CreatedAt: ctx.Time(),
		UpdatedAt: time.Now().UTC(),
	}
	err := ctx.Persist().InsertAddressChain(ctx.Ctx(), ctx.DB(), addressChain, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	bech32Addr, err := formatting.FormatBech32(models.Bech32HRP, address.Bytes())
	if err != nil {
		return err
	}

	addressBech32 := &db.AddressBech32{
		Address:       address.String(),
		Bech32Address: bech32Addr,
		UpdatedAt:     time.Now().UTC(),
	}
	err = ctx.Persist().InsertAddressBech32(ctx.Ctx(), ctx.DB(), addressBech32, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	outputAddressAccumulate := &db.OutputAddressAccumulate{
		OutputID:      outputID.String(),
		Address:       address.String(),
		TransactionID: txID.String(),
		OutputIndex:   idx,
		CreatedAt:     time.Now(),
	}
	err = outputAddressAccumulate.ComputeID()
	if err != nil {
		return err
	}
	err = ctx.Persist().InsertOutputAddressAccumulateOut(ctx.Ctx(), ctx.DB(), outputAddressAccumulate, cfg.PerformUpdates)
	if err != nil {
		return err
	}
	err = ctx.Persist().InsertOutputAddressAccumulateIn(ctx.Ctx(), ctx.DB(), outputAddressAccumulate, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	outputAddresses := &db.OutputAddresses{
		OutputID:           outputID.String(),
		Address:            address.String(),
		RedeemingSignature: sig,
		CreatedAt:          ctx.Time(),
		UpdatedAt:          time.Now().UTC(),
	}
	err = ctx.Persist().InsertOutputAddresses(ctx.Ctx(), ctx.DB(), outputAddresses, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	if sig == nil {
		return nil
	}

	return ctx.Persist().UpdateOutputAddresses(ctx.Ctx(), ctx.DB(), outputAddresses)
}

func (w *Writer) ProcessStateOut(
	ctx services.ConsumerCtx,
	out verify.State,
	txID ids.ID,
	outputCount uint32,
	assetID ids.ID,
	amount uint64,
	totalout uint64,
	chainID string,
	stake bool,
	genesisutxo bool,
) (uint64, uint64, error) {
	xOut := func(oo secp256k1fx.OutputOwners) *secp256k1fx.TransferOutput {
		return &secp256k1fx.TransferOutput{OutputOwners: oo}
	}

	var err error

	switch typedOut := out.(type) {
	case *platformvm.StakeableLockOut:
		xOut, ok := typedOut.TransferableOut.(*secp256k1fx.TransferOutput)
		if !ok {
			return 0, 0, fmt.Errorf("invalid type *secp256k1fx.TransferOutput")
		}
		if assetID == w.avaxAssetID {
			totalout, err = math.Add64(totalout, xOut.Amt)
			if err != nil {
				return 0, 0, err
			}
		}

		err = w.InsertOutput(
			ctx,
			txID,
			outputCount,
			assetID,
			xOut,
			models.OutputTypesSECP2556K1Transfer,
			0,
			nil,
			typedOut.Locktime,
			chainID,
			stake,
			false,
			true,
			genesisutxo,
		)
		if err != nil {
			return 0, 0, err
		}
	case *nftfx.TransferOutput:
		err = w.InsertOutput(
			ctx,
			txID,
			outputCount,
			assetID,
			xOut(typedOut.OutputOwners),
			models.OutputTypesNFTTransfer,
			typedOut.GroupID,
			typedOut.Payload,
			0,
			chainID,
			stake,
			false,
			false,
			genesisutxo,
		)
		if err != nil {
			return 0, 0, err
		}
	case *nftfx.MintOutput:
		err = w.InsertOutput(
			ctx,
			txID,
			outputCount,
			assetID,
			xOut(typedOut.OutputOwners),
			models.OutputTypesNFTMint,
			typedOut.GroupID,
			nil,
			0,
			chainID,
			stake,
			false,
			false,
			genesisutxo,
		)
		if err != nil {
			return 0, 0, err
		}
	case *secp256k1fx.MintOutput:
		err = w.InsertOutput(
			ctx,
			txID,
			outputCount,
			assetID,
			xOut(typedOut.OutputOwners),
			models.OutputTypesSECP2556K1Mint,
			0,
			nil,
			0,
			chainID,
			stake,
			false,
			false,
			genesisutxo,
		)
		if err != nil {
			return 0, 0, err
		}
	case *secp256k1fx.TransferOutput:
		if assetID == w.avaxAssetID {
			totalout, err = math.Add64(totalout, typedOut.Amount())
			if err != nil {
				return 0, 0, err
			}
		}
		err = w.InsertOutput(
			ctx,
			txID,
			outputCount,
			assetID,
			typedOut,
			models.OutputTypesSECP2556K1Transfer,
			0,
			nil,
			0,
			chainID,
			stake,
			false,
			false,
			genesisutxo,
		)
		if err != nil {
			return 0, 0, err
		}
		amount, err = math.Add64(amount, typedOut.Amount())
		if err != nil {
			return 0, 0, stacktrace.Propagate(err, "add %v to %v", typedOut.Amount(), amount)
		}
	default:
		return 0, 0, fmt.Errorf("unknown type %s", reflect.TypeOf(out))
	}

	return amount, totalout, nil
}
