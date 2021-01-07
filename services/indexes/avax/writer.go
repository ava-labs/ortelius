// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"fmt"
	"reflect"

	"github.com/ava-labs/avalanchego/vms/platformvm"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

var (
	MaxSerializationLen = 64000

	// MaxMemoLen is the maximum number of bytes a memo can be in the database
	MaxMemoLen = 2048
)

var ecdsaRecoveryFactory = crypto.FactorySECP256K1R{}

type Writer struct {
	chainID     string
	avaxAssetID ids.ID
	stream      *health.Stream
}

func NewWriter(chainID string, avaxAssetID ids.ID, stream *health.Stream) *Writer {
	return &Writer{chainID: chainID, avaxAssetID: avaxAssetID, stream: stream}
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
		totalout, err = w.InsertTransactionOuts(idx, ctx, totalout, out, baseTx.ID(), w.chainID, false)
		if err != nil {
			return err
		}
		idx++
	}

	if addOuts != nil {
		for _, out := range addOuts.Outs {
			totalout, err = w.InsertTransactionOuts(idx, ctx, totalout, out, baseTx.ID(), addOuts.ChainID, addOuts.Stake)
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
) error {
	if len(txBytes) > MaxSerializationLen {
		txBytes = []byte("")
	}
	if len(memo) > MaxMemoLen {
		memo = nil
	}

	t := &services.Transaction{
		TxID:    txID.String(),
		ChainID: chainID,
		TxType:  txType,
		Memo:    memo,
		TxBytes: txBytes,
		Txfee:   txfee,
		Genesis: genesis,
	}

	err := ctx.Persist().InsertTransaction(ctx.Ctx(), ctx.DB(), ctx.Time(), t, cfg.PerformUpdates)
	if err != nil {
		return w.stream.EventErr("InsertTransaction", err)
	}
	return nil
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

	outputsRedeeming := services.OutputsRedeeming{
		InputID:     txID.String(),
		RedeemedAt:  ctx.Time(),
		TxID:        txID.String(),
		Amount:      in.Input().Amount(),
		OutputIndex: in.OutputIndex,
		InTxID:      chainID,
		AssetID:     chainID,
		ChainID:     chainID,
		CreatedAt:   ctx.Time(),
	}

	err = ctx.Persist().InsertOutputsRedeeming(ctx.Ctx(), ctx.DB(), ctx.Time(), &outputsRedeeming, cfg.PerformUpdates)
	if err != nil {
		return 0, w.stream.EventErr("InsertOutputsRedeeming", err)
	}

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
				err = w.InsertOutputAddress(ctx, inputID, publicKey.Address(), sig[:])
				if err != nil {
					return 0, err
				}
			}
		}
	}
	return totalin, nil
}

func (w *Writer) InsertTransactionOuts(
	idx uint32,
	ctx services.ConsumerCtx,
	totalout uint64,
	out *avax.TransferableOutput,
	txID ids.ID,
	chainID string,
	stake bool,
) (uint64, error) {
	var err error
	_, totalout, err = w.ProcessStateOut(ctx, out.Out, txID, idx, out.AssetID(), 0, totalout, chainID, stake)
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
) error {
	outputID := txID.Prefix(uint64(idx))

	var err error
	_, err = ctx.DB().
		InsertInto("avm_outputs").
		Pair("id", outputID.String()).
		Pair("chain_id", chainID).
		Pair("transaction_id", txID.String()).
		Pair("output_index", idx).
		Pair("asset_id", assetID.String()).
		Pair("output_type", outputType).
		Pair("amount", out.Amount()).
		Pair("locktime", out.Locktime).
		Pair("threshold", out.Threshold).
		Pair("group_id", groupID).
		Pair("payload", payload).
		Pair("stake_locktime", stakeLocktime).
		Pair("stake", stake).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return w.stream.EventErr("avm_outputs.insert", err)
	}
	if cfg.PerformUpdates {
		_, err = ctx.DB().
			Update("avm_outputs").
			Set("chain_id", chainID).
			Set("transaction_id", txID.String()).
			Set("output_index", idx).
			Set("asset_id", assetID.String()).
			Set("output_type", outputType).
			Set("amount", out.Amount()).
			Set("locktime", out.Locktime).
			Set("threshold", out.Threshold).
			Set("group_id", groupID).
			Set("payload", payload).
			Set("stake_locktime", stakeLocktime).
			Set("stake", stake).
			Where("id = ?", outputID.String()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return w.stream.EventErr("avm_outputs.update", err)
		}
	}

	// Ingest each Output Address
	for _, addr := range out.Addresses() {
		addrBytes := [20]byte{}
		copy(addrBytes[:], addr)
		err = w.InsertOutputAddress(ctx, outputID, ids.ShortID(addrBytes), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) InsertAddressFromPublicKey(
	ctx services.ConsumerCtx,
	publicKey crypto.PublicKey,
) error {
	_, err := ctx.DB().
		InsertInto("addresses").
		Pair("address", publicKey.Address().String()).
		Pair("public_key", publicKey.Bytes()).
		ExecContext(ctx.Ctx())

	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("addresses.insert", err)
	}
	return nil
}

func (w *Writer) InsertOutputAddress(
	ctx services.ConsumerCtx,
	outputID ids.ID,
	address ids.ShortID,
	sig []byte,
) error {
	_, err := ctx.DB().
		InsertInto("address_chain").
		Pair("address", address.String()).
		Pair("chain_id", w.chainID).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return w.stream.EventErr("address_chain.insert", err)
	}

	builder := ctx.DB().
		InsertInto("avm_output_addresses").
		Pair("output_id", outputID.String()).
		Pair("address", address.String()).
		Pair("created_at", ctx.Time())

	if sig != nil {
		builder = builder.Pair("redeeming_signature", sig)
	}

	_, err = builder.ExecContext(ctx.Ctx())
	switch {
	case err == nil:
		return nil
	case !db.ErrIsDuplicateEntryError(err):
		return ctx.Job().EventErr("avm_output_addresses.insert", err)
	case sig == nil:
		return nil
	}

	_, err = ctx.DB().
		Update("avm_output_addresses").
		Set("redeeming_signature", sig).
		Where("output_id = ? and address = ?", outputID.String(), address.String()).
		ExecContext(ctx.Ctx())
	if err != nil {
		return ctx.Job().EventErr("avm_output_addresses.update", err)
	}

	return nil
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

		// these would be from genesis, and they are stake..
		stake = true

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
		)
		if err != nil {
			return 0, 0, err
		}
		amount, err = math.Add64(amount, typedOut.Amount())
		if err != nil {
			return 0, 0, ctx.Job().EventErr("add_to_amount", err)
		}
	default:
		return 0, 0, ctx.Job().EventErr("assertion_to_output", fmt.Errorf("unknown type %s", reflect.TypeOf(out)))
	}

	return amount, totalout, nil
}
