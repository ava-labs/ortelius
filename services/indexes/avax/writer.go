// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"fmt"
	"reflect"

	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/platformvm"
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
	chainID string
	stream  *health.Stream
}

func NewWriter(chainID string, stream *health.Stream) *Writer {
	return &Writer{chainID: chainID, stream: stream}
}

func (w *Writer) InsertTransaction(ctx services.ConsumerCtx, txBytes []byte, unsignedBytes []byte, baseTx *avax.BaseTx, creds []verify.Verifiable, txType models.TransactionType, addIns []*avax.TransferableInput, addOuts []*avax.TransferableOutput) error {
	var (
		err   error
		total uint64 = 0
		errs         = wrappers.Errs{}
	)

	for i, in := range append(baseTx.Ins, addIns...) {
		total, err = math.Add64(total, in.Input().Amount())
		if err != nil {
			errs.Add(err)
		}

		inputID := in.TxID.Prefix(uint64(in.OutputIndex))

		_, err = ctx.DB().
			InsertInto("avm_outputs_redeeming").
			Pair("id", inputID.String()).
			Pair("redeemed_at", dbr.Now).
			Pair("redeeming_transaction_id", baseTx.ID().String()).
			Pair("amount", in.Input().Amount()).
			Pair("output_index", in.OutputIndex).
			Pair("created_at", ctx.Time()).
			ExecContext(ctx.Ctx())
		if err != nil && !db.ErrIsDuplicateEntryError(err) {
			errs.Add(err)
		}

		// Upsert this input as an output in case we haven't seen the parent tx
		// We leave Addrs blank because we inserted them above with their signatures
		// w.InsertOutput(ctx, in.UTXOID.TxID, in.UTXOID.OutputIndex, in.AssetID(), &secp256k1fx.TransferOutput{
		// 	Amt:          in.In.Amount(),
		// 	OutputOwners: secp256k1fx.OutputOwners{},
		// }, models.OutputTypesSECP2556K1Transfer, 0, nil)

		// For each signature we recover the public key and the data to the db
		cred, _ := creds[i].(*secp256k1fx.Credential)
		for _, sig := range cred.Sigs {
			publicKey, err := ecdsaRecoveryFactory.RecoverPublicKey(unsignedBytes, sig[:])
			if err != nil {
				return err
			}

			errs.Add(
				w.InsertAddressFromPublicKey(ctx, publicKey),
				w.InsertOutputAddress(ctx, inputID, publicKey.Address(), sig[:]),
			)
		}
	}

	// If the tx or memo is too big we can't store it in the db
	if len(txBytes) > MaxSerializationLen {
		txBytes = []byte{}
	}

	if len(baseTx.Memo) > MaxMemoLen {
		baseTx.Memo = nil
	}

	// Add baseTx to the table
	_, err = ctx.DB().
		InsertInto("avm_transactions").
		Pair("id", baseTx.ID().String()).
		Pair("chain_id", w.chainID).
		Pair("type", txType.String()).
		Pair("memo", baseTx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		errs.Add(err)
	}

	// Process baseTx outputs by adding to the outputs table
	for idx, out := range append(baseTx.Outs, addOuts...) {
		switch transferOutput := out.Out.(type) {
		case *platformvm.StakeableLockOut:
			xOut, ok := transferOutput.TransferableOut.(*secp256k1fx.TransferOutput)
			if !ok {
				return fmt.Errorf("invalid type *secp256k1fx.TransferOutput")
			}
			// needs to support StakeableLockOut Locktime...
			// xOut.Locktime = transferOutput.Locktime
			errs.Add(w.InsertOutput(ctx, baseTx.ID(), uint32(idx), out.AssetID(), xOut, models.OutputTypesSECP2556K1Transfer, 0, nil))
		case *secp256k1fx.TransferOutput:
			errs.Add(w.InsertOutput(ctx, baseTx.ID(), uint32(idx), out.AssetID(), transferOutput, models.OutputTypesSECP2556K1Transfer, 0, nil))
		default:
			errs.Add(fmt.Errorf("unknown type %s", reflect.TypeOf(transferOutput)))
		}
	}
	return errs.Err
}

func (w *Writer) InsertOutput(ctx services.ConsumerCtx, txID ids.ID, idx uint32, assetID ids.ID, out *secp256k1fx.TransferOutput, outputType models.OutputType, groupID uint32, payload []byte) error {
	outputID := txID.Prefix(uint64(idx))

	var err error
	errs := wrappers.Errs{}
	_, err = ctx.DB().
		InsertInto("avm_outputs").
		Pair("id", outputID.String()).
		Pair("chain_id", w.chainID).
		Pair("transaction_id", txID.String()).
		Pair("output_index", idx).
		Pair("asset_id", assetID.String()).
		Pair("output_type", outputType).
		Pair("amount", out.Amount()).
		Pair("locktime", out.Locktime).
		Pair("threshold", out.Threshold).
		Pair("group_id", groupID).
		Pair("payload", payload).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		errs.Add(w.stream.EventErr("insert_output.insert", err))
	}

	// Ingest each Output Address
	for _, addr := range out.Addresses() {
		addrBytes := [20]byte{}
		copy(addrBytes[:], addr)
		errs.Add(w.InsertOutputAddress(ctx, outputID, ids.NewShortID(addrBytes), nil))
	}
	return errs.Err
}

func (w *Writer) InsertAddressFromPublicKey(ctx services.ConsumerCtx, publicKey crypto.PublicKey) error {
	_, err := ctx.DB().
		InsertInto("addresses").
		Pair("address", publicKey.Address().String()).
		Pair("public_key", publicKey.Bytes()).
		ExecContext(ctx.Ctx())

	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("insert_address_from_public_key", err)
	}
	return nil
}

func (w *Writer) InsertOutputAddress(ctx services.ConsumerCtx, outputID ids.ID, address ids.ShortID, sig []byte) error {
	builder := ctx.DB().
		InsertInto("avm_output_addresses").
		Pair("output_id", outputID.String()).
		Pair("address", address.String()).
		Pair("created_at", ctx.Time())

	if sig != nil {
		builder = builder.Pair("redeeming_signature", sig)
	}

	_, err := builder.ExecContext(ctx.Ctx())
	errs := wrappers.Errs{}
	switch {
	case err == nil:
		return nil
	case !db.ErrIsDuplicateEntryError(err):
		errs.Add(ctx.Job().EventErr("insert_output_address", err))
	case sig == nil:
		return nil
	}

	_, err = ctx.DB().
		Update("avm_output_addresses").
		Set("redeeming_signature", sig).
		Where("output_id = ? and address = ?", outputID.String(), address.String()).
		ExecContext(ctx.Ctx())
	if err != nil {
		errs.Add(ctx.Job().EventErr("insert_output_address", err))
	}

	return errs.Err
}
