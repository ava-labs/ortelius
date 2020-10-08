// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/services"
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

func (w *Writer) InsertTransaction(ctx services.ConsumerCtx, txBytes []byte, unsignedBytes []byte, baseTx *avax.BaseTx, creds []verify.Verifiable, txType models.TransactionType) error {
	var (
		err   error
		total uint64 = 0
	)

	redeemedOutputs := make([]string, 0, 2*len(baseTx.Ins))
	for i, in := range baseTx.Ins {
		total, err = math.Add64(total, in.Input().Amount())
		if err != nil {
			return err
		}

		inputID := in.TxID.Prefix(uint64(in.OutputIndex))

		// Save id so we can mark this output as consumed
		redeemedOutputs = append(redeemedOutputs, inputID.String())

		// Upsert this input as an output in case we haven't seen the parent tx
		w.InsertOutput(ctx, in.UTXOID.TxID, in.UTXOID.OutputIndex, in.AssetID(), &secp256k1fx.TransferOutput{
			Amt: in.In.Amount(),
			OutputOwners: secp256k1fx.OutputOwners{
				// We leave Addrs blank because we inserted them above with their signatures
				Addrs: []ids.ShortID{},
			},
		}, false, models.OutputTypesSECP2556K1Transfer, 0, nil)

		// For each signature we recover the public key and the data to the db
		cred, ok := creds[i].(*secp256k1fx.Credential)
		if !ok {
			return nil
		}

		for _, sig := range cred.Sigs {
			publicKey, err := ecdsaRecoveryFactory.RecoverPublicKey(unsignedBytes, sig[:])
			if err != nil {
				return err
			}

			w.InsertAddressFromPublicKey(ctx, publicKey)
			w.InsertOutputAddress(ctx, inputID, publicKey.Address(), sig[:])
		}
	}

	// Mark all inputs as redeemed
	if len(redeemedOutputs) > 0 {
		_, err = ctx.DB().
			Update("avm_outputs").
			Set("redeemed_at", dbr.Now).
			Set("redeeming_transaction_id", baseTx.ID().String()).
			Where("id IN ?", redeemedOutputs).
			ExecContext(ctx.Ctx())
		if err != nil {
			return err
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
		Pair("chain_id", baseTx.BlockchainID.String()).
		Pair("type", txType.String()).
		Pair("memo", baseTx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !services.ErrIsDuplicateEntryError(err) {
		return err
	}

	// Process baseTx outputs by adding to the outputs table
	for idx, out := range baseTx.Outs {
		xOut, ok := out.Output().(*secp256k1fx.TransferOutput)
		if !ok {
			continue
		}
		w.InsertOutput(ctx, baseTx.ID(), uint32(idx), out.AssetID(), xOut, true, models.OutputTypesSECP2556K1Transfer, 0, nil)
	}
	return nil
}

func (w *Writer) InsertOutput(ctx services.ConsumerCtx, txID ids.ID, idx uint32, assetID ids.ID, out *secp256k1fx.TransferOutput, upd bool, outputType models.OutputType, groupID uint32, payload []byte) error {
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
		Pair("created_at", ctx.Time()).
		Pair("locktime", out.Locktime).
		Pair("threshold", out.Threshold).
		Pair("group_id", groupID).
		Pair("payload", payload).
		ExecContext(ctx.Ctx())

	if err != nil {
		// We got an error and it's not a duplicate entry error, so log it
		if !services.ErrIsDuplicateEntryError(err) {
			errs.Add(w.stream.EventErr("insert_output.insert", err))
			// We got a duplicate entry error and we want to update
		} else if upd {
			if _, err = ctx.DB().
				Update("avm_outputs").
				Set("chain_id", w.chainID).
				Set("output_type", outputType).
				Set("amount", out.Amount()).
				Set("locktime", out.Locktime).
				Set("threshold", out.Threshold).
				Set("group_id", groupID).
				Set("payload", payload).
				Where("avm_outputs.id = ?", outputID.String()).
				ExecContext(ctx.Ctx()); err != nil {
				errs.Add(w.stream.EventErr("insert_output.update", err))
			}
		}
	}

	// Ingest each Output Address
	for _, addr := range out.Addresses() {
		addrBytes := [20]byte{}
		copy(addrBytes[:], addr)
		errs.Add(w.InsertOutputAddress(ctx, outputID, ids.NewShortID(addrBytes), nil))
	}
	return errs.Err
}

func (w *Writer) InsertAddressFromPublicKey(ctx services.ConsumerCtx, publicKey crypto.PublicKey) {
	_, err := ctx.DB().
		InsertInto("addresses").
		Pair("address", publicKey.Address().String()).
		Pair("public_key", publicKey.Bytes()).
		ExecContext(ctx.Ctx())

	if err != nil && !services.ErrIsDuplicateEntryError(err) {
		_ = ctx.Job().EventErr("insert_address_from_public_key", err)
	}
}

func (w *Writer) InsertOutputAddress(ctx services.ConsumerCtx, outputID ids.ID, address ids.ShortID, sig []byte) error {
	builder := ctx.DB().
		InsertInto("avm_output_addresses").
		Pair("output_id", outputID.String()).
		Pair("address", address.String())

	if sig != nil {
		builder = builder.Pair("redeeming_signature", sig)
	}

	_, err := builder.ExecContext(ctx.Ctx())
	errs := wrappers.Errs{}
	switch {
	case err == nil:
		return nil
	case !services.ErrIsDuplicateEntryError(err):
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
