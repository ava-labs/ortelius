// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/ava-labs/gecko/utils/math"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/secp256k1fx"
	"github.com/gocraft/dbr"
)

type ingestCtx struct {
	context.Context
	db dbr.SessionRunner

	timestamp              uint64
	jsonSerialization      []byte
	canonicalSerialization []byte
}

func errIsNotDuplicateEntryError(err error) bool {
	return strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}

func (i *Index) Bootstrap() error {
	g, err := genesis.VMGenesis(12345, avm.ID)
	if err != nil {
		return err
	}

	avmGenesis := &avm.Genesis{}
	if err := i.vm.Codec().Unmarshal(g.GenesisData, avmGenesis); err != nil {
		return err
	}

	timestamp, err := platformGenesisTimestamp()
	if err != nil {
		return err
	}

	for _, tx := range avmGenesis.Txs {
		txBytes, err := i.vm.Codec().Marshal(tx)
		if err != nil {
			return err
		}
		utx := &avm.UniqueTx{TxState: &avm.TxState{Tx: &avm.Tx{UnsignedTx: tx, Creds: nil}}}
		if err := i.db.AddTx(utx, timestamp, txBytes); err != nil {
			return err
		}
	}

	return nil
}

// AddTx ingests a transaction and adds it to the services
func (r *DBIndex) AddTx(tx *avm.UniqueTx, ts uint64, canonicalSerialization []byte) error {
	// Create db tx
	sess := r.newDBSession("add_tx")
	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}

	defer dbTx.RollbackUnlessCommitted()

	// Ingest the tx and commit
	err = r.ingestTx(ingestCtx{
		db:                     dbTx,
		timestamp:              ts,
		canonicalSerialization: canonicalSerialization,
	}, tx)
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (r *DBIndex) ingestTx(ctx ingestCtx, tx *avm.UniqueTx) error {
	// Create the JSON serialization that we'll
	var err error
	ctx.jsonSerialization, err = json.Marshal(tx)
	if err != nil {
		return err
	}

	// Validate that the serializations aren't too long
	if len(ctx.canonicalSerialization) > MaxSerializationLen {
		return ErrSerializationTooLong
	}
	if len(ctx.jsonSerialization) > MaxSerializationLen {
		return ErrSerializationTooLong
	}

	// Finish processing with a type-specific ingestion routine
	switch tx := tx.UnsignedTx.(type) {
	case *avm.GenesisAsset:
		return r.ingestCreateAssetTx(ctx, &tx.CreateAssetTx, tx.Alias)
	case *avm.CreateAssetTx:
		return r.ingestCreateAssetTx(ctx, tx, "")
	case *avm.OperationTx:
		// 	r.ingestOperationTx(ctx, tx)
	case *avm.BaseTx:
		return r.ingestBaseTx(ctx, tx)
	default:
		return errors.New("unknown tx type")
	}
	return nil
}

func (r *DBIndex) ingestCreateAssetTx(ctx ingestCtx, tx *avm.CreateAssetTx, alias string) error {
	wrappedTxBytes, err := r.codec.Marshal(&avm.Tx{UnsignedTx: tx})
	if err != nil {
		return err
	}
	txID := ids.NewID(hashing.ComputeHash256Array(wrappedTxBytes))

	outputCount := 0
	var amount uint64
	for _, state := range tx.States {
		for _, out := range state.Outs {
			outputCount++

			xOut, ok := out.(*secp256k1fx.TransferOutput)
			if !ok {
				continue
			}

			err := r.ingestOutput(ctx, txID, outputCount-1, txID, xOut)
			if err != nil && !errIsNotDuplicateEntryError(err) {
				return err
			}
			amount, err = math.Add64(amount, xOut.Amount())
			if err != nil {
				return err
			}
		}
	}

	_, err = ctx.db.
		InsertInto("avm_assets").
		Pair("id", txID.Bytes()).
		Pair("chain_Id", r.chainID.Bytes()).
		Pair("name", tx.Name).
		Pair("symbol", tx.Symbol).
		Pair("denomination", tx.Denomination).
		Pair("alias", alias).
		Pair("current_supply", amount).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	_, err = ctx.db.
		InsertInto("avm_transactions").
		Pair("id", txID.Bytes()).
		Pair("chain_id", r.chainID.Bytes()).
		Pair("type", TXTypeCreateAsset).
		Pair("amount", amount).
		Pair("input_count", 0).
		Pair("output_count", outputCount).
		Pair("ingested_at", time.Unix(int64(ctx.timestamp), 0)).
		Pair("canonical_serialization", ctx.canonicalSerialization).
		Pair("json_serialization", ctx.jsonSerialization).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}
	return nil
}

func (r *DBIndex) ingestBaseTx(ctx ingestCtx, tx *avm.BaseTx) error {
	// Process tx inputs by calculating the tx volume and marking the outpoints
	// as spent
	var (
		err   error
		total uint64 = 0
	)

	redeemOutputsConditions := []dbr.Builder{}
	for _, in := range tx.Ins {
		total, err = math.Add64(total, in.Input().Amount())
		if err != nil {
			return err
		}

		redeemOutputsConditions = append(redeemOutputsConditions, dbr.And(
			dbr.Expr("transaction_id = ?", in.TxID.Bytes()),
			dbr.Eq("output_index", in.OutputIndex),
		))

		// db.Update("output_addresses").Set("redeeming_signature", in.In.SigIndices)
	}

	if len(redeemOutputsConditions) > 0 {
		_, err = ctx.db.
			Update("avm_outputs").
			Set("redeeming_transaction_id", tx.ID().Bytes()).
			Where(dbr.Or(redeemOutputsConditions...)).
			Exec()
		if err != nil {
			return err
		}
	}

	// Add tx to the table
	_, err = ctx.db.
		InsertInto("avm_transactions").
		Pair("id", tx.ID().Bytes()).
		Pair("chain_id", tx.BCID.Bytes()).
		Pair("type", TXTypeBase).
		Pair("amount", total).
		Pair("input_count", len(tx.Ins)).
		Pair("output_count", len(tx.Outs)).
		Pair("ingested_at", time.Unix(int64(ctx.timestamp), 0)).
		Pair("canonical_serialization", ctx.canonicalSerialization).
		Pair("json_serialization", ctx.jsonSerialization).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	// Process tx outputs by adding to the outputs table
	for idx, out := range tx.Outs {
		xOut, ok := out.Output().(*secp256k1fx.TransferOutput)
		if !ok {
			continue
		}
		err = r.ingestOutput(ctx, tx.ID(), idx, out.AssetID(), xOut)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *DBIndex) ingestOutput(ctx ingestCtx, txID ids.ID, idx int, assetID ids.ID, out *secp256k1fx.TransferOutput) error {
	_, err := ctx.db.
		InsertInto("avm_outputs").
		Pair("transaction_id", txID.Bytes()).
		Pair("output_index", idx).
		Pair("asset_id", assetID.Bytes()).
		Pair("output_type", OutputTypesSECP2556K1Transfer).
		Pair("amount", out.Amount()).
		// Pair("locktime", out.Output().).
		// Pair("threshold", out.Output().Threshold).
		Pair("locktime", 0).
		Pair("threshold", 0).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	for _, addr := range out.Addresses() {
		_, err = ctx.db.
			InsertInto("avm_output_addresses").
			Pair("transaction_id", txID.Bytes()).
			Pair("output_index", idx).
			Pair("address", addr).
			Exec()
		if err != nil && !errIsNotDuplicateEntryError(err) {
			return err
		}
	}

	return nil
}
