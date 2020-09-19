// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"errors"
	"strings"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/codec"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

const (
	// MaxSerializationLen is the maximum number of bytes a canonically
	// serialized tx can be stored as in the database.
	MaxSerializationLen = 64000

	// MaxMemoLen is the maximum number of bytes a memo can be in the database
	MaxMemoLen = 2048
)

var (
	// ErrSerializationTooLong is returned when trying to ingest data with a
	// serialization larger than our max
	ErrSerializationTooLong = errors.New("serialization is too long")
)

func (db *DB) bootstrap(ctx context.Context, genesisBytes []byte, timestamp int64) error {
	var (
		err  error
		job  = db.stream.NewJob("bootstrap")
		sess = db.db.NewSession(job)
	)
	job.KeyValue("chain_id", db.chainID)

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	avmGenesis := &avm.Genesis{}
	if err = db.vm.Codec().Unmarshal(genesisBytes, avmGenesis); err != nil {
		return err
	}

	// Create db tx
	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	var txBytes []byte
	cCtx := services.NewConsumerContext(ctx, job, dbTx, timestamp)
	for _, tx := range avmGenesis.Txs {
		txBytes, err = db.vm.Codec().Marshal(tx)
		if err != nil {
			return err
		}
		err = db.ingestCreateAssetTx(cCtx, txBytes, &tx.CreateAssetTx, tx.Alias, true)
		if err != nil {
			return err
		}
	}

	err = dbTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// AddTx ingests a Transaction and adds it to the services
func (db *DB) Index(ctx context.Context, i services.Consumable) error {
	var (
		err  error
		job  = db.stream.NewJob("index")
		sess = db.db.NewSession(job)
	)
	job.KeyValue("id", i.ID())
	job.KeyValue("chain_id", i.ChainID())

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	// Create db tx
	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Ingest the tx and commit
	err = db.ingestTx(services.NewConsumerContext(ctx, job, dbTx, i.Timestamp()), i.Body())
	if err != nil {
		return err
	}

	err = dbTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) ingestTx(ctx services.ConsumerCtx, txBytes []byte) error {
	tx, err := parseTx(db.vm.Codec(), txBytes)
	if err != nil {
		return err
	}

	// Finish processing with a type-specific ingestion routine
	switch castTx := tx.UnsignedTx.(type) {
	case *avm.GenesisAsset:
		return db.ingestCreateAssetTx(ctx, txBytes, &castTx.CreateAssetTx, castTx.Alias, false)
	case *avm.CreateAssetTx:
		return db.ingestCreateAssetTx(ctx, txBytes, castTx, "", false)
	case *avm.OperationTx:
		// 	db.ingestOperationTx(ctx, tx)
	case *avm.ImportTx:
		return db.ingestBaseTx(ctx, txBytes, tx, &castTx.BaseTx, models.TXTypeImport)
	case *avm.ExportTx:
		return db.ingestBaseTx(ctx, txBytes, tx, &castTx.BaseTx, models.TXTypeExport)
	case *avm.BaseTx:
		return db.ingestBaseTx(ctx, txBytes, tx, castTx, models.TXTypeBase)
	default:
		return errors.New("unknown tx type")
	}
	return nil
}

func (db *DB) ingestCreateAssetTx(ctx services.ConsumerCtx, txBytes []byte, tx *avm.CreateAssetTx, alias string, bootstrap bool) error {
	var err error
	var txID ids.ID
	if bootstrap {
		wrappedTxBytes, err := db.vm.Codec().Marshal(&avm.Tx{UnsignedTx: tx})
		if err != nil {
			return err
		}
		txID = ids.NewID(hashing.ComputeHash256Array(wrappedTxBytes))
	} else {
		txID = tx.ID()
	}

	var outputCount uint32
	var amount uint64
	for _, state := range tx.States {
		for _, out := range state.Outs {
			outputCount++

			switch outtype := out.(type) {
			case *nftfx.TransferOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				db.ingestOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTTransferOutput, outtype.GroupID, outtype.Payload)
			case *nftfx.MintOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				db.ingestOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTMint, outtype.GroupID, nil)
			case *secp256k1fx.MintOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				db.ingestOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTMint, 0, nil)
			case *secp256k1fx.TransferOutput:
				db.ingestOutput(ctx, txID, outputCount-1, txID, outtype, true, models.OutputTypesSECP2556K1Transfer, 0, nil)
				amount, err = math.Add64(amount, outtype.Amount())
				if err != nil {
					_ = ctx.Job().EventErr("add_to_amount", err)
				}
			default:
				_ = ctx.Job().EventErr("assertion_to_output", errors.New("Output is not known"))
			}
		}
	}

	_, err = ctx.DB().
		InsertInto("avm_assets").
		Pair("id", txID.String()).
		Pair("chain_Id", db.chainID).
		Pair("name", tx.Name).
		Pair("symbol", tx.Symbol).
		Pair("denomination", tx.Denomination).
		Pair("alias", alias).
		Pair("current_supply", amount).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return err
	}

	// If the tx or memo is too big we can't store it in the db
	if len(txBytes) > MaxSerializationLen {
		txBytes = []byte{}
	}

	if len(tx.Memo) > MaxMemoLen {
		tx.Memo = nil
	}

	_, err = ctx.DB().
		InsertInto("avm_transactions").
		Pair("id", txID.String()).
		Pair("chain_id", db.chainID).
		Pair("type", models.TXTypeCreateAsset).
		Pair("memo", tx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return err
	}
	return nil
}

func (db *DB) ingestBaseTx(ctx services.ConsumerCtx, txBytes []byte, uniqueTx *avm.Tx, baseTx *avm.BaseTx, txType models.TransactionType) error {
	var (
		err   error
		total uint64 = 0
		creds        = uniqueTx.Credentials()
	)

	unsignedTxBytes, err := db.vm.Codec().Marshal(&uniqueTx.UnsignedTx)
	if err != nil {
		return err
	}

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
		db.ingestOutput(ctx, in.UTXOID.TxID, in.UTXOID.OutputIndex, in.AssetID(), &secp256k1fx.TransferOutput{
			Amt: in.In.Amount(),
			OutputOwners: secp256k1fx.OutputOwners{
				// We leave Addrs blank because we ingested them above with their signatures
				Addrs: []ids.ShortID{},
			},
		}, false, models.OutputTypesSECP2556K1Transfer, 0, nil)

		// For each signature we recover the public key and the data to the db
		cred, ok := creds[i].(*secp256k1fx.Credential)
		if !ok {
			return nil
		}
		for _, sig := range cred.Sigs {
			publicKey, err := db.ecdsaRecoveryFactory.RecoverPublicKey(unsignedTxBytes, sig[:])
			if err != nil {
				return err
			}

			db.ingestAddressFromPublicKey(ctx, publicKey)
			db.ingestOutputAddress(ctx, inputID, publicKey.Address(), sig[:])
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
		Pair("type", txType).
		Pair("memo", baseTx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return err
	}

	// Process baseTx outputs by adding to the outputs table
	for idx, out := range baseTx.Outs {
		xOut, ok := out.Output().(*secp256k1fx.TransferOutput)
		if !ok {
			continue
		}
		db.ingestOutput(ctx, baseTx.ID(), uint32(idx), out.AssetID(), xOut, true, models.OutputTypesSECP2556K1Transfer, 0, nil)
	}
	return nil
}

func (db *DB) ingestOutput(ctx services.ConsumerCtx, txID ids.ID, idx uint32, assetID ids.ID, out *secp256k1fx.TransferOutput, upd bool, outputType models.OutputType, groupID uint32, payload []byte) {
	outputID := txID.Prefix(uint64(idx))

	var err error
	_, err = ctx.DB().
		InsertInto("avm_outputs").
		Pair("id", outputID.String()).
		Pair("chain_id", db.chainID).
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
		if !errIsDuplicateEntryError(err) {
			_ = db.stream.EventErr("ingest_output.insert", err)
			// We got a duplicate entry error and we want to update
		} else if upd {
			if _, err = ctx.DB().
				Update("avm_outputs").
				Set("chain_id", db.chainID).
				Set("output_type", outputType).
				Set("amount", out.Amount()).
				Set("locktime", out.Locktime).
				Set("threshold", out.Threshold).
				Set("group_id", groupID).
				Set("payload", payload).
				Where("avm_outputs.id = ?", outputID.String()).
				ExecContext(ctx.Ctx()); err != nil {
				_ = db.stream.EventErr("ingest_output.update", err)
			}
		}
	}

	// Ingest each Output Address
	for _, addr := range out.Addresses() {
		addrBytes := [20]byte{}
		copy(addrBytes[:], addr)
		db.ingestOutputAddress(ctx, outputID, ids.NewShortID(addrBytes), nil)
	}
}

func (db *DB) ingestAddressFromPublicKey(ctx services.ConsumerCtx, publicKey crypto.PublicKey) {
	_, err := ctx.DB().
		InsertInto("addresses").
		Pair("address", publicKey.Address().String()).
		Pair("public_key", publicKey.Bytes()).
		ExecContext(ctx.Ctx())

	if err != nil && !errIsDuplicateEntryError(err) {
		_ = ctx.Job().EventErr("ingest_address_from_public_key", err)
	}
}

func (db *DB) ingestOutputAddress(ctx services.ConsumerCtx, outputID ids.ID, address ids.ShortID, sig []byte) {
	builder := ctx.DB().
		InsertInto("avm_output_addresses").
		Pair("output_id", outputID.String()).
		Pair("address", address.String()).
		Pair("created_at", ctx.Time())

	if sig != nil {
		builder = builder.Pair("redeeming_signature", sig)
	}

	_, err := builder.ExecContext(ctx.Ctx())
	switch {
	case err == nil:
		return
	case !errIsDuplicateEntryError(err):
		_ = ctx.Job().EventErr("ingest_output_address", err)
		return
	case sig == nil:
		return
	}

	_, err = ctx.DB().
		Update("avm_output_addresses").
		Set("redeeming_signature", sig).
		Where("output_id = ? and address = ?", outputID.String(), address.String()).
		ExecContext(ctx.Ctx())
	if err != nil {
		_ = ctx.Job().EventErr("ingest_output_address", err)
		return
	}
}

func errIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}

func parseTx(c codec.Codec, bytes []byte) (*avm.Tx, error) {
	tx := &avm.Tx{}
	err := c.Unmarshal(bytes, tx)
	if err != nil {
		return nil, err
	}
	unsignedBytes, err := c.Marshal(&tx.UnsignedTx)
	if err != nil {
		return nil, err
	}

	tx.Initialize(unsignedBytes, bytes)
	return tx, nil

	// utx := &avm.UniqueTx{
	// 	TxState: &avm.TxState{
	// 		Tx: tx,
	// 	},
	// 	txID: tx.ID(),
	// }
	//
	// return utx, nil
}
