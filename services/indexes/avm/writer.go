// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"errors"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/codec"
	"github.com/ava-labs/avalanchego/utils/hashing"
	avalancheMath "github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
	"github.com/palantir/stacktrace"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
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
	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")
)

type Writer struct {
	db        *services.DB
	chainID   string
	networkID uint32
	codec     codec.Codec
	stream    *health.Stream

	avax *avax.Writer
}

func NewWriter(conns *services.Connections, networkID uint32, chainID string) (*Writer, error) {
	avmCodec, err := newAVMCodec(networkID, chainID)
	if err != nil {
		return nil, err
	}

	return &Writer{
		codec:     avmCodec,
		chainID:   chainID,
		networkID: networkID,
		stream:    conns.Stream(),
		db:        services.NewDB(conns.Stream(), conns.DB()),
		avax:      avax.NewWriter(chainID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "avm-index" }

func (w *Writer) Bootstrap(ctx context.Context) error {
	var (
		err                  error
		platformGenesisBytes []byte
		job                  = w.stream.NewJob("bootstrap")
	)
	job.KeyValue("chain_id", w.chainID)

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	platformGenesisBytes, _, err = genesis.Genesis(w.networkID)
	if err != nil {
		return stacktrace.Propagate(err, "Failed to get platform genesis bytes")
	}

	platformGenesis := &platformvm.Genesis{}
	if err = platformvm.GenesisCodec.Unmarshal(platformGenesisBytes, platformGenesis); err != nil {
		return stacktrace.Propagate(err, "Failed to parse platform genesis bytes")
	}
	if err = platformGenesis.Initialize(); err != nil {
		return stacktrace.Propagate(err, "Failed to initialize platform genesis")
	}

	for _, chain := range platformGenesis.Chains {
		createChainTx, ok := chain.UnsignedTx.(*platformvm.UnsignedCreateChainTx)
		if !ok {
			return stacktrace.Propagate(ErrIncorrectGenesisChainTxType, "Platform genesis contains invalid Chains")
		}

		if !createChainTx.VMID.Equals(avm.ID) {
			continue
		}

		dbSess := w.db.NewSessionForEventReceiver(job)
		cCtx := services.NewConsumerContext(ctx, job, dbSess, int64(platformGenesis.Timestamp))

		return w.insertGenesis(cCtx, createChainTx.GenesisData)
	}
	return nil
}

func (w *Writer) Consume(ctx context.Context, i services.Consumable) error {
	var (
		err  error
		job  = w.stream.NewJob("index")
		sess = w.db.NewSessionForEventReceiver(job)
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
	err = w.insertTx(services.NewConsumerContext(ctx, job, dbTx, i.Timestamp()), i.Body())
	if err != nil {
		return stacktrace.Propagate(err, "Failed to insert tx")
	}

	if err = dbTx.Commit(); err != nil {
		return stacktrace.Propagate(err, "Failed to commit database tx")
	}

	return nil
}

func (w *Writer) insertGenesis(ctx services.ConsumerCtx, genesisBytes []byte) error {
	avmGenesis := &avm.Genesis{}
	if err := w.codec.Unmarshal(genesisBytes, avmGenesis); err != nil {
		return stacktrace.Propagate(err, "Failed to parse avm genesis bytes")
	}

	for i, tx := range avmGenesis.Txs {
		txBytes, err := w.codec.Marshal(tx)
		if err != nil {
			return stacktrace.Propagate(err, "Failed to serialize avm genesis tx %d", i)
		}

		if err = w.insertCreateAssetTx(ctx, txBytes, &tx.CreateAssetTx, tx.Alias, true); err != nil {
			return stacktrace.Propagate(err, "Failed to index avm genesis tx %d", i)
		}
	}w
	return nil
}

func (w *Writer) insertTx(ctx services.ConsumerCtx, txBytes []byte) error {
	tx, err := parseTx(w.codec, txBytes)
	if err != nil {
		return err
	}

	unsignedBytes, err := w.codec.Marshal(&tx.UnsignedTx)
	if err != nil {
		return err
	}

	// Finish processing with a type-specific insertions routine
	switch castTx := tx.UnsignedTx.(type) {
	case *avm.GenesisAsset:
		return w.insertCreateAssetTx(ctx, txBytes, &castTx.CreateAssetTx, castTx.Alias, false)
	case *avm.CreateAssetTx:
		return w.insertCreateAssetTx(ctx, txBytes, castTx, "", false)
	case *avm.OperationTx:
	case *avm.ImportTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx.BaseTx, tx.Credentials(), models.TransactionTypeAVMImport)
	case *avm.ExportTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx.BaseTx, tx.Credentials(), models.TransactionTypeAVMExport)
	case *avm.BaseTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx, tx.Credentials(), models.TransactionTypeBase)
	default:
		return errors.New("unknown tx type")
	}
	return nil
}

func (w *Writer) insertCreateAssetTx(ctx services.ConsumerCtx, txBytes []byte, tx *avm.CreateAssetTx, alias string, bootstrap bool) error {
	var err error
	var txID ids.ID
	if bootstrap {
		wrappedTxBytes, err := w.codec.Marshal(&avm.Tx{UnsignedTx: tx})
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
				w.avax.InsertOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTTransferOutput, outtype.GroupID, outtype.Payload)
			case *nftfx.MintOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				w.avax.InsertOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTMint, outtype.GroupID, nil)
			case *secp256k1fx.MintOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				w.avax.InsertOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTMint, 0, nil)
			case *secp256k1fx.TransferOutput:
				w.avax.InsertOutput(ctx, txID, outputCount-1, txID, outtype, true, models.OutputTypesSECP2556K1Transfer, 0, nil)
				amount, err = avalancheMath.Add64(amount, outtype.Amount())
				if err != nil {
					_ = ctx.Job().EventErr("add_to_amount", err)
				}
			default:
				_ = ctx.Job().EventErr("assertion_to_output", errors.New("output is not known"))
			}
		}
	}

	_, err = ctx.DB().
		InsertInto("avm_assets").
		Pair("id", txID.String()).
		Pair("chain_Id", w.chainID).
		Pair("name", tx.Name).
		Pair("symbol", tx.Symbol).
		Pair("denomination", tx.Denomination).
		Pair("alias", alias).
		Pair("current_supply", amount).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !services.ErrIsDuplicateEntryError(err) {
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
		Pair("chain_id", w.chainID).
		Pair("type", models.TransactionTypeCreateAsset).
		Pair("memo", tx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !services.ErrIsDuplicateEntryError(err) {
		return err
	}
	return nil
}
