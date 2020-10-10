// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"errors"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/utils/codec"
	avalancheMath "github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
	"github.com/palantir/stacktrace"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

var (
	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")
)

type Writer struct {
	chainID   string
	networkID uint32

	codec codec.Codec
	avax  *avax.Writer
	conns *services.Connections
}

func NewWriter(conns *services.Connections, networkID uint32, chainID string) (*Writer, error) {
	avmCodec, err := newAVMCodec(networkID, chainID)
	if err != nil {
		return nil, err
	}

	return &Writer{
		conns:     conns,
		chainID:   chainID,
		codec:     avmCodec,
		networkID: networkID,
		avax:      avax.NewWriter(chainID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "avm-index" }

func (w *Writer) Bootstrap(ctx context.Context) error {
	var (
		err                  error
		platformGenesisBytes []byte
		job                  = w.conns.Stream().NewJob("bootstrap")
	)
	job.KeyValue("chain_id", w.chainID)

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	// Get platform genesis block
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

	// Scan chains in platform genesis until we find the singular AVM chain, which
	// is the X chain, and then we're done
	for _, chain := range platformGenesis.Chains {
		createChainTx, ok := chain.UnsignedTx.(*platformvm.UnsignedCreateChainTx)
		if !ok {
			return stacktrace.Propagate(ErrIncorrectGenesisChainTxType, "Platform genesis contains invalid Chains")
		}

		if !createChainTx.VMID.Equals(avm.ID) {
			continue
		}

		dbSess := w.conns.DB().NewSessionForEventReceiver(job)
		cCtx := services.NewConsumerContext(ctx, job, dbSess, int64(platformGenesis.Timestamp))
		return w.insertGenesis(cCtx, createChainTx.GenesisData)
	}
	return nil
}

func (w *Writer) Consume(ctx context.Context, i services.Consumable) error {
	var (
		err  error
		job  = w.conns.Stream().NewJob("index")
		sess = w.conns.DB().NewSessionForEventReceiver(job)
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
		txBytes, err := w.codec.Marshal(&avm.Tx{UnsignedTx: &tx.CreateAssetTx})
		if err != nil {
			return stacktrace.Propagate(err, "Failed to serialize wrapped avm genesis tx %d", i)
		}
		unsignedBytes, err := w.codec.Marshal(&tx.CreateAssetTx)
		if err != nil {
			return err
		}

		tx.Initialize(unsignedBytes, txBytes)

		if err = w.insertCreateAssetTx(ctx, txBytes, &tx.CreateAssetTx, nil, tx.Alias); err != nil {
			return stacktrace.Propagate(err, "Failed to index avm genesis tx %d", i)
		}
	}
	return nil
}

func (w *Writer) insertTx(ctx services.ConsumerCtx, txBytes []byte) error {
	tx, err := parseTx(w.codec, txBytes)
	if err != nil {
		return err
	}

	// fire and forget..
	// update the created_at on the state table if we have an earlier date in ctx.Time().
	// which means we need to re-run aggregation calculations from this earlier date.
	_, _ = models.UpdateAvmAssetAggregationLiveStateTimestamp(ctx.Ctx(), ctx.DB(), ctx.Time())

	// Finish processing with a type-specific ingestion routine
	unsignedBytes, err := w.codec.Marshal(&tx.UnsignedTx)
	if err != nil {
		return err
	}

	tx.Initialize(unsignedBytes, txBytes)

	// Finish processing with a type-specific insertions routine
	switch castTx := tx.UnsignedTx.(type) {
	case *avm.CreateAssetTx:
		return w.insertCreateAssetTx(ctx, txBytes, castTx, tx.Credentials(), "")
	case *avm.OperationTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx.BaseTx, tx.Credentials(), models.TransactionTypeOperation)
	case *avm.ImportTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx.BaseTx, tx.Credentials(), models.TransactionTypeAVMImport)
	case *avm.ExportTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx.BaseTx, tx.Credentials(), models.TransactionTypeAVMExport)
	case *avm.BaseTx:
		return w.avax.InsertTransaction(ctx, txBytes, unsignedBytes, &castTx.BaseTx, tx.Credentials(), models.TransactionTypeBase)
	default:
		return errors.New("unknown tx type")
	}
}

func (w *Writer) insertCreateAssetTx(ctx services.ConsumerCtx, txBytes []byte, tx *avm.CreateAssetTx, creds []verify.Verifiable, alias string) error {
	var (
		err         error
		outputCount uint32
		amount      uint64
		errs        = wrappers.Errs{}
	)

	errs.Add(w.avax.InsertTransaction(ctx, txBytes, tx.UnsignedBytes(), &tx.BaseTx.BaseTx, creds, models.TransactionTypeCreateAsset))

	xOut := func(oo secp256k1fx.OutputOwners) *secp256k1fx.TransferOutput {
		return &secp256k1fx.TransferOutput{OutputOwners: oo}
	}

	for _, state := range tx.States {
		for _, out := range state.Outs {
			switch typedOut := out.(type) {
			case *nftfx.TransferOutput:
				errs.Add(w.avax.InsertOutput(ctx, tx.ID(), outputCount, tx.ID(), xOut(typedOut.OutputOwners), models.OutputTypesNFTTransfer, typedOut.GroupID, typedOut.Payload))
			case *nftfx.MintOutput:
				errs.Add(w.avax.InsertOutput(ctx, tx.ID(), outputCount, tx.ID(), xOut(typedOut.OutputOwners), models.OutputTypesNFTMint, typedOut.GroupID, nil))
			case *secp256k1fx.MintOutput:
				errs.Add(w.avax.InsertOutput(ctx, tx.ID(), outputCount, tx.ID(), xOut(typedOut.OutputOwners), models.OutputTypesSECP2556K1Mint, 0, nil))
			case *secp256k1fx.TransferOutput:
				errs.Add(w.avax.InsertOutput(ctx, tx.ID(), outputCount, tx.ID(), typedOut, models.OutputTypesSECP2556K1Transfer, 0, nil))
				if amount, err = avalancheMath.Add64(amount, typedOut.Amount()); err != nil {
					_ = ctx.Job().EventErr("add_to_amount", err)
				}
			default:
				_ = ctx.Job().EventErr("assertion_to_output", errors.New("output is not known"))
			}

			outputCount++
		}
	}

	_, err = ctx.DB().
		InsertInto("avm_assets").
		Pair("id", tx.ID().String()).
		Pair("chain_Id", w.chainID).
		Pair("name", tx.Name).
		Pair("symbol", tx.Symbol).
		Pair("denomination", tx.Denomination).
		Pair("alias", alias).
		Pair("current_supply", amount).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return err
	}
	return nil
}