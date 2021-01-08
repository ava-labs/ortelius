// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/gocraft/health"

	"github.com/ava-labs/avalanchego/database"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/avalanchego/snow/engine/avalanche/state"

	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/gocraft/dbr/v2"
	"github.com/palantir/stacktrace"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

var (
	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")
)

type Writer struct {
	chainID     string
	networkID   uint32
	avaxAssetID ids.ID

	codec codec.Manager
	avax  *avax.Writer
	conns *services.Connections
	vm    *avm.VM
	ctx   *snow.Context
	db    database.Database
}

func NewWriter(conns *services.Connections, networkID uint32, chainID string) (*Writer, error) {
	vm, ctx, avmCodec, db, err := newAVMCodec(networkID, chainID)
	if err != nil {
		return nil, err
	}

	_, avaxAssetID, err := genesis.Genesis(networkID)
	if err != nil {
		return nil, err
	}

	return &Writer{
		conns:       conns,
		chainID:     chainID,
		db:          db,
		vm:          vm,
		ctx:         ctx,
		codec:       avmCodec,
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		avax:        avax.NewWriter(chainID, avaxAssetID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "avm-index" }

func (w *Writer) Bootstrap(ctx context.Context, persist services.Persist) error {
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
	_, err = platformvm.GenesisCodec.Unmarshal(platformGenesisBytes, platformGenesis)
	if err != nil {
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

		if createChainTx.VMID != avm.ID {
			continue
		}

		dbSess := w.conns.DB().NewSessionForEventReceiver(job)
		cCtx := services.NewConsumerContext(ctx, job, dbSess, int64(platformGenesis.Timestamp), persist)
		return w.insertGenesis(cCtx, createChainTx.GenesisData)
	}

	return nil
}

func (w *Writer) ConsumeConsensus(ctx context.Context, c services.Consumable, persist services.Persist) error {
	noopdb := &utils.NoopDatabase{}

	serializer := &state.Serializer{}
	serializer.Initialize(w.ctx, w.vm, noopdb)

	vertex, err := serializer.Parse(c.Body())
	if err != nil {
		return err
	}

	epoch, err := vertex.Epoch()
	if err != nil {
		return err
	}

	vertexTxs, err := vertex.Txs()
	if err != nil {
		return err
	}

	var (
		job  = w.conns.Stream().NewJob("index-consensus")
		sess = w.conns.DB().NewSessionForEventReceiver(job)
	)
	job.KeyValue("id", c.ID())
	job.KeyValue("chain_id", c.ChainID())

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	cCtx := services.NewConsumerContext(ctx, job, dbTx, c.Timestamp(), persist)

	for _, vtx := range vertexTxs {
		switch txt := vtx.(type) {
		case *avm.UniqueTx:

			txID := txt.Tx.ID()

			transactionsEpoch := &services.TransactionsEpoch{
				ID:        txID.String(),
				Epoch:     epoch,
				VertexID:  vertex.ID().String(),
				CreatedAt: cCtx.Time(),
			}
			err = cCtx.Persist().InsertTransactionsEpoch(cCtx.Ctx(), cCtx.DB(), transactionsEpoch, cfg.PerformUpdates)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unable to determine vertex transaction %s", reflect.TypeOf(txt))
		}
	}

	if err = dbTx.Commit(); err != nil {
		return stacktrace.Propagate(err, "Failed to commit database tx")
	}

	return nil
}

func (w *Writer) Consume(ctx context.Context, i services.Consumable, persist services.Persist) error {
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
	err = w.insertTx(services.NewConsumerContext(ctx, job, dbTx, i.Timestamp(), persist), i.Body())
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
	ver, err := w.codec.Unmarshal(genesisBytes, avmGenesis)
	if err != nil {
		return stacktrace.Propagate(err, "Failed to parse avm genesis bytes")
	}

	for i, tx := range avmGenesis.Txs {
		txBytes, err := w.codec.Marshal(ver, &avm.Tx{UnsignedTx: &tx.CreateAssetTx})
		if err != nil {
			return stacktrace.Propagate(err, "Failed to serialize wrapped avm genesis tx %d", i)
		}
		unsignedBytes, err := w.codec.Marshal(ver, &tx.CreateAssetTx)
		if err != nil {
			return err
		}

		tx.Initialize(unsignedBytes, txBytes)

		if err = w.insertCreateAssetTx(ctx, txBytes, &tx.CreateAssetTx, nil, tx.Alias, true); err != nil {
			return stacktrace.Propagate(err, "Failed to index avm genesis tx %d", i)
		}
	}
	return nil
}

func (w *Writer) insertTx(ctx services.ConsumerCtx, txBytes []byte) error {
	_, tx, err := parseTx(w.codec, txBytes)
	if err != nil {
		return err
	}

	// Finish processing with a type-specific ingestion routine
	switch castTx := tx.UnsignedTx.(type) {
	case *avm.CreateAssetTx:
		return w.insertCreateAssetTx(ctx, txBytes, castTx, tx.Credentials(), "", false)
	case *avm.OperationTx:
		return w.insertOperationTx(ctx, txBytes, castTx, tx.Credentials(), false)
	case *avm.ImportTx:
		return w.avax.InsertTransaction(
			ctx,
			txBytes,
			tx.UnsignedBytes(),
			&castTx.BaseTx.BaseTx,
			tx.Credentials(),
			models.TransactionTypeAVMImport,
			&avax.AddInsContainer{Ins: castTx.ImportedIns, ChainID: castTx.SourceChain.String()},
			nil,
			0,
			false,
		)
	case *avm.ExportTx:
		return w.avax.InsertTransaction(
			ctx,
			txBytes,
			tx.UnsignedBytes(),
			&castTx.BaseTx.BaseTx,
			tx.Credentials(),
			models.TransactionTypeAVMExport,
			nil,
			&avax.AddOutsContainer{Outs: castTx.ExportedOuts, ChainID: castTx.DestinationChain.String()},
			0,
			false,
		)
	case *avm.BaseTx:
		return w.avax.InsertTransaction(
			ctx,
			txBytes,
			tx.UnsignedBytes(),
			&castTx.BaseTx,
			tx.Credentials(),
			models.TransactionTypeBase,
			nil,
			nil,
			0,
			false,
		)
	default:
		return fmt.Errorf("unknown tx type %s", reflect.TypeOf(castTx))
	}
}

func (w *Writer) insertOperationTx(
	ctx services.ConsumerCtx,
	txBytes []byte,
	tx *avm.OperationTx,
	creds []verify.Verifiable,
	genesis bool,
) error {
	var (
		err         error
		outputCount uint32
		amount      uint64
		totalout    uint64 = 0
	)

	// we must process the Outs to get the outputCount updated
	// before working on the Ops
	// the outs get processed again in InsertTransaction
	for _, out := range tx.Outs {
		_, err = w.avax.InsertTransactionOuts(outputCount, ctx, 0, out, tx.ID(), w.chainID, false)
		if err != nil {
			return err
		}
		outputCount++
	}

	for _, txOps := range tx.Ops {
		for _, out := range txOps.Op.Outs() {
			amount, totalout, err = w.avax.ProcessStateOut(ctx, out, tx.ID(), outputCount, txOps.AssetID(), amount, totalout, w.chainID, false)
			if err != nil {
				return err
			}
			outputCount++
		}
	}

	return w.avax.InsertTransaction(ctx, txBytes, tx.UnsignedBytes(), &tx.BaseTx.BaseTx, creds, models.TransactionTypeOperation, nil, nil, totalout, genesis)
}

func (w *Writer) insertCreateAssetTx(ctx services.ConsumerCtx, txBytes []byte, tx *avm.CreateAssetTx, creds []verify.Verifiable, alias string, genesis bool) error {
	var (
		err         error
		outputCount uint32
		amount      uint64
		totalout    uint64 = 0
	)

	// we must process the Outs to get the outputCount updated
	// before working on the states
	// the outs get processed again in InsertTransaction
	for _, out := range tx.Outs {
		_, err = w.avax.InsertTransactionOuts(outputCount, ctx, 0, out, tx.ID(), w.chainID, false)
		if err != nil {
			return err
		}
		outputCount++
	}

	for _, state := range tx.States {
		for _, out := range state.Outs {
			amount, totalout, err = w.avax.ProcessStateOut(ctx, out, tx.ID(), outputCount, tx.ID(), amount, totalout, w.chainID, false)
			if err != nil {
				return err
			}
			outputCount++
		}
	}

	asset := &services.Assets{
		ID:            tx.ID().String(),
		ChainID:       w.chainID,
		Name:          tx.Name,
		Symbol:        tx.Symbol,
		Denomination:  tx.Denomination,
		Alias:         alias,
		CurrentSupply: amount,
		CreatedAt:     ctx.Time(),
	}

	err = ctx.Persist().InsertAssets(ctx.Ctx(), ctx.DB(), asset, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	return w.avax.InsertTransaction(ctx, txBytes, tx.UnsignedBytes(), &tx.BaseTx.BaseTx, creds, models.TransactionTypeCreateAsset, nil, nil, totalout, genesis)
}
