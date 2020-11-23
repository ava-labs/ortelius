// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/ortelius/stream"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/codec"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"

	"github.com/ava-labs/ortelius/services"
	avaxIndexer "github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

var (
	ChainID = ids.ID{}

	ErrUnknownBlockType = errors.New("unknown block type")
)

type Writer struct {
	chainID     string
	networkID   uint32
	avaxAssetID ids.ID

	codec codec.Codec
	conns *services.Connections
	avax  *avaxIndexer.Writer
}

func NewWriter(conns *services.Connections, networkID uint32, chainID string) (*Writer, error) {
	_, avaxAssetID, err := genesis.Genesis(networkID)
	if err != nil {
		return nil, err
	}

	return &Writer{
		conns:       conns,
		chainID:     chainID,
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		codec:       platformvm.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "pvm-index" }

func (w *Writer) Consume(ctx context.Context, c services.Consumable) error {
	job := w.conns.Stream().NewJob("index")
	sess := w.conns.DB().NewSessionForEventReceiver(job)

	if stream.IndexerTaskEnabled {
		// fire and forget..
		// update the created_at on the state table if we have an earlier date in ctx.Time().
		// which means we need to re-run aggregation calculations from this earlier date.
		_, _ = models.UpdateAvmAssetAggregationLiveStateTimestamp(ctx, sess, time.Unix(c.Timestamp(), 0))
	}

	// Create w tx
	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Consume the tx and commit
	err = w.indexBlock(services.NewConsumerContext(ctx, job, dbTx, c.Timestamp()), c.Body())
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (w *Writer) Bootstrap(ctx context.Context) error {
	job := w.conns.Stream().NewJob("bootstrap")

	genesisBytes, _, err := genesis.Genesis(w.networkID)
	if err != nil {
		return err
	}

	platformGenesis := &platformvm.Genesis{}
	if err := platformvm.GenesisCodec.Unmarshal(genesisBytes, platformGenesis); err != nil {
		return err
	}
	if err = platformGenesis.Initialize(); err != nil {
		return err
	}

	var (
		db   = w.conns.DB().NewSessionForEventReceiver(job)
		errs = wrappers.Errs{}
		cCtx = services.NewConsumerContext(ctx, job, db, int64(platformGenesis.Timestamp))
	)

	for idx, utxo := range platformGenesis.UTXOs {
		select {
		case <-ctx.Done():
			break
		default:
		}

		switch transferOutput := utxo.Out.(type) {
		case *platformvm.StakeableLockOut:
			xOut, ok := transferOutput.TransferableOut.(*secp256k1fx.TransferOutput)
			if !ok {
				return fmt.Errorf("invalid type *secp256k1fx.TransferOutput")
			}
			errs.Add(w.avax.InsertOutput(cCtx, ChainID, uint32(idx), utxo.AssetID(), xOut, models.OutputTypesSECP2556K1Transfer, 0, nil, transferOutput.Locktime, ChainID.String()))
		case *secp256k1fx.TransferOutput:
			errs.Add(w.avax.InsertOutput(cCtx, ChainID, uint32(idx), utxo.AssetID(), transferOutput, models.OutputTypesSECP2556K1Transfer, 0, nil, 0, ChainID.String()))
		default:
			return fmt.Errorf("invalid type %s", reflect.TypeOf(transferOutput))
		}
	}

	for _, tx := range append(platformGenesis.Validators, platformGenesis.Chains...) {
		select {
		case <-ctx.Done():
			break
		default:
		}

		errs.Add(w.indexTransaction(cCtx, ChainID, *tx, true))
	}

	return errs.Err
}

func initializeTx(c codec.Codec, tx platformvm.Tx) error {
	unsignedBytes, err := c.Marshal(&tx.UnsignedTx)
	if err != nil {
		return err
	}
	signedBytes, err := c.Marshal(&tx)
	if err != nil {
		return err
	}
	tx.Initialize(unsignedBytes, signedBytes)
	return nil
}

func (w *Writer) indexBlock(ctx services.ConsumerCtx, blockBytes []byte) error {
	var block platformvm.Block
	if err := w.codec.Unmarshal(blockBytes, &block); err != nil {
		return ctx.Job().EventErr("index_block.unmarshal_block", err)
	}

	errs := wrappers.Errs{}

	switch blk := block.(type) {
	case *platformvm.ProposalBlock:
		errs.Add(
			initializeTx(w.codec, blk.Tx),
			w.indexCommonBlock(ctx, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blk.ID(), blk.Tx, false),
		)
	case *platformvm.StandardBlock:
		errs.Add(w.indexCommonBlock(ctx, models.BlockTypeStandard, blk.CommonBlock, blockBytes))
		for _, tx := range blk.Txs {
			errs.Add(
				initializeTx(w.codec, *tx),
				w.indexTransaction(ctx, blk.ID(), *tx, false),
			)
		}
	case *platformvm.AtomicBlock:
		errs.Add(
			initializeTx(w.codec, blk.Tx),
			w.indexCommonBlock(ctx, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blk.ID(), blk.Tx, false),
		)
	case *platformvm.Abort:
		errs.Add(w.indexCommonBlock(ctx, models.BlockTypeAbort, blk.CommonBlock, blockBytes))
	case *platformvm.Commit:
		errs.Add(w.indexCommonBlock(ctx, models.BlockTypeCommit, blk.CommonBlock, blockBytes))
	default:
		return ctx.Job().EventErr("index_block", ErrUnknownBlockType)
	}

	return errs.Err
}

func (w *Writer) indexCommonBlock(ctx services.ConsumerCtx, blkType models.BlockType, blk platformvm.CommonBlock, blockBytes []byte) error {
	blkID := ids.ID(hashing.ComputeHash256Array(blockBytes))

	if len(blockBytes) > 32000 {
		blockBytes = []byte("")
	}
	_, err := ctx.DB().
		InsertInto("pvm_blocks").
		Pair("id", blkID.String()).
		Pair("chain_id", w.chainID).
		Pair("type", blkType).
		Pair("parent_id", blk.ParentID().String()).
		Pair("created_at", ctx.Time()).
		Pair("serialization", blockBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("pvm_blocks.insert", err)
	}
	if cfg.PerformUpdates {
		_, err = ctx.DB().
			Update("pvm_blocks").
			Set("chain_id", w.chainID).
			Set("type", blkType).
			Set("parent_id", blk.ParentID().String()).
			Set("serialization", blockBytes).
			Where("id = ?", blkID.String()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("pvm_blocks.update", err)
		}
	}

	return nil
}

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, _ ids.ID, tx platformvm.Tx, genesis bool) error {
	var (
		baseTx avax.BaseTx
		typ    models.TransactionType
	)

	var ins []*avax.TransferableInput
	var outs []*avax.TransferableOutput

	outChain := w.chainID
	inChain := w.chainID

	switch castTx := tx.UnsignedTx.(type) {
	case *platformvm.UnsignedAddValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = castTx.Stake
		typ = models.TransactionTypeAddValidator
	case *platformvm.UnsignedAddSubnetValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeAddSubnetValidator
	case *platformvm.UnsignedAddDelegatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = castTx.Stake
		typ = models.TransactionTypeAddDelegator
	case *platformvm.UnsignedCreateSubnetTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateSubnet
	case *platformvm.UnsignedCreateChainTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateChain
	case *platformvm.UnsignedImportTx:
		baseTx = castTx.BaseTx.BaseTx
		ins = castTx.ImportedInputs
		inChain = castTx.SourceChain.String()
		typ = models.TransactionTypePVMImport
	case *platformvm.UnsignedExportTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = castTx.ExportedOutputs
		outChain = castTx.DestinationChain.String()
		typ = models.TransactionTypePVMExport
	case *platformvm.UnsignedAdvanceTimeTx:
		return nil
	case *platformvm.UnsignedRewardValidatorTx:
		return nil
	}

	return w.avax.InsertTransaction(ctx, tx.Bytes(), tx.UnsignedBytes(), &baseTx, tx.Creds, typ, ins, inChain, outs, outChain, 0, genesis)
}
