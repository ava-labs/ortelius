// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"errors"
	"time"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/ortelius/stream"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/platformvm"
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

	codec codec.Manager
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

func (w *Writer) ConsumeConsensus(_ context.Context, c services.Consumable) error {
	return nil
}

func (w *Writer) Consume(ctx context.Context, c services.Consumable) error {
	job := w.conns.Stream().NewJob("index")
	sess := w.conns.DB().NewSessionForEventReceiver(job)

	if stream.IndexerTaskEnabled {
		// fire and forget..
		// update the created_at on the state table if we have an earlier date in ctx.Time().
		// which means we need to re-run aggregation calculations from this earlier date.
		_, _ = models.UpdateAvmAssetAggregationLiveStateTimestamp(ctx, sess, time.Unix(c.Timestamp(), 0))
	}

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
	_, err = platformvm.GenesisCodec.Unmarshal(genesisBytes, platformGenesis)
	if err != nil {
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

		_, _, err = w.avax.ProcessStateOut(cCtx, utxo.Out, ChainID, uint32(idx), utxo.AssetID(), 0, 0, w.chainID, false)
		if err != nil {
			return err
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

func initializeTx(version uint16, c codec.Manager, tx platformvm.Tx) error {
	unsignedBytes, err := c.Marshal(version, &tx.UnsignedTx)
	if err != nil {
		return err
	}
	signedBytes, err := c.Marshal(version, &tx)
	if err != nil {
		return err
	}
	tx.Initialize(unsignedBytes, signedBytes)
	return nil
}

func (w *Writer) indexBlock(ctx services.ConsumerCtx, blockBytes []byte) error {
	var block platformvm.Block
	ver, err := w.codec.Unmarshal(blockBytes, &block)
	if err != nil {
		return ctx.Job().EventErr("index_block.unmarshal_block", err)
	}
	blkID := ids.ID(hashing.ComputeHash256Array(blockBytes))

	errs := wrappers.Errs{}

	switch blk := block.(type) {
	case *platformvm.ProposalBlock:
		errs.Add(
			initializeTx(ver, w.codec, blk.Tx),
			w.indexCommonBlock(ctx, blkID, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blkID, blk.Tx, false),
		)
	case *platformvm.StandardBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, blockBytes))
		for _, tx := range blk.Txs {
			errs.Add(
				initializeTx(ver, w.codec, *tx),
				w.indexTransaction(ctx, blkID, *tx, false),
			)
		}
	case *platformvm.AtomicBlock:
		errs.Add(
			initializeTx(ver, w.codec, blk.Tx),
			w.indexCommonBlock(ctx, blkID, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blkID, blk.Tx, false),
		)
	case *platformvm.Abort:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeAbort, blk.CommonBlock, blockBytes))
	case *platformvm.Commit:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeCommit, blk.CommonBlock, blockBytes))
	default:
		return ctx.Job().EventErr("index_block", ErrUnknownBlockType)
	}

	return errs.Err
}

func (w *Writer) indexCommonBlock(ctx services.ConsumerCtx, blkID ids.ID, blkType models.BlockType, blk platformvm.CommonBlock, blockBytes []byte) error {
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

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, blkID ids.ID, tx platformvm.Tx, genesis bool) error {
	var (
		baseTx avax.BaseTx
		typ    models.TransactionType
	)

	var ins avaxIndexer.AddInsContainer
	var outs avaxIndexer.AddOutsContainer

	var err error
	switch castTx := tx.UnsignedTx.(type) {
	case *platformvm.UnsignedAddValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs.Outs = castTx.Stake
		outs.Stake = true
		outs.ChainID = w.chainID
		typ = models.TransactionTypeAddValidator
		err = w.InsertTransactionValidator(ctx, baseTx.ID(), castTx.Validator)
		if err != nil {
			return err
		}
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedAddSubnetValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeAddSubnetValidator
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedAddDelegatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs.Outs = castTx.Stake
		outs.Stake = true
		outs.ChainID = w.chainID
		typ = models.TransactionTypeAddDelegator
		err = w.InsertTransactionValidator(ctx, baseTx.ID(), castTx.Validator)
		if err != nil {
			return err
		}
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedCreateSubnetTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateSubnet
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedCreateChainTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateChain
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedImportTx:
		baseTx = castTx.BaseTx.BaseTx
		ins.Ins = castTx.ImportedInputs
		ins.ChainID = castTx.SourceChain.String()
		typ = models.TransactionTypePVMImport
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedExportTx:
		baseTx = castTx.BaseTx.BaseTx
		outs.Outs = castTx.ExportedOutputs
		outs.ChainID = castTx.DestinationChain.String()
		typ = models.TransactionTypePVMExport
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedAdvanceTimeTx:
		return nil
	case *platformvm.UnsignedRewardValidatorTx:
		_, err := ctx.DB().
			InsertInto("rewards").
			Pair("id", castTx.ID().String()).
			Pair("block_id", blkID.String()).
			Pair("txid", castTx.TxID.String()).
			Pair("shouldprefercommit", castTx.InitiallyPrefersCommit(nil)).
			Pair("created_at", ctx.Time()).
			ExecContext(ctx.Ctx())
		if err != nil && !db.ErrIsDuplicateEntryError(err) {
			return ctx.Job().EventErr("rewards.insert", err)
		}
		if cfg.PerformUpdates {
			_, err := ctx.DB().
				Update("rewards").
				Set("block_id", blkID.String()).
				Set("txid", castTx.TxID.String()).
				Set("shouldprefercommit", castTx.InitiallyPrefersCommit(nil)).
				Where("id = ?", castTx.ID().String()).
				ExecContext(ctx.Ctx())
			if err != nil {
				return ctx.Job().EventErr("rewards.update", err)
			}
		}
		return nil
	}

	return w.avax.InsertTransaction(ctx, tx.Bytes(), tx.UnsignedBytes(), &baseTx, tx.Creds, typ, &ins, &outs, 0, genesis)
}

func (w *Writer) InsertTransactionValidator(ctx services.ConsumerCtx, txID ids.ID, validator platformvm.Validator) error {
	_, err := ctx.DB().
		InsertInto("transactions_validator").
		Pair("id", txID.String()).
		Pair("node_id", validator.NodeID.String()).
		Pair("start", validator.Start).
		Pair("end", validator.End).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("transactions_validator.insert", err)
	}
	if cfg.PerformUpdates {
		_, err := ctx.DB().
			Update("transactions_validator").
			Set("node_id", validator.NodeID.String()).
			Set("start", validator.Start).
			Set("end", validator.End).
			Where("id = ?", txID.String()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("transactions_validator.update", err)
		}
	}
	return nil
}

func (w *Writer) InsertTransactionBlock(ctx services.ConsumerCtx, txID ids.ID, blkTxID ids.ID) error {
	_, err := ctx.DB().
		InsertInto("transactions_block").
		Pair("id", txID.String()).
		Pair("tx_block_id", blkTxID.String()).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("transactions_block.insert", err)
	}
	if cfg.PerformUpdates {
		_, err := ctx.DB().
			Update("transactions_block").
			Set("tx_block_id", blkTxID.String()).
			Where("id = ?", txID.String()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("transactions_block.update", err)
		}
	}
	return nil
}
