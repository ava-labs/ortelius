// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ava-labs/avalanchego/vms/secp256k1fx"

	"github.com/ava-labs/avalanchego/vms/components/verify"

	"github.com/ava-labs/ortelius/cfg"

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
	MaxSerializationLen = (16 * 1024 * 1024) - 1

	ChainID = ids.ID{}

	ErrUnknownBlockType = errors.New("unknown block type")
)

type Writer struct {
	chainID     string
	networkID   uint32
	avaxAssetID ids.ID

	codec codec.Manager
	avax  *avaxIndexer.Writer
}

func NewWriter(networkID uint32, chainID string) (*Writer, error) {
	_, avaxAssetID, err := genesis.Genesis(networkID, "")
	if err != nil {
		return nil, err
	}

	return &Writer{
		chainID:     chainID,
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		codec:       platformvm.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID),
	}, nil
}

func (*Writer) Name() string { return "pvm-index" }

func (w *Writer) ParseJSON(txBytes []byte) ([]byte, error) {
	var blockTx platformvm.Tx
	_, err := w.codec.Unmarshal(txBytes, &blockTx)
	if err != nil {
		var block platformvm.Block
		_, err := w.codec.Unmarshal(txBytes, &block)
		if err != nil {
			return nil, err
		}
		return json.Marshal(&block)
	}
	return json.Marshal(&blockTx)
}

func (w *Writer) ConsumeConsensus(_ context.Context, _ *services.Connections, _ services.Consumable, _ services.Persist) error {
	return nil
}

func (w *Writer) Consume(ctx context.Context, conns *services.Connections, c services.Consumable, persist services.Persist) error {
	job := conns.Stream().NewJob("pvm-index")
	sess := conns.DB().NewSessionForEventReceiver(job)

	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Consume the tx and commit
	err = w.indexBlock(services.NewConsumerContext(ctx, job, dbTx, c.Timestamp(), c.Nanosecond(), persist), c.Body())
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (w *Writer) Bootstrap(ctx context.Context, conns *services.Connections, persist services.Persist) error {
	job := conns.Stream().NewJob("bootstrap")

	genesisBytes, _, err := genesis.Genesis(w.networkID, "")
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
		db   = conns.DB().NewSessionForEventReceiver(job)
		errs = wrappers.Errs{}
		cCtx = services.NewConsumerContext(ctx, job, db, int64(platformGenesis.Timestamp), 0, persist)
	)

	for idx, utxo := range platformGenesis.UTXOs {
		select {
		case <-ctx.Done():
			break
		default:
		}

		_, _, err = w.avax.ProcessStateOut(
			cCtx,
			utxo.Out,
			ChainID,
			uint32(idx),
			utxo.AssetID(),
			0,
			0,
			w.chainID,
			false,
			true,
		)
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

func (w *Writer) indexCommonBlock(
	ctx services.ConsumerCtx,
	blkID ids.ID,
	blkType models.BlockType,
	blk platformvm.CommonBlock,
	blockBytes []byte,
) error {
	if len(blockBytes) > MaxSerializationLen {
		blockBytes = []byte("")
	}

	pvmBlocks := &services.PvmBlocks{
		ID:            blkID.String(),
		ChainID:       w.chainID,
		Type:          blkType,
		ParentID:      blk.ParentID().String(),
		Serialization: blockBytes,
		CreatedAt:     ctx.Time(),
	}
	return ctx.Persist().InsertPvmBlocks(ctx.Ctx(), ctx.DB(), pvmBlocks, cfg.PerformUpdates)
}

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, blkID ids.ID, tx platformvm.Tx, genesis bool) error {
	var (
		baseTx avax.BaseTx
		typ    models.TransactionType
	)

	var ins *avaxIndexer.AddInsContainer
	var outs *avaxIndexer.AddOutsContainer
	var rewardsOwner verify.Verifiable
	var err error
	switch castTx := tx.UnsignedTx.(type) {
	case *platformvm.UnsignedAddValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.Stake,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddValidator
		err = w.InsertTransactionValidator(ctx, baseTx.ID(), castTx.Validator)
		if err != nil {
			return err
		}
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
		rewardsOwner = castTx.RewardsOwner
	case *platformvm.UnsignedAddSubnetValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeAddSubnetValidator
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedAddDelegatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.Stake,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddDelegator
		err = w.InsertTransactionValidator(ctx, baseTx.ID(), castTx.Validator)
		if err != nil {
			return err
		}
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
		rewardsOwner = castTx.RewardsOwner
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
		ins = &avaxIndexer.AddInsContainer{
			Ins:     castTx.ImportedInputs,
			ChainID: castTx.SourceChain.String(),
		}
		typ = models.TransactionTypePVMImport
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedExportTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.ExportedOutputs,
			ChainID: castTx.DestinationChain.String(),
		}
		typ = models.TransactionTypePVMExport
		err = w.InsertTransactionBlock(ctx, baseTx.ID(), blkID)
		if err != nil {
			return err
		}
	case *platformvm.UnsignedAdvanceTimeTx:
		return nil
	case *platformvm.UnsignedRewardValidatorTx:
		rewards := &services.Rewards{
			ID:                 castTx.ID().String(),
			BlockID:            blkID.String(),
			Txid:               castTx.TxID.String(),
			Shouldprefercommit: castTx.InitiallyPrefersCommit(nil),
			CreatedAt:          ctx.Time(),
		}
		return ctx.Persist().InsertRewards(ctx.Ctx(), ctx.DB(), rewards, cfg.PerformUpdates)
	}

	if rewardsOwner != nil {
		owner, ok := rewardsOwner.(*secp256k1fx.OutputOwners)
		if !ok {
			return fmt.Errorf("rewards owner not secp256k1fx.OutputOwners")
		}
		outputsRewards := &services.OutputsRewards{
			ID:        baseTx.ID().String(),
			ChainID:   w.chainID,
			Threshold: owner.Threshold,
			Locktime:  owner.Locktime,
			CreatedAt: ctx.Time(),
		}
		err = ctx.Persist().InsertOutputsRewards(ctx.Ctx(), ctx.DB(), outputsRewards, cfg.PerformUpdates)
		if err != nil {
			return err
		}

		// Ingest each Output Address
		for ipos, addr := range owner.Addresses() {
			addrBytes := [20]byte{}
			copy(addrBytes[:], addr)
			addrid := ids.ShortID(addrBytes)
			outputsRewardsAddress := &services.OutputsRewardsAddress{
				ID:          baseTx.ID().String(),
				Address:     addrid.String(),
				OutputIndex: uint32(ipos),
			}
			err = ctx.Persist().InsertOutputsRewardsAddress(ctx.Ctx(), ctx.DB(), outputsRewardsAddress, cfg.PerformUpdates)
			if err != nil {
				return err
			}
		}
	}

	return w.avax.InsertTransaction(
		ctx,
		tx.Bytes(),
		tx.UnsignedBytes(),
		&baseTx,
		tx.Creds,
		typ,
		ins,
		outs,
		0,
		genesis,
	)
}

func (w *Writer) InsertTransactionValidator(ctx services.ConsumerCtx, txID ids.ID, validator platformvm.Validator) error {
	transactionsValidator := &services.TransactionsValidator{
		ID:        txID.String(),
		NodeID:    validator.NodeID.String(),
		Start:     validator.Start,
		End:       validator.End,
		CreatedAt: ctx.Time(),
	}
	return ctx.Persist().InsertTransactionsValidator(ctx.Ctx(), ctx.DB(), transactionsValidator, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionBlock(ctx services.ConsumerCtx, txID ids.ID, blkTxID ids.ID) error {
	transactionsBlock := &services.TransactionsBlock{
		ID:        txID.String(),
		TxBlockID: blkTxID.String(),
		CreatedAt: ctx.Time(),
	}
	return ctx.Persist().InsertTransactionsBlock(ctx.Ctx(), ctx.DB(), transactionsBlock, cfg.PerformUpdates)
}
