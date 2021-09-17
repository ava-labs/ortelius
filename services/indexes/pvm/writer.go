// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/idb"
	avaxIndexer "github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/prometheus/client_golang/prometheus"
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
	ctx   *snow.Context
}

func NewWriter(networkID uint32, chainID string) (*Writer, error) {
	_, avaxAssetID, err := genesis.Genesis(networkID, "")
	if err != nil {
		return nil, err
	}

	bcLookup := &ids.Aliaser{}
	bcLookup.Initialize()
	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, err
	}
	if err = bcLookup.Alias(id, "P"); err != nil {
		return nil, err
	}

	ctx := &snow.Context{
		NetworkID:     networkID,
		ChainID:       id,
		Log:           logging.NoLog{},
		Metrics:       prometheus.NewRegistry(),
		BCLookup:      bcLookup,
		EpochDuration: time.Hour,
	}

	return &Writer{
		chainID:     chainID,
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		codec:       platformvm.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID),
		ctx:         ctx,
	}, nil
}

func (*Writer) Name() string { return "pvm-index" }

func (w *Writer) initCtxPtx(p *platformvm.Tx) {
	switch castTx := p.UnsignedTx.(type) {
	case *platformvm.UnsignedAddValidatorTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, utxo := range castTx.Stake {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedAddSubnetValidatorTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedAddDelegatorTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, utxo := range castTx.Stake {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedCreateSubnetTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedCreateChainTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedImportTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, out := range castTx.Outs {
			out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedExportTx:
		for _, utxo := range castTx.UTXOs() {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, out := range castTx.ExportedOutputs {
			out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedAdvanceTimeTx:
		castTx.InitCtx(w.ctx)
	case *platformvm.UnsignedRewardValidatorTx:
		castTx.InitCtx(w.ctx)
	default:
	}
}

func (w *Writer) initCtx(b *platformvm.Block) {
	switch blk := (*b).(type) {
	case *platformvm.ProposalBlock:
		w.initCtxPtx(&blk.Tx)
	case *platformvm.StandardBlock:
		for _, tx := range blk.Txs {
			w.initCtxPtx(tx)
		}
	case *platformvm.AtomicBlock:
		w.initCtxPtx(&blk.Tx)
	case *platformvm.AbortBlock:
	case *platformvm.CommitBlock:
	default:
	}
}

type PtxDataProposerModel struct {
	ID           string    `json:"tx"`
	ParentID     string    `json:"parentID"`
	PChainHeight uint64    `json:"pChainHeight"`
	Proposer     string    `json:"proposer"`
	TimeStamp    time.Time `json:"timeStamp"`
}

func NewPtxDataProposerModel(b block.Block) *PtxDataProposerModel {
	switch properBlockDetail := b.(type) {
	case block.SignedBlock:
		return &PtxDataProposerModel{
			ID:           properBlockDetail.ID().String(),
			ParentID:     properBlockDetail.ParentID().String(),
			PChainHeight: properBlockDetail.PChainHeight(),
			Proposer:     properBlockDetail.Proposer().String(),
			TimeStamp:    properBlockDetail.Timestamp(),
		}
	default:
		return &PtxDataProposerModel{
			ID:           properBlockDetail.ID().String(),
			PChainHeight: 0,
			Proposer:     "",
		}
	}
}

type PtxDataModel struct {
	Tx       *platformvm.Tx        `json:"tx,omitempty"`
	Block    *platformvm.Block     `json:"block,omitempty"`
	Proposer *PtxDataProposerModel `json:"proposer,omitempty"`
}

func (w *Writer) ParseJSON(txBytes []byte) ([]byte, error) {
	parsePlatformTx := func(b []byte) (*PtxDataModel, error) {
		var block platformvm.Block
		_, err := w.codec.Unmarshal(b, &block)
		if err != nil {
			var blockTx platformvm.Tx
			_, err = w.codec.Unmarshal(b, &blockTx)
			if err != nil {
				return nil, err
			}
			w.initCtxPtx(&blockTx)
			return &PtxDataModel{
				Tx: &blockTx,
			}, nil
		}
		w.initCtx(&block)
		return &PtxDataModel{
			Block: &block,
		}, nil
	}
	proposerBlock, err := block.Parse(txBytes)
	if err != nil {
		platformBlock, err := parsePlatformTx(txBytes)
		if err != nil {
			return nil, err
		}
		return json.Marshal(platformBlock)
	}
	platformBlock, err := parsePlatformTx(proposerBlock.Block())
	if err != nil {
		return nil, err
	}
	platformBlock.Proposer = NewPtxDataProposerModel(proposerBlock)
	return json.Marshal(platformBlock)
}

func (w *Writer) ConsumeConsensus(_ context.Context, _ *servicesconn.Connections, _ services.Consumable, _ idb.Persist) error {
	return nil
}

func (w *Writer) Consume(ctx context.Context, conns *servicesconn.Connections, c services.Consumable, persist idb.Persist) error {
	job := conns.StreamDBDedup().NewJob("pvm-index")
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

func (w *Writer) Bootstrap(ctx context.Context, conns *servicesconn.Connections, persist idb.Persist) error {
	job := conns.QuietStream().NewJob("bootstrap")

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
	var pblock platformvm.Block
	var ver uint16
	var err error

	proposerBlock, err := block.Parse(blockBytes)
	if err == nil {
		ver, err = w.codec.Unmarshal(proposerBlock.Block(), &pblock)
		if err != nil {
			return ctx.Job().EventErr("index_block.unmarshal_block", err)
		}
	} else {
		proposerBlock = nil
		ver, err = w.codec.Unmarshal(blockBytes, &pblock)
		if err != nil {
			return ctx.Job().EventErr("index_block.unmarshal_block", err)
		}
	}

	blkID := ids.ID(hashing.ComputeHash256Array(blockBytes))

	if proposerBlock != nil {
		var pvmProposer *idb.PvmProposer
		switch properBlockDetail := proposerBlock.(type) {
		case block.SignedBlock:
			pvmProposer = &idb.PvmProposer{
				ID:           properBlockDetail.ID().String(),
				ParentID:     properBlockDetail.ParentID().String(),
				BlkID:        blkID.String(),
				PChainHeight: properBlockDetail.PChainHeight(),
				Proposer:     properBlockDetail.Proposer().String(),
				TimeStamp:    properBlockDetail.Timestamp(),
				CreatedAt:    ctx.Time(),
			}
		default:
			pvmProposer = &idb.PvmProposer{
				ID:           properBlockDetail.ID().String(),
				ParentID:     properBlockDetail.ParentID().String(),
				BlkID:        blkID.String(),
				PChainHeight: 0,
				Proposer:     "",
				TimeStamp:    ctx.Time(),
				CreatedAt:    ctx.Time(),
			}
		}
		err := ctx.Persist().InsertPvmProposer(ctx.Ctx(), ctx.DB(), pvmProposer, cfg.PerformUpdates)
		if err != nil {
			return err
		}
	}

	errs := wrappers.Errs{}

	switch blk := pblock.(type) {
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
	case *platformvm.AbortBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeAbort, blk.CommonBlock, blockBytes))
	case *platformvm.CommitBlock:
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

	pvmBlocks := &idb.PvmBlocks{
		ID:            blkID.String(),
		ChainID:       w.chainID,
		Type:          blkType,
		ParentID:      blk.Parent().String(),
		Serialization: blockBytes,
		CreatedAt:     ctx.Time(),
		Height:        blk.Height(),
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
		if castTx.RewardsOwner != nil {
			err = w.insertTransactionsRewardsOwners(ctx, castTx.RewardsOwner, baseTx, castTx.Stake)
			if err != nil {
				return err
			}
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
		if castTx.RewardsOwner != nil {
			err = w.insertTransactionsRewardsOwners(ctx, castTx.RewardsOwner, baseTx, castTx.Stake)
			if err != nil {
				return err
			}
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
		rewards := &idb.Rewards{
			ID:                 castTx.ID().String(),
			BlockID:            blkID.String(),
			Txid:               castTx.TxID.String(),
			Shouldprefercommit: castTx.InitiallyPrefersCommit(nil),
			CreatedAt:          ctx.Time(),
		}
		return ctx.Persist().InsertRewards(ctx.Ctx(), ctx.DB(), rewards, cfg.PerformUpdates)
	default:
		return fmt.Errorf("unknown tx type %s", reflect.TypeOf(castTx))
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

func (w *Writer) insertTransactionsRewardsOwners(ctx services.ConsumerCtx, rewardsOwner verify.Verifiable, baseTx avax.BaseTx, stakeOuts []*avax.TransferableOutput) error {
	var err error

	owner, ok := rewardsOwner.(*secp256k1fx.OutputOwners)
	if !ok {
		return fmt.Errorf("rewards owner %v", reflect.TypeOf(rewardsOwner))
	}

	// Ingest each Output Address
	for ipos, addr := range owner.Addresses() {
		addrid := ids.ShortID{}
		copy(addrid[:], addr)
		txRewardsOwnerAddress := &idb.TransactionsRewardsOwnersAddress{
			ID:          baseTx.ID().String(),
			Address:     addrid.String(),
			OutputIndex: uint32(ipos),
			UpdatedAt:   time.Now().UTC(),
		}

		err = ctx.Persist().InsertTransactionsRewardsOwnersAddress(ctx.Ctx(), ctx.DB(), txRewardsOwnerAddress, cfg.PerformUpdates)
		if err != nil {
			return err
		}
	}

	// write out outputs in the len(outs) and len(outs)+1 positions to identify these rewards
	outcnt := len(baseTx.Outs) + len(stakeOuts)
	for ipos := outcnt; ipos < outcnt+2; ipos++ {
		outputID := baseTx.ID().Prefix(uint64(ipos))

		txRewardsOutputs := &idb.TransactionsRewardsOwnersOutputs{
			ID:            outputID.String(),
			TransactionID: baseTx.ID().String(),
			OutputIndex:   uint32(ipos),
			CreatedAt:     ctx.Time(),
		}

		err = ctx.Persist().InsertTransactionsRewardsOwnersOutputs(ctx.Ctx(), ctx.DB(), txRewardsOutputs, cfg.PerformUpdates)
		if err != nil {
			return err
		}
	}

	txRewardsOwner := &idb.TransactionsRewardsOwners{
		ID:        baseTx.ID().String(),
		ChainID:   w.chainID,
		Threshold: owner.Threshold,
		Locktime:  owner.Locktime,
		CreatedAt: ctx.Time(),
	}

	return ctx.Persist().InsertTransactionsRewardsOwners(ctx.Ctx(), ctx.DB(), txRewardsOwner, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionValidator(ctx services.ConsumerCtx, txID ids.ID, validator platformvm.Validator) error {
	transactionsValidator := &idb.TransactionsValidator{
		ID:        txID.String(),
		NodeID:    validator.NodeID.String(),
		Start:     validator.Start,
		End:       validator.End,
		CreatedAt: ctx.Time(),
	}
	return ctx.Persist().InsertTransactionsValidator(ctx.Ctx(), ctx.DB(), transactionsValidator, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionBlock(ctx services.ConsumerCtx, txID ids.ID, blkTxID ids.ID) error {
	transactionsBlock := &idb.TransactionsBlock{
		ID:        txID.String(),
		TxBlockID: blkTxID.String(),
		CreatedAt: ctx.Time(),
	}
	return ctx.Persist().InsertTransactionsBlock(ctx.Ctx(), ctx.DB(), transactionsBlock, cfg.PerformUpdates)
}
