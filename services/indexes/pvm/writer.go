// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ava-labs/avalanchego/api/metrics"
	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/platformvm/blocks"
	p_genesis "github.com/ava-labs/avalanchego/vms/platformvm/genesis"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
	"github.com/ava-labs/avalanchego/vms/platformvm/validator"
	"github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services"
	avaxIndexer "github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/utils"
	"github.com/palantir/stacktrace"
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
	_, avaxAssetID, err := genesis.FromConfig(genesis.GetConfig(networkID))
	if err != nil {
		return nil, err
	}

	bcLookup := ids.NewAliaser()
	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, err
	}
	if err = bcLookup.Alias(id, "P"); err != nil {
		return nil, err
	}

	ctx := &snow.Context{
		NetworkID: networkID,
		ChainID:   id,
		Log:       logging.NoLog{},
		Metrics:   metrics.NewOptionalGatherer(),
		BCLookup:  bcLookup,
	}

	return &Writer{
		chainID:     chainID,
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		codec:       txs.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID),
		ctx:         ctx,
	}, nil
}

func (*Writer) Name() string { return "pvm-index" }

func (w *Writer) initCtxPtx(p *txs.Tx) {
	switch castTx := p.Unsigned.(type) {
	case *txs.AddValidatorTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, utxo := range castTx.StakeOuts {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.AddSubnetValidatorTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.AddDelegatorTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, utxo := range castTx.StakeOuts {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.CreateSubnetTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.CreateChainTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.ImportTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, out := range castTx.Outs {
			out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.ExportTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, out := range castTx.ExportedOutputs {
			out.InitCtx(w.ctx)
		}
		castTx.InitCtx(w.ctx)
	case *txs.AdvanceTimeTx:
		castTx.InitCtx(w.ctx)
	case *txs.RewardValidatorTx:
		castTx.InitCtx(w.ctx)
	default:
	}
}

func (w *Writer) initCtx(b blocks.Block) {
	switch blk := b.(type) {
	case *blocks.ApricotProposalBlock:
		w.initCtxPtx(blk.Tx)
	case *blocks.ApricotStandardBlock:
		for _, tx := range blk.Transactions {
			w.initCtxPtx(tx)
		}
	case *blocks.ApricotAtomicBlock:
		w.initCtxPtx(blk.Tx)
	case *blocks.BlueberryProposalBlock:
		for _, tx := range blk.Transactions {
			w.initCtxPtx(tx)
		}
	case *blocks.BlueberryStandardBlock:
		for _, tx := range blk.Transactions {
			w.initCtxPtx(tx)
		}
	case *blocks.BlueberryAbortBlock:
	case *blocks.BlueberryCommitBlock:
	case *blocks.ApricotAbortBlock:
	case *blocks.ApricotCommitBlock:
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
	Tx           *txs.Tx               `json:"tx,omitempty"`
	TxType       *string               `json:"txType,omitempty"`
	Block        *blocks.Block         `json:"block,omitempty"`
	BlockID      *string               `json:"blockID,omitempty"`
	BlockType    *string               `json:"blockType,omitempty"`
	Proposer     *PtxDataProposerModel `json:"proposer,omitempty"`
	ProposerType *string               `json:"proposerType,omitempty"`
}

func (w *Writer) ParseJSON(txBytes []byte) ([]byte, error) {
	parsePlatformTx := func(b []byte) (*PtxDataModel, error) {
		var block blocks.Block
		_, err := w.codec.Unmarshal(b, &block)
		if err != nil {
			var blockTx txs.Tx
			_, err = w.codec.Unmarshal(b, &blockTx)
			if err != nil {
				return nil, err
			}
			w.initCtxPtx(&blockTx)
			txtype := reflect.TypeOf(&blockTx)
			txtypeS := txtype.String()
			return &PtxDataModel{
				Tx:     &blockTx,
				TxType: &txtypeS,
			}, nil
		}
		w.initCtx(block)
		blkID := ids.ID(hashing.ComputeHash256Array(b))
		blkIDS := blkID.String()
		btype := reflect.TypeOf(block)
		btypeS := btype.String()
		return &PtxDataModel{
			BlockID:   &blkIDS,
			Block:     &block,
			BlockType: &btypeS,
		}, nil
	}
	proposerBlock, _, err := block.Parse(txBytes) // double check if the blueberry bool is relevant
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
	pbtype := reflect.TypeOf(proposerBlock)
	pbtypeS := pbtype.String()
	platformBlock.ProposerType = &pbtypeS
	return json.Marshal(platformBlock)
}

func (w *Writer) ConsumeConsensus(_ context.Context, _ *utils.Connections, _ services.Consumable, _ db.Persist) error {
	return nil
}

func (w *Writer) Consume(ctx context.Context, conns *utils.Connections, c services.Consumable, persist db.Persist) error {
	job := conns.Stream().NewJob("pvm-index")
	sess := conns.DB().NewSessionForEventReceiver(job)

	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Consume the tx and commit
	err = w.indexBlock(services.NewConsumerContext(ctx, dbTx, c.Timestamp(), c.Nanosecond(), persist), c.Body())
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (w *Writer) Bootstrap(ctx context.Context, conns *utils.Connections, persist db.Persist) error {
	genesisBytes, _, err := genesis.FromConfig(genesis.GetConfig(w.networkID))
	if err != nil {
		return err
	}
	platformGenesis, err := p_genesis.Parse(genesisBytes)
	if err != nil {
		return err
	}

	var (
		job  = conns.Stream().NewJob("bootstrap")
		db   = conns.DB().NewSessionForEventReceiver(job)
		errs = wrappers.Errs{}
		cCtx = services.NewConsumerContext(ctx, db, int64(platformGenesis.Timestamp), 0, persist)
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

func initializeTx(version uint16, c codec.Manager, tx *txs.Tx) error {
	unsignedBytes, err := c.Marshal(version, &tx.Unsigned)
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

func (w *Writer) indexBlock(ctx services.ConsumerCtx, proposerblockBytes []byte) error {
	var pblock blocks.Block
	var ver uint16
	var err error

	proposerBlock, _, err := block.Parse(proposerblockBytes)
	blockBytes := proposerblockBytes
	if err == nil {
		ver, err = w.codec.Unmarshal(proposerBlock.Block(), &pblock)
		if err != nil {
			return stacktrace.Propagate(err, "proposer bytes")
		}
		blockBytes = append([]byte{}, proposerBlock.Block()...)
	} else {
		proposerBlock = nil
		ver, err = w.codec.Unmarshal(blockBytes, &pblock)
		if err != nil {
			return stacktrace.Propagate(err, "block bytes")
		}
	}

	blkID := ids.ID(hashing.ComputeHash256Array(blockBytes))

	if proposerBlock != nil {
		proposerBlkID := ids.ID(hashing.ComputeHash256Array(proposerblockBytes))
		var pvmProposer *db.PvmProposer
		switch properBlockDetail := proposerBlock.(type) {
		case block.SignedBlock:
			pvmProposer = &db.PvmProposer{
				ID:            properBlockDetail.ID().String(),
				ParentID:      properBlockDetail.ParentID().String(),
				BlkID:         blkID.String(),
				ProposerBlkID: proposerBlkID.String(),
				PChainHeight:  properBlockDetail.PChainHeight(),
				Proposer:      properBlockDetail.Proposer().String(),
				TimeStamp:     properBlockDetail.Timestamp(),
				CreatedAt:     ctx.Time(),
			}
		default:
			pvmProposer = &db.PvmProposer{
				ID:            properBlockDetail.ID().String(),
				ParentID:      properBlockDetail.ParentID().String(),
				BlkID:         blkID.String(),
				ProposerBlkID: proposerBlkID.String(),
				PChainHeight:  0,
				Proposer:      "",
				TimeStamp:     ctx.Time(),
				CreatedAt:     ctx.Time(),
			}
		}
		err := ctx.Persist().InsertPvmProposer(ctx.Ctx(), ctx.DB(), pvmProposer, cfg.PerformUpdates)
		if err != nil {
			return err
		}
	}

	errs := wrappers.Errs{}

	switch blk := pblock.(type) {
	case *blocks.ApricotProposalBlock:
		errs.Add(
			initializeTx(ver, w.codec, blk.Tx),
			w.indexCommonBlock(ctx, blkID, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blkID, *blk.Tx, false),
		)
	case *blocks.ApricotStandardBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, blockBytes))
		for _, tx := range blk.Transactions {
			errs.Add(
				initializeTx(ver, w.codec, tx),
				w.indexTransaction(ctx, blkID, *tx, false),
			)
		}
	case *blocks.ApricotAtomicBlock:
		errs.Add(
			initializeTx(ver, w.codec, blk.Tx),
			w.indexCommonBlock(ctx, blkID, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blkID, *blk.Tx, false),
		)
	case *blocks.ApricotAbortBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeAbort, blk.CommonBlock, blockBytes))
	case *blocks.ApricotCommitBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeCommit, blk.CommonBlock, blockBytes))
	case *blocks.BlueberryProposalBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, blockBytes))
		for _, tx := range blk.Transactions {
			errs.Add(
				initializeTx(ver, w.codec, tx),
				w.indexTransaction(ctx, blkID, *tx, false),
			)
		}
	case *blocks.BlueberryStandardBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, blockBytes))
		for _, tx := range blk.Transactions {
			errs.Add(
				initializeTx(ver, w.codec, tx),
				w.indexTransaction(ctx, blkID, *tx, false),
			)
		}
	case *blocks.BlueberryAbortBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeAbort, blk.CommonBlock, blockBytes))
	case *blocks.BlueberryCommitBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeCommit, blk.CommonBlock, blockBytes))
	default:
		return fmt.Errorf("unknown type %s", reflect.TypeOf(pblock))
	}

	return errs.Err
}

func (w *Writer) indexCommonBlock(
	ctx services.ConsumerCtx,
	blkID ids.ID,
	blkType models.BlockType,
	blk blocks.CommonBlock,
	blockBytes []byte,
) error {
	if len(blockBytes) > MaxSerializationLen {
		blockBytes = []byte("")
	}

	pvmBlocks := &db.PvmBlocks{
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

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, blkID ids.ID, tx txs.Tx, genesis bool) error {
	var (
		baseTx avax.BaseTx
		typ    models.TransactionType
	)

	var ins *avaxIndexer.AddInsContainer
	var outs *avaxIndexer.AddOutsContainer

	var err error
	switch castTx := tx.Unsigned.(type) {
	case *txs.AddValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.StakeOuts,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddValidator
		err = w.InsertTransactionValidator(ctx, tx.ID(), castTx.Validator)
		if err != nil {
			return err
		}
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
		if castTx.RewardsOwner != nil {
			err = w.insertTransactionsRewardsOwners(ctx, tx.ID(), castTx.RewardsOwner, baseTx, castTx.StakeOuts)
			if err != nil {
				return err
			}
		}
	case *txs.AddSubnetValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeAddSubnetValidator
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
	case *txs.AddDelegatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.StakeOuts,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddDelegator
		err = w.InsertTransactionValidator(ctx, tx.ID(), castTx.Validator)
		if err != nil {
			return err
		}
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
		err = w.insertTransactionsRewardsOwners(ctx, tx.ID(), castTx.DelegationRewardsOwner, baseTx, castTx.StakeOuts)
		if err != nil {
			return err
		}
	case *txs.CreateSubnetTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateSubnet
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
	case *txs.CreateChainTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateChain
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
	case *txs.ImportTx:
		baseTx = castTx.BaseTx.BaseTx
		ins = &avaxIndexer.AddInsContainer{
			Ins:     castTx.ImportedInputs,
			ChainID: castTx.SourceChain.String(),
		}
		typ = models.TransactionTypePVMImport
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
	case *txs.ExportTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.ExportedOutputs,
			ChainID: castTx.DestinationChain.String(),
		}
		typ = models.TransactionTypePVMExport
		err = w.InsertTransactionBlock(ctx, tx.ID(), blkID)
		if err != nil {
			return err
		}
	case *txs.AdvanceTimeTx:
		return nil
	case *txs.RewardValidatorTx:
		rewards := &db.Rewards{
			ID:                 tx.ID().String(),
			BlockID:            blkID.String(),
			Txid:               castTx.TxID.String(),
			Shouldprefercommit: castTx.ShouldPreferCommit,
			CreatedAt:          ctx.Time(),
		}
		return ctx.Persist().InsertRewards(ctx.Ctx(), ctx.DB(), rewards, cfg.PerformUpdates)
	default:
		return fmt.Errorf("unknown tx type %s", reflect.TypeOf(castTx))
	}

	return w.avax.InsertTransaction(
		ctx,
		tx.Bytes(),
		tx.ID(),
		tx.Unsigned.Bytes(),
		&baseTx,
		tx.Creds,
		typ,
		ins,
		outs,
		0,
		genesis,
	)
}

func (w *Writer) insertTransactionsRewardsOwners(ctx services.ConsumerCtx, txID ids.ID, rewardsOwner verify.Verifiable, baseTx avax.BaseTx, stakeOuts []*avax.TransferableOutput) error {
	var err error

	owner, ok := rewardsOwner.(*secp256k1fx.OutputOwners)
	if !ok {
		return fmt.Errorf("rewards owner %v", reflect.TypeOf(rewardsOwner))
	}

	// Ingest each Output Address
	for ipos, addr := range owner.Addresses() {
		addrid := ids.ShortID{}
		copy(addrid[:], addr)
		txRewardsOwnerAddress := &db.TransactionsRewardsOwnersAddress{
			ID:          txID.String(),
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
		outputID := txID.Prefix(uint64(ipos))

		txRewardsOutputs := &db.TransactionsRewardsOwnersOutputs{
			ID:            outputID.String(),
			TransactionID: txID.String(),
			OutputIndex:   uint32(ipos),
			CreatedAt:     ctx.Time(),
		}

		err = ctx.Persist().InsertTransactionsRewardsOwnersOutputs(ctx.Ctx(), ctx.DB(), txRewardsOutputs, cfg.PerformUpdates)
		if err != nil {
			return err
		}
	}

	txRewardsOwner := &db.TransactionsRewardsOwners{
		ID:        txID.String(),
		ChainID:   w.chainID,
		Threshold: owner.Threshold,
		Locktime:  owner.Locktime,
		CreatedAt: ctx.Time(),
	}

	return ctx.Persist().InsertTransactionsRewardsOwners(ctx.Ctx(), ctx.DB(), txRewardsOwner, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionValidator(ctx services.ConsumerCtx, txID ids.ID, validator validator.Validator) error {
	transactionsValidator := &db.TransactionsValidator{
		ID:        txID.String(),
		NodeID:    validator.NodeID.String(),
		Start:     validator.Start,
		End:       validator.End,
		CreatedAt: ctx.Time(),
	}
	return ctx.Persist().InsertTransactionsValidator(ctx.Ctx(), ctx.DB(), transactionsValidator, cfg.PerformUpdates)
}

func (w *Writer) InsertTransactionBlock(ctx services.ConsumerCtx, txID ids.ID, blkTxID ids.ID) error {
	transactionsBlock := &db.TransactionsBlock{
		ID:        txID.String(),
		TxBlockID: blkTxID.String(),
		CreatedAt: ctx.Time(),
	}
	return ctx.Persist().InsertTransactionsBlock(ctx.Ctx(), ctx.DB(), transactionsBlock, cfg.PerformUpdates)
}
