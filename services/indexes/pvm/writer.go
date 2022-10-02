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
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/utils/constants"
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

	avax *avaxIndexer.Writer
	ctx  *snow.Context
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
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID),
		ctx:         ctx,
	}, nil
}

func (*Writer) Name() string { return "pvm-index" }

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

func (w *Writer) ParseJSON(b []byte) ([]byte, error) {
	// Try and parse as a tx
	tx, err := txs.Parse(blocks.GenesisCodec, b)
	if err == nil {
		tx.Unsigned.InitCtx(w.ctx)
		// TODO: Should we be reporting the type of [tx.Unsigned] rather than
		//       `tx`?
		txtype := reflect.TypeOf(tx)
		txtypeS := txtype.String()
		return json.Marshal(&PtxDataModel{
			Tx:     tx,
			TxType: &txtypeS,
		})
	}

	// Try and parse as block
	blk, err := blocks.Parse(blocks.GenesisCodec, b)
	if err == nil {
		blk.InitCtx(w.ctx)
		blkID := blk.ID()
		blkIDStr := blkID.String()
		btype := reflect.TypeOf(blk)
		btypeS := btype.String()
		return json.Marshal(&PtxDataModel{
			BlockID:   &blkIDStr,
			Block:     &blk,
			BlockType: &btypeS,
		})
	}

	// Try and parse as proposervm block
	proposerBlock, _, err := block.Parse(b)
	if err != nil {
		return nil, err
	}

	blk, err = blocks.Parse(blocks.GenesisCodec, proposerBlock.Block())
	if err != nil {
		return nil, err
	}

	blk.InitCtx(w.ctx)
	blkID := blk.ID()
	blkIDStr := blkID.String()
	btype := reflect.TypeOf(blk)
	btypeS := btype.String()
	pbtype := reflect.TypeOf(proposerBlock)
	pbtypeS := pbtype.String()
	return json.Marshal(&PtxDataModel{
		BlockID:      &blkIDStr,
		Block:        &blk,
		BlockType:    &btypeS,
		Proposer:     NewPtxDataProposerModel(proposerBlock),
		ProposerType: &pbtypeS,
	})
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

func (w *Writer) indexBlock(ctx services.ConsumerCtx, blockBytes []byte) error {
	proposerBlock, _, err := block.Parse(blockBytes)
	var innerBlockBytes []byte
	if err != nil {
		innerBlockBytes = blockBytes
		// We use the "nil"ness below, so we explicitly empty the value here to
		// avoid unexpected errors
		proposerBlock = nil
	} else {
		innerBlockBytes = proposerBlock.Block()
	}

	blk, err := blocks.Parse(blocks.GenesisCodec, innerBlockBytes)
	if err != nil {
		return err
	}

	blkID := blk.ID()
	if proposerBlock != nil {
		// Deprecated: There is *no* good reason to not use proposerBlock.ID().
		//             This would change the historical values.
		proposerBlkHash := ids.ID(hashing.ComputeHash256Array(blockBytes))

		var pvmProposer *db.PvmProposer
		switch proposerBlk := proposerBlock.(type) {
		case block.SignedBlock:
			pvmProposer = &db.PvmProposer{
				ID:            proposerBlk.ID().String(),
				ParentID:      proposerBlk.ParentID().String(),
				BlkID:         blkID.String(),
				ProposerBlkID: proposerBlkHash.String(),
				PChainHeight:  proposerBlk.PChainHeight(),
				Proposer:      proposerBlk.Proposer().String(),
				TimeStamp:     proposerBlk.Timestamp(),
				CreatedAt:     ctx.Time(),
			}
		default:
			pvmProposer = &db.PvmProposer{
				ID:            proposerBlk.ID().String(),
				ParentID:      proposerBlk.ParentID().String(),
				BlkID:         blkID.String(),
				ProposerBlkID: proposerBlkHash.String(),
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
	switch blk := blk.(type) {
	case *blocks.ApricotProposalBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeProposal, blk.CommonBlock, innerBlockBytes))
	case *blocks.ApricotStandardBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, innerBlockBytes))
	case *blocks.ApricotAtomicBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeProposal, blk.CommonBlock, innerBlockBytes))
	case *blocks.ApricotAbortBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeAbort, blk.CommonBlock, innerBlockBytes))
	case *blocks.ApricotCommitBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeCommit, blk.CommonBlock, innerBlockBytes))
	case *blocks.BanffProposalBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, innerBlockBytes))
	case *blocks.BanffStandardBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeStandard, blk.CommonBlock, innerBlockBytes))
	case *blocks.BanffAbortBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeAbort, blk.CommonBlock, innerBlockBytes))
	case *blocks.BanffCommitBlock:
		errs.Add(w.indexCommonBlock(ctx, blkID, models.BlockTypeCommit, blk.CommonBlock, innerBlockBytes))
	default:
		return fmt.Errorf("unknown type %T", blk)
	}
	for _, tx := range blk.Txs() {
		errs.Add(w.indexTransaction(ctx, blkID, *tx, false))
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
		txID   = tx.ID()
		baseTx avax.BaseTx
		typ    models.TransactionType
		ins    *avaxIndexer.AddInsContainer
		outs   *avaxIndexer.AddOutsContainer
	)
	switch castTx := tx.Unsigned.(type) {
	case *txs.AddValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.StakeOuts,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddValidator
		err := w.InsertTransactionValidator(ctx, txID, castTx.Validator)
		if err != nil {
			return err
		}
		if castTx.RewardsOwner != nil {
			err = w.insertTransactionsRewardsOwners(ctx, txID, castTx.RewardsOwner, baseTx, castTx.StakeOuts)
			if err != nil {
				return err
			}
		}
	case *txs.AddSubnetValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeAddSubnetValidator
	case *txs.AddDelegatorTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.StakeOuts,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddDelegator
		err := w.InsertTransactionValidator(ctx, txID, castTx.Validator)
		if err != nil {
			return err
		}
		err = w.insertTransactionsRewardsOwners(ctx, txID, castTx.DelegationRewardsOwner, baseTx, castTx.StakeOuts)
		if err != nil {
			return err
		}
	case *txs.CreateSubnetTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateSubnet
	case *txs.CreateChainTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeCreateChain
	case *txs.ImportTx:
		baseTx = castTx.BaseTx.BaseTx
		ins = &avaxIndexer.AddInsContainer{
			Ins:     castTx.ImportedInputs,
			ChainID: castTx.SourceChain.String(),
		}
		typ = models.TransactionTypePVMImport
	case *txs.ExportTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.ExportedOutputs,
			ChainID: castTx.DestinationChain.String(),
		}
		typ = models.TransactionTypePVMExport
	case *txs.AdvanceTimeTx:
		return nil
	case *txs.RemoveSubnetValidatorTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeRemoveSubnetValidator
	case *txs.TransformSubnetTx:
		baseTx = castTx.BaseTx.BaseTx
		typ = models.TransactionTypeTransformSubnet
	case *txs.AddPermissionlessValidatorTx:
		// TODO: Handle this for all subnetIDs
		if castTx.Subnet != constants.PrimaryNetworkID {
			break
		}

		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.StakeOuts,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddPermissionlessValidator
		err := w.InsertTransactionValidator(ctx, txID, castTx.Validator)
		if err != nil {
			return err
		}
		// TODO: What to do about the different rewards owners?
		err = w.insertTransactionsRewardsOwners(ctx, txID, castTx.ValidatorRewardsOwner, baseTx, castTx.StakeOuts)
		if err != nil {
			return err
		}
	case *txs.AddPermissionlessDelegatorTx:
		// TODO: Handle this for all subnetIDs
		if castTx.Subnet != constants.PrimaryNetworkID {
			break
		}

		baseTx = castTx.BaseTx.BaseTx
		outs = &avaxIndexer.AddOutsContainer{
			Outs:    castTx.StakeOuts,
			Stake:   true,
			ChainID: w.chainID,
		}
		typ = models.TransactionTypeAddPermissionlessDelegator
		err := w.InsertTransactionValidator(ctx, txID, castTx.Validator)
		if err != nil {
			return err
		}
		err = w.insertTransactionsRewardsOwners(ctx, txID, castTx.DelegationRewardsOwner, baseTx, castTx.StakeOuts)
		if err != nil {
			return err
		}
	case *txs.RewardValidatorTx:
		rewards := &db.Rewards{
			ID:                 txID.String(),
			BlockID:            blkID.String(),
			Txid:               castTx.TxID.String(),
			Shouldprefercommit: castTx.ShouldPreferCommit,
			CreatedAt:          ctx.Time(),
		}
		return ctx.Persist().InsertRewards(ctx.Ctx(), ctx.DB(), rewards, cfg.PerformUpdates)
	default:
		return fmt.Errorf("unknown tx type %T", castTx)
	}

	err := w.InsertTransactionBlock(ctx, txID, blkID)
	if err != nil {
		return err
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
		return fmt.Errorf("rewards owner %T", rewardsOwner)
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
