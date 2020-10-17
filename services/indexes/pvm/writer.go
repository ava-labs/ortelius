// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

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
	ChainID = ids.NewID([32]byte{})

	ErrUnknownBlockType = errors.New("unknown block type")
)

type Writer struct {
	chainID   string
	networkID uint32

	codec codec.Codec
	conns *services.Connections
	avax  *avaxIndexer.Writer
}

func NewWriter(conns *services.Connections, networkID uint32, chainID string) (*Writer, error) {
	return &Writer{
		conns:     conns,
		chainID:   chainID,
		networkID: networkID,
		codec:     platformvm.Codec,
		avax:      avaxIndexer.NewWriter(chainID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "pvm-index" }

func (w *Writer) Consume(ctx context.Context, c services.Consumable) error {
	job := w.conns.Stream().NewJob("index")
	sess := w.conns.DB().NewSessionForEventReceiver(job)

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
			// needs to support StakeableLockOut Locktime...
			// xOut.Locktime = transferOutput.Locktime
			errs.Add(w.avax.InsertOutput(cCtx, ChainID, uint32(idx), utxo.AssetID(), xOut, models.OutputTypesSECP2556K1Transfer, 0, nil))
		case *secp256k1fx.TransferOutput:
			errs.Add(w.avax.InsertOutput(cCtx, ChainID, uint32(idx), utxo.AssetID(), transferOutput, models.OutputTypesSECP2556K1Transfer, 0, nil))
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

		errs.Add(w.indexTransaction(cCtx, ChainID, *tx))
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
			w.indexTransaction(ctx, blk.ID(), blk.Tx),
		)
	case *platformvm.StandardBlock:
		errs.Add(w.indexCommonBlock(ctx, models.BlockTypeStandard, blk.CommonBlock, blockBytes))
		for _, tx := range blk.Txs {
			errs.Add(
				initializeTx(w.codec, *tx),
				w.indexTransaction(ctx, blk.ID(), *tx),
			)
		}
	case *platformvm.AtomicBlock:
		errs.Add(
			initializeTx(w.codec, blk.Tx),
			w.indexCommonBlock(ctx, models.BlockTypeProposal, blk.CommonBlock, blockBytes),
			w.indexTransaction(ctx, blk.ID(), blk.Tx),
		)
	case *platformvm.Abort:
		errs.Add(w.indexCommonBlock(ctx, models.BlockTypeAbort, blk.CommonBlock, blockBytes))
	case *platformvm.Commit:
		errs.Add(w.indexCommonBlock(ctx, models.BlockTypeCommit, blk.CommonBlock, blockBytes))
	default:
		ctx.Job().EventErr("index_block", ErrUnknownBlockType)
	}
	return nil
}

func (w *Writer) indexCommonBlock(ctx services.ConsumerCtx, blkType models.BlockType, blk platformvm.CommonBlock, blockBytes []byte) error {
	blkID := ids.NewID(hashing.ComputeHash256Array(blockBytes))

	_, err := ctx.DB().
		InsertInto("pvm_blocks").
		Pair("id", blkID.String()).
		Pair("chain_id", w.chainID).
		Pair("type", blkType).
		Pair("parent_id", blk.ParentID().String()).
		Pair("serialization", blockBytes).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("index_common_block.upsert_block", err)
	}
	return nil
}

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, _ ids.ID, tx platformvm.Tx) error {
	var (
		baseTx avax.BaseTx
		typ    models.TransactionType
	)

	var ins []*avax.TransferableInput
	var outs []*avax.TransferableOutput

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
		typ = models.TransactionTypePVMImport
	case *platformvm.UnsignedExportTx:
		baseTx = castTx.BaseTx.BaseTx
		outs = castTx.ExportedOutputs
		typ = models.TransactionTypePVMExport
	case *platformvm.UnsignedAdvanceTimeTx:
		return nil
	case *platformvm.UnsignedRewardValidatorTx:
		return nil
	}

	return w.avax.InsertTransaction(ctx, tx.Bytes(), tx.UnsignedBytes(), &baseTx, tx.Creds, typ, ins, outs)
}

// func (w *Writer) indexCreateChainTx(ctx services.ConsumerCtx, blockID ids.ID, tx *platformvm.UnsignedCreateChainTx) error {
// 	txBytes, err := w.codec.Marshal(tx)
// 	if err != nil {
// 		return err
// 	}
//
// 	txID := ids.NewID(hashing.ComputeHash256Array(txBytes))
//
// 	err = w.indexTransactionOld(ctx, blockID, models.TransactionTypeCreateChain, txID)
// 	if err != nil {
// 		return err
// 	}
//
// 	// Add chain
// 	_, err = ctx.DB().
// 		InsertInto("pvm_chains").
// 		Pair("id", txID.String()).
// 		Pair("network_id", tx.NetworkID).
// 		Pair("subnet_id", tx.SubnetID.String()).
// 		Pair("name", tx.ChainName).
// 		Pair("vm_id", tx.VMID.String()).
// 		Pair("genesis_data", tx.GenesisData).
// 		ExecContext(ctx.Ctx())
// 	if err != nil && !errIsDuplicateEntryError(err) {
// 		return ctx.Job().EventErr("index_create_chain_tx.upsert_chain", err)
// 	}
//
// 	// Add FX IDs
// 	// if len(tx.ControlSigs) > 0 {
// 	// 	builder := ctx.DB().
// 	// 		InsertInto("pvm_chains_fx_ids").
// 	// 		Columns("chain_id", "fx_id")
// 	// 	for _, fxID := range tx.FxIDs {
// 	// 		builder.Values(w.chainID, fxID.String())
// 	// 	}
// 	// 	_, err = builder.ExecContext(ctx.Ctx())
// 	// 	if err != nil && !errIsDuplicateEntryError(err) {
// 	// 		return ctx.Job().EventErr("index_create_chain_tx.upsert_chain_fx_ids", err)
// 	// 	}
// 	// }
// 	//
// 	// // Add Control Sigs
// 	// if len(tx.ControlSigs) > 0 {
// 	// 	builder := ctx.DB().
// 	// 		InsertInto("pvm_chains_control_signatures").
// 	// 		Columns("chain_id", "signature")
// 	// 	for _, sig := range tx.ControlSigs {
// 	// 		builder.Values(w.chainID, sig[:])
// 	// 	}
// 	// 	_, err = builder.ExecContext(ctx.Ctx())
// 	// 	if err != nil && !errIsDuplicateEntryError(err) {
// 	// 		return ctx.Job().EventErr("index_create_chain_tx.upsert_chain_control_sigs", err)
// 	// 	}
// 	// }
// 	return nil
// }
//
// func (w *Writer) indexCreateSubnetTx(ctx services.ConsumerCtx, blockID ids.ID, tx *platformvm.UnsignedCreateSubnetTx) error {
// 	txBytes, err := w.codec.Marshal(tx)
// 	if err != nil {
// 		return err
// 	}
//
// 	txID := ids.NewID(hashing.ComputeHash256Array(txBytes))
//
// 	err = w.indexTransactionOld(ctx, blockID, models.TransactionTypeCreateSubnet, txID)
// 	if err != nil {
// 		return err
// 	}
//
// 	// Add subnet
// 	_, err = ctx.DB().
// 		InsertInto("pvm_subnets").
// 		Pair("id", txID.String()).
// 		Pair("network_id", tx.NetworkID).
// 		Pair("chain_id", w.chainID).
// 		// Pair("threshold", tx.Threshold).
// 		Pair("created_at", ctx.Time()).
// 		ExecContext(ctx.Ctx())
// 	if err != nil && !errIsDuplicateEntryError(err) {
// 		return ctx.Job().EventErr("index_create_subnet_tx.upsert_subnet", err)
// 	}
//
// 	// Add control keys
// 	// if len(tx.ControlKeys) > 0 {
// 	// 	builder := ctx.DB().
// 	// 		InsertInto("pvm_subnet_control_keys").
// 	// 		Columns("subnet_id", "address")
// 	// 	for _, address := range tx.ControlKeys {
// 	// 		builder.Values(txID.String(), address.String())
// 	// 	}
// 	// 	_, err = builder.ExecContext(ctx.Ctx())
// 	// 	if err != nil && !errIsDuplicateEntryError(err) {
// 	// 		return ctx.Job().EventErr("index_create_subnet_tx.upsert_control_keys", err)
// 	// 	}
// 	// }
//
// 	return nil
// }

// func (db *DB) indexValidator(ctx services.ConsumerCtx, txID ids.ID, dv platformvm.DurationValidator, destination ids.ShortID, shares uint32, subnetID ids.ID) error {
// 	_, err := ctx.DB().
// 		InsertInto("pvm_validators").
// 		Pair("transaction_id", txID.String()).
// 		Pair("node_id", dv.NodeID.String()).
// 		Pair("weight", dv.Weight()).
// 		Pair("start_time", dv.StartTime()).
// 		Pair("end_time", dv.EndTime()).
// 		Pair("destination", destination.String()).
// 		Pair("shares", shares).
// 		Pair("subnet_id", subnetID.String()).
// 		ExecContext(ctx.Ctx())
// 	if err != nil && !errIsDuplicateEntryError(err) {
// 		return ctx.Job().EventErr("index_validator.upsert_validator", err)
// 	}
// 	return nil
// }

func errIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}
