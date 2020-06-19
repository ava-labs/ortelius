// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"errors"
	"strings"

	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/ava-labs/gecko/utils/wrappers"
	"github.com/ava-labs/gecko/vms/platformvm"

	"github.com/ava-labs/ortelius/services"
)

var (
	ErrUnknownBlockType = errors.New("unknown block type")
)

func (db *DB) Consume(ctx context.Context, c services.Consumable) error {
	job := db.stream.NewJob("index")
	sess := db.db.NewSession(job)

	// Create db tx
	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Consume the tx and commit
	err = db.indexBlock(services.NewConsumerContext(ctx, job, dbTx, c.Timestamp()), c.Body())
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (db *DB) Bootstrap(ctx context.Context) error {
	ctx = context.Background()

	pvmGenesisBytes, err := genesis.Genesis(db.networkID)
	if err != nil {
		return err
	}

	pvmGenesis := &platformvm.Genesis{}
	if err := platformvm.Codec.Unmarshal(pvmGenesisBytes, pvmGenesis); err != nil {
		return err
	}

	err = pvmGenesis.Initialize()
	if err != nil {
		return err
	}

	job := db.stream.NewJob("bootstrap")
	sess := db.db.NewSession(job)
	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	cCtx := services.NewConsumerContext(ctx, job, dbTx, int64(pvmGenesis.Timestamp))
	blockID := ids.NewID([32]byte{})

	for _, createChainTx := range pvmGenesis.Chains {
		err = db.indexCreateChainTx(cCtx, blockID, createChainTx)
		if err != nil {
			return err
		}
	}

	for _, addValidatorTx := range pvmGenesis.Validators.Txs {
		err = db.indexProposalTx(cCtx, blockID, addValidatorTx)
		if err != nil {
			return err
		}
	}

	return dbTx.Commit()
}

func (db *DB) indexBlock(ctx services.ConsumerCtx, blockBytes []byte) error {
	var block platformvm.Block
	if err := platformvm.Codec.Unmarshal(blockBytes, &block); err != nil {
		return ctx.Job().EventErr("index_block.unmarshal_block", err)
	}

	switch blk := block.(type) {
	case *platformvm.ProposalBlock:
		errs := wrappers.Errs{}
		errs.Add(
			db.indexCommonBlock(ctx, BlockTypeProposal, blk.CommonBlock, blockBytes),
			db.indexProposalTx(ctx, blk.ID(), blk.Tx),
		)
		return errs.Err
	case *platformvm.StandardBlock:
		blockErr := db.indexCommonBlock(ctx, BlockTypeStandard, blk.CommonBlock, blockBytes)
		for _, tx := range blk.Txs {
			if err := db.indexDecisionTx(ctx, blk.ID(), tx); err != nil {
				return err
			}
		}
		return blockErr
	case *platformvm.AtomicBlock:
		blockErr := db.indexCommonBlock(ctx, BlockTypeAtomic, blk.CommonBlock, blockBytes)
		if err := db.indexAtomicTx(ctx, blk.ID(), blk.Tx); err != nil {
			return err
		}
		return blockErr
	case *platformvm.Abort:
		return db.indexCommonBlock(ctx, BlockTypeAbort, blk.CommonBlock, blockBytes)
	case *platformvm.Commit:
		return db.indexCommonBlock(ctx, BlockTypeCommit, blk.CommonBlock, blockBytes)
	default:
		ctx.Job().EventErr("index_block", ErrUnknownBlockType)
	}
	return nil
}

func (db *DB) indexCommonBlock(ctx services.ConsumerCtx, blkType BlockType, blk platformvm.CommonBlock, blockBytes []byte) error {
	blkID := ids.NewID(hashing.ComputeHash256Array(blockBytes))

	_, err := ctx.DB().
		InsertInto("pvm_blocks").
		Pair("id", blkID.String()).
		Pair("type", blkType).
		Pair("parent_id", blk.ParentID().String()).
		Pair("chain_id", db.chainID).
		Pair("serialization", blockBytes).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("index_common_block.upsert_block", err)
	}
	return nil
}

func (db *DB) indexTransaction(ctx services.ConsumerCtx, blockID ids.ID, txType TransactionType, id ids.ID, nonce uint64, sig [65]byte) error {
	_, err := ctx.DB().
		InsertInto("pvm_transactions").
		Pair("id", id.String()).
		Pair("block_id", blockID.String()).
		Pair("type", txType).
		Pair("nonce", nonce).
		Pair("signature", sig[:]).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("index_transaction.upsert_transaction", err)
	}
	return nil
}

func (db *DB) indexDecisionTx(ctx services.ConsumerCtx, blockID ids.ID, dTx platformvm.DecisionTx) error {
	switch tx := dTx.(type) {
	case *platformvm.CreateChainTx:
		return db.indexCreateChainTx(ctx, blockID, tx)
	case *platformvm.CreateSubnetTx:
		return db.indexCreateSubnetTx(ctx, blockID, tx)
	}
	return nil
}

func (db *DB) indexProposalTx(ctx services.ConsumerCtx, blockID ids.ID, tx platformvm.ProposalTx) error {
	// var (
	// 	nonce       uint64
	// 	sig         [65]byte
	// 	txType      TransactionType
	// 	shares      uint32
	// 	subnetID    ids.ID
	// 	dv          platformvm.DurationValidator
	// 	destination ids.ShortID
	// )
	//
	// switch tx := tx.(type) {
	// case *platformvm.RewardValidatorTx:
	// 	// return db.indexRewardValidatorTx(ctx, blockID, tx)
	// 	return nil
	// case *platformvm.AdvanceTimeTx:
	// 	// return db.indexRewardValidatorTx(ctx, blockID, tx)
	// 	return nil
	// case *platformvm.AddDefaultSubnetDelegatorTx:
	// 	nonce, sig, dv, shares, subnetID, txType, destination = tx.Nonce, tx.Sig, tx.DurationValidator, platformvm.NumberOfShares, ids.Empty, TransactionTypeAddDefaultSubnetDelegator, tx.Destination
	// case *platformvm.AddDefaultSubnetValidatorTx:
	// 	nonce, sig, dv, shares, subnetID, txType, destination = tx.Nonce, tx.Sig, tx.DurationValidator, tx.Shares, ids.Empty, TransactionTypeAddDefaultSubnetValidator, tx.Destination
	// case *platformvm.AddNonDefaultSubnetValidatorTx:
	// 	nonce, sig, dv, shares, subnetID, txType, destination = tx.Nonce, tx.PayerSig, tx.DurationValidator, 0, tx.Subnet, TransactionTypeAddNonDefaultSubnetValidator, ids.ShortEmpty
	// default:
	// 	ctx.Job().Event("unknown_transaction_type")
	// 	return nil
	// }
	//
	// txBytes, err := db.codec.Marshal(tx)
	// if err != nil {
	// 	return err
	// }
	//
	// if err := db.indexValidator(ctx, ids.NewID(hashing.ComputeHash256Array(txBytes)), dv, destination, shares, subnetID); err != nil {
	// 	return err
	// }
	//
	// if err := db.indexTransaction(ctx, blockID, txType, ids.NewID(hashing.ComputeHash256Array(txBytes)), nonce, sig); err != nil {
	// 	return err
	// }
	return nil
}

func (db *DB) indexAtomicTx(ctx services.ConsumerCtx, blockID ids.ID, atomicTx platformvm.AtomicTx) error {
	txBytes, err := db.codec.Marshal(atomicTx)
	if err != nil {
		return err
	}

	switch tx := atomicTx.(type) {
	case *platformvm.ImportTx:
		return db.indexTransaction(ctx, blockID, TransactionTypeImport, ids.NewID(hashing.ComputeHash256Array(txBytes)), tx.Nonce, tx.Sig)
	case *platformvm.ExportTx:
		return db.indexTransaction(ctx, blockID, TransactionTypeExport, ids.NewID(hashing.ComputeHash256Array(txBytes)), tx.Nonce, tx.Sig)
	}
	return nil
}

func (db *DB) indexCreateChainTx(ctx services.ConsumerCtx, blockID ids.ID, tx *platformvm.CreateChainTx) error {
	txBytes, err := db.codec.Marshal(tx)
	if err != nil {
		return err
	}

	txID := ids.NewID(hashing.ComputeHash256Array(txBytes))

	err = db.indexTransaction(ctx, blockID, TransactionTypeCreateChain, txID, tx.Nonce, [65]byte{})
	if err != nil {
		return err
	}

	// Add chain
	_, err = ctx.DB().
		InsertInto("pvm_chains").
		Pair("id", txID.String()).
		Pair("network_id", tx.NetworkID).
		Pair("subnet_id", tx.SubnetID.String()).
		Pair("name", tx.ChainName).
		Pair("vm_id", tx.VMID.String()).
		Pair("genesis_data", tx.GenesisData).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("index_create_chain_tx.upsert_chain", err)
	}

	// Add FX IDs
	if len(tx.ControlSigs) > 0 {
		builder := ctx.DB().
			InsertInto("pvm_chains_fx_ids").
			Columns("chain_id", "fx_id")
		for _, fxID := range tx.FxIDs {
			builder.Values(db.chainID, fxID.String())
		}
		_, err = builder.ExecContext(ctx.Ctx())
		if err != nil && !errIsDuplicateEntryError(err) {
			return ctx.Job().EventErr("index_create_chain_tx.upsert_chain_fx_ids", err)
		}
	}

	// Add Control Sigs
	if len(tx.ControlSigs) > 0 {
		builder := ctx.DB().
			InsertInto("pvm_chains_control_signatures").
			Columns("chain_id", "signature")
		for _, sig := range tx.ControlSigs {
			builder.Values(db.chainID, sig[:])
		}
		_, err = builder.ExecContext(ctx.Ctx())
		if err != nil && !errIsDuplicateEntryError(err) {
			return ctx.Job().EventErr("index_create_chain_tx.upsert_chain_control_sigs", err)
		}
	}
	return nil
}

func (db *DB) indexCreateSubnetTx(ctx services.ConsumerCtx, blockID ids.ID, tx *platformvm.CreateSubnetTx) error {
	txBytes, err := db.codec.Marshal(tx)
	if err != nil {
		return err
	}

	txID := ids.NewID(hashing.ComputeHash256Array(txBytes))

	err = db.indexTransaction(ctx, blockID, TransactionTypeCreateSubnet, txID, tx.Nonce, tx.Sig)
	if err != nil {
		return err
	}

	// Add subnet
	_, err = ctx.DB().
		InsertInto("pvm_subnets").
		Pair("id", txID.String()).
		Pair("network_id", tx.NetworkID).
		Pair("chain_id", db.chainID).
		Pair("threshold", tx.Threshold).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("index_create_subnet_tx.upsert_subnet", err)
	}

	// Add control keys
	if len(tx.ControlKeys) > 0 {
		builder := ctx.DB().
			InsertInto("pvm_subnet_control_keys").
			Columns("subnet_id", "address")
		for _, address := range tx.ControlKeys {
			builder.Values(txID.String(), address.String())
		}
		_, err = builder.ExecContext(ctx.Ctx())
		if err != nil && !errIsDuplicateEntryError(err) {
			return ctx.Job().EventErr("index_create_subnet_tx.upsert_control_keys", err)
		}
	}

	return nil
}

func (db *DB) indexValidator(ctx services.ConsumerCtx, txID ids.ID, dv platformvm.DurationValidator, destination ids.ShortID, shares uint32, subnetID ids.ID) error {
	_, err := ctx.DB().
		InsertInto("pvm_validators").
		Pair("transaction_id", txID.String()).
		Pair("node_id", dv.NodeID.String()).
		Pair("weight", dv.Weight()).
		Pair("start_time", dv.StartTime()).
		Pair("end_time", dv.EndTime()).
		Pair("destination", destination.String()).
		Pair("shares", shares).
		Pair("subnet_id", subnetID.String()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("index_validator.upsert_validator", err)
	}
	return nil
}

func errIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}
