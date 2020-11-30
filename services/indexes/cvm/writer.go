// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cvm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ethereum/go-ethereum/common"

	"github.com/ava-labs/coreth/plugin/evm"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/codec"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/ortelius/services"
	avaxIndexer "github.com/ava-labs/ortelius/services/indexes/avax"
)

type CChainType uint16

var (
	In  CChainType = 1
	Out CChainType = 2

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
		codec:       evm.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "cvm-index" }

func (w *Writer) Consume(ctx context.Context, c services.Consumable) error {
	job := w.conns.Stream().NewJob("index")
	sess := w.conns.DB().NewSessionForEventReceiver(job)

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
	return nil
}

func (w *Writer) indexBlock(ctx services.ConsumerCtx, blockBytes []byte) error {
	atomicTX := new(evm.UnsignedAtomicTx)
	_, err := w.codec.Unmarshal(blockBytes, atomicTX)
	if err != nil {
		return err
	}

	txID, err := ids.ToID(hashing.ComputeHash256(blockBytes))
	if err != nil {
		return err
	}

	switch atx := (*atomicTX).(type) {
	case *evm.UnsignedExportTx:
		return w.indexExportTx(ctx, txID, atx, blockBytes)
	case *evm.UnsignedImportTx:
		return w.indexImportTx(ctx, txID, atx, blockBytes)
	default:
		return ctx.Job().EventErr(fmt.Sprintf("unknown atomic tx %s", reflect.TypeOf(atx)), ErrUnknownBlockType)
	}
}

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, id ids.ID, blockChainID ids.ID, chainID ids.ID) error {
	_, err := ctx.DB().
		InsertInto("cvm_transactions").
		Pair("id", id.String()).
		Pair("blockchain_id", blockChainID.String()).
		Pair("chain_id", chainID.String()).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("cvm_transaction.insert", err)
	}
	if cfg.PerformUpdates {
		_, err := ctx.DB().
			Update("cvm_transactions").
			Set("blockchain_id", blockChainID.String()).
			Set("chain_id", chainID.String()).
			Where("id = ?", ctx.Time()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("cvm_transaction.update", err)
		}
	}
	return nil
}

func (w *Writer) insertAddress(typ CChainType, ctx services.ConsumerCtx, idx uint64, id ids.ID, address common.Address, assetID ids.ID, amount uint64, nonce uint64) error {
	idprefix := id.Prefix(idx)
	_, err := ctx.DB().
		InsertInto("cvm_address").
		Pair("id", idprefix.String()).
		Pair("type", typ).
		Pair("idx", idx).
		Pair("transaction_id", id.String()).
		Pair("address", address.String()).
		Pair("asset_id", assetID.String()).
		Pair("amount", amount).
		Pair("nonce", nonce).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("cvm_address.insert", err)
	}
	if cfg.PerformUpdates {
		_, err := ctx.DB().
			Update("cvm_address").
			Set("idx", idx).
			Set("transaction_id", id.String()).
			Set("address", address.String()).
			Set("asset_id", assetID.String()).
			Set("amount", amount).
			Set("nonce", nonce).
			Where("id = ? and type = ?", idprefix.String(), typ).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("cvm_address.update", err)
		}
	}
	return nil
}

func (w *Writer) indexExportTx(ctx services.ConsumerCtx, txID ids.ID, blk *evm.UnsignedExportTx, _ []byte) error {
	err := w.indexTransaction(ctx, txID, blk.BlockchainID, blk.DestinationChain)
	if err != nil {
		return err
	}

	for icnt, in := range blk.Ins {
		icntval := uint64(icnt)
		err = w.insertAddress(In, ctx, icntval, txID, in.Address, in.AssetID, in.Amount, in.Nonce)
		if err != nil {
			return err
		}
	}

	for idx, out := range blk.ExportedOutputs {
		var totalout uint64
		_, err = w.avax.InsertTransactionOuts(idx, ctx, totalout, out, txID, blk.DestinationChain.String())
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) indexImportTx(ctx services.ConsumerCtx, txID ids.ID, blk *evm.UnsignedImportTx, unsignedBytes []byte) error {
	err := w.indexTransaction(ctx, txID, blk.BlockchainID, blk.SourceChain)
	if err != nil {
		return err
	}

	for icnt, out := range blk.Outs {
		icntval := uint64(icnt)
		err = w.insertAddress(Out, ctx, icntval, txID, out.Address, out.AssetID, out.Amount, 0)
		if err != nil {
			return err
		}
	}

	for inidx, in := range blk.ImportedInputs {
		var totalin uint64
		_, err = w.avax.InsertTransactionIns(inidx, ctx, totalin, in, txID, []verify.Verifiable{in.In}, unsignedBytes, blk.SourceChain.String())
		if err != nil {
			return err
		}
	}

	return nil
}
