// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cvm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/coreth/core/types"

	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ethereum/go-ethereum/common"

	"github.com/ava-labs/coreth/plugin/evm"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/ortelius/services"
	avaxIndexer "github.com/ava-labs/ortelius/services/indexes/avax"
)

var (
	ErrUnknownBlockType = errors.New("unknown block type")
)

type Writer struct {
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
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		codec:       evm.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID, conns.Stream()),
	}, nil
}

func (*Writer) Name() string { return "cvm-index" }

func (w *Writer) Consume(ctx context.Context, c services.Consumable, blockHeader *types.Header) error {
	job := w.conns.Stream().NewJob("index")
	sess := w.conns.DB().NewSessionForEventReceiver(job)

	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Consume the tx and commit
	err = w.indexBlock(services.NewConsumerContext(ctx, job, dbTx, c.Timestamp()), c.Body(), blockHeader)
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (w *Writer) Bootstrap(ctx context.Context) error {
	return nil
}

func (w *Writer) indexBlock(ctx services.ConsumerCtx, blockBytes []byte, blockHeader *types.Header) error {
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
		return w.indexExportTx(ctx, txID, atx, blockBytes, blockHeader)
	case *evm.UnsignedImportTx:
		return w.indexImportTx(ctx, txID, atx, blockBytes, blockHeader)
	default:
		return ctx.Job().EventErr(fmt.Sprintf("unknown atomic tx %s", reflect.TypeOf(atx)), ErrUnknownBlockType)
	}
}

func (w *Writer) indexTransaction(ctx services.ConsumerCtx, id ids.ID, typ models.CChainType, blockChainID ids.ID, blockHeader *types.Header, txFee uint64, unsignedBytes []byte) error {
	_, err := ctx.DB().
		InsertBySql("insert into cvm_transactions (id,type,blockchain_id,created_at,block) values(?,?,?,?,"+blockHeader.Number.String()+")",
			id.String(), typ, blockChainID.String(), ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return ctx.Job().EventErr("cvm_transaction.insert", err)
	}
	if cfg.PerformUpdates {
		_, err := ctx.DB().
			UpdateBySql("update cvm_transactions set type=?,blockchain_id=?,block="+blockHeader.Number.String()+" where id=?",
				typ, blockChainID.String(), id.String()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("cvm_transaction.update", err)
		}
	}

	avmTxtype := ""
	switch typ {
	case models.CChainImport:
		avmTxtype = "atomic_import_tx"
	case models.CChainExport:
		avmTxtype = "atomic_export_tx"
	}

	return w.avax.InsertTransactionBase(ctx, id, blockChainID.String(), avmTxtype, []byte(""), unsignedBytes, txFee, false)
}

func (w *Writer) insertAddress(typ models.CChainType, ctx services.ConsumerCtx, idx uint64, id ids.ID, address common.Address, assetID ids.ID, amount uint64, nonce uint64) error {
	idprefix := id.Prefix(idx)
	_, err := ctx.DB().
		InsertInto("cvm_addresses").
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
		return ctx.Job().EventErr("cvm_addresses.insert", err)
	}
	if cfg.PerformUpdates {
		_, err := ctx.DB().
			Update("cvm_addresses").
			Set("type", typ).
			Set("idx", idx).
			Set("transaction_id", id.String()).
			Set("address", address.String()).
			Set("asset_id", assetID.String()).
			Set("amount", amount).
			Set("nonce", nonce).
			Where("id = ?", idprefix.String()).
			ExecContext(ctx.Ctx())
		if err != nil {
			return ctx.Job().EventErr("cvm_addresses.update", err)
		}
	}
	return nil
}

func (w *Writer) indexExportTx(ctx services.ConsumerCtx, txID ids.ID, tx *evm.UnsignedExportTx, unsignedBytes []byte, blockHeader *types.Header) error {
	var err error

	var totalin uint64
	for icnt, in := range tx.Ins {
		icntval := uint64(icnt)
		err = w.insertAddress(models.CChainIn, ctx, icntval, txID, in.Address, in.AssetID, in.Amount, in.Nonce)
		if err != nil {
			return err
		}
		totalin += in.Amount
	}

	var totalout uint64
	var idx uint32
	for _, out := range tx.ExportedOutputs {
		totalout, err = w.avax.InsertTransactionOuts(idx, ctx, totalout, out, txID, tx.DestinationChain.String())
		if err != nil {
			return err
		}
		idx++
	}

	return w.indexTransaction(ctx, txID, models.CChainExport, tx.BlockchainID, blockHeader, totalin-totalout, unsignedBytes)
}

func (w *Writer) indexImportTx(ctx services.ConsumerCtx, txID ids.ID, tx *evm.UnsignedImportTx, unsignedBytes []byte, blockHeader *types.Header) error {
	var err error

	var totalout uint64
	for icnt, out := range tx.Outs {
		icntval := uint64(icnt)
		err = w.insertAddress(models.CchainOut, ctx, icntval, txID, out.Address, out.AssetID, out.Amount, 0)
		if err != nil {
			return err
		}
		totalout += out.Amount
	}

	var totalin uint64
	for inidx, in := range tx.ImportedInputs {
		totalin, err = w.avax.InsertTransactionIns(inidx, ctx, totalin, in, txID, []verify.Verifiable{in.In}, unsignedBytes, tx.SourceChain.String())
		if err != nil {
			return err
		}
	}

	return w.indexTransaction(ctx, txID, models.CChainImport, tx.BlockchainID, blockHeader, totalin-totalout, unsignedBytes)
}
