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

	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/coreth/plugin/evm"
	"github.com/ava-labs/ortelius/cfg"
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
	avax  *avaxIndexer.Writer
}

func NewWriter(networkID uint32, chainID string) (*Writer, error) {
	_, avaxAssetID, err := genesis.Genesis(networkID)
	if err != nil {
		return nil, err
	}

	return &Writer{
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		codec:       evm.Codec,
		avax:        avaxIndexer.NewWriter(chainID, avaxAssetID),
	}, nil
}

func (*Writer) Name() string { return "cvm-index" }

func (w *Writer) Consume(ctx context.Context, conns *services.Connections, c services.Consumable, blockHeader *types.Header, persist services.Persist) error {
	job := conns.Stream().NewJob("cvm-index")
	sess := conns.DB().NewSessionForEventReceiver(job)

	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Consume the tx and commit
	err = w.indexBlock(services.NewConsumerContext(ctx, job, dbTx, c.Timestamp(), persist), c.Body(), blockHeader)
	if err != nil {
		return err
	}

	err = dbTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) Bootstrap(_ context.Context, _ *services.Connections) error {
	return nil
}

func (w *Writer) indexBlock(ctx services.ConsumerCtx, blockBytes []byte, blockHeader *types.Header) error {
	atomicTX := new(evm.Tx)
	_, err := w.codec.Unmarshal(blockBytes, atomicTX)
	if err != nil {
		return err
	}
	return w.indexBlockInternal(ctx, atomicTX, blockBytes, blockHeader)
}

func (w *Writer) indexBlockInternal(ctx services.ConsumerCtx, atomicTX *evm.Tx, blockBytes []byte, blockHeader *types.Header) error {
	txID, err := ids.ToID(hashing.ComputeHash256(blockBytes))
	if err != nil {
		return err
	}

	switch atx := atomicTX.UnsignedTx.(type) {
	case *evm.UnsignedExportTx:
		return w.indexExportTx(ctx, txID, atx, blockBytes, blockHeader)
	case *evm.UnsignedImportTx:
		return w.indexImportTx(ctx, txID, atx, atomicTX.Creds, blockBytes, blockHeader)
	default:
		return ctx.Job().EventErr(fmt.Sprintf("unknown atomic tx %s", reflect.TypeOf(atx)), ErrUnknownBlockType)
	}
}

func (w *Writer) indexTransaction(
	ctx services.ConsumerCtx,
	id ids.ID,
	typ models.CChainType,
	blockChainID ids.ID,
	blockHeader *types.Header,
	txFee uint64,
	unsignedBytes []byte,
) error {
	cvmTransaction := &services.CvmTransactions{
		ID:           id.String(),
		Type:         typ,
		BlockchainID: blockChainID.String(),
		Block:        blockHeader.Number.String(),
		CreatedAt:    ctx.Time(),
	}
	err := ctx.Persist().InsertCvmTransactions(ctx.Ctx(), ctx.DB(), cvmTransaction, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	avmTxtype := ""
	switch typ {
	case models.CChainImport:
		avmTxtype = "atomic_import_tx"
	case models.CChainExport:
		avmTxtype = "atomic_export_tx"
	}

	return w.avax.InsertTransactionBase(
		ctx,
		id,
		blockChainID.String(),
		avmTxtype,
		[]byte(""),
		unsignedBytes,
		txFee,
		false,
	)
}

func (w *Writer) insertAddress(
	typ models.CChainType,
	ctx services.ConsumerCtx,
	idx uint64,
	id ids.ID,
	address common.Address,
	assetID ids.ID,
	amount uint64,
	nonce uint64,
) error {
	idprefix := id.Prefix(idx)

	cvmAddress := &services.CvmAddresses{
		ID:            idprefix.String(),
		Type:          typ,
		Idx:           idx,
		TransactionID: id.String(),
		Address:       address.String(),
		AssetID:       assetID.String(),
		Amount:        amount,
		Nonce:         nonce,
		CreatedAt:     ctx.Time(),
	}
	return ctx.Persist().InsertCvmAddresses(ctx.Ctx(), ctx.DB(), cvmAddress, cfg.PerformUpdates)
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
		totalout, err = w.avax.InsertTransactionOuts(idx, ctx, totalout, out, txID, tx.DestinationChain.String(), false)
		if err != nil {
			return err
		}
		idx++
	}

	return w.indexTransaction(ctx, txID, models.CChainExport, tx.BlockchainID, blockHeader, totalin-totalout, unsignedBytes)
}

func (w *Writer) indexImportTx(ctx services.ConsumerCtx, txID ids.ID, tx *evm.UnsignedImportTx, creds []verify.Verifiable, unsignedBytes []byte, blockHeader *types.Header) error {
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
		totalin, err = w.avax.InsertTransactionIns(inidx, ctx, totalin, in, txID, creds, unsignedBytes, tx.SourceChain.String())
		if err != nil {
			return err
		}
	}

	return w.indexTransaction(ctx, txID, models.CChainImport, tx.BlockchainID, blockHeader, totalin-totalout, unsignedBytes)
}
