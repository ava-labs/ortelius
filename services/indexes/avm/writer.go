// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/ava-labs/avalanchego/api/metrics"
	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/avalanche/vertex"
	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/avm/fxs"
	"github.com/ava-labs/avalanchego/vms/avm/txs"
	avalancheGoAvax "github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	p_genesis "github.com/ava-labs/avalanchego/vms/platformvm/genesis"
	p_txs "github.com/ava-labs/avalanchego/vms/platformvm/txs"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/avalanchego/wallet/chain/x"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/utils"
	"github.com/gocraft/dbr/v2"
	"github.com/palantir/stacktrace"
)

var (
	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")
)

const MaxCodecSize = 100_000_000

type Writer struct {
	chainID     string
	networkID   uint32
	avaxAssetID ids.ID

	codec codec.Manager
	avax  *avax.Writer
	ctx   *snow.Context
}

func NewWriter(networkID uint32, chainID string) (*Writer, error) {
	avmCodec := x.Parser.Codec()
	avmCodec.SetMaxSize(MaxCodecSize)

	_, avaxAssetID, err := genesis.FromConfig(genesis.GetConfig(networkID))
	if err != nil {
		return nil, err
	}

	bcLookup := ids.NewAliaser()

	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, err
	}
	if err = bcLookup.Alias(id, "X"); err != nil {
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
		codec:       avmCodec,
		networkID:   networkID,
		avaxAssetID: avaxAssetID,
		avax:        avax.NewWriter(chainID, avaxAssetID),
		ctx:         ctx,
	}, nil
}

func (*Writer) Name() string { return "avm-index" }

func (w *Writer) initCtx(tx *txs.Tx) {
	switch castTx := tx.Unsigned.(type) {
	case *txs.CreateAssetTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
	case *txs.OperationTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
	case *txs.ImportTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
	case *txs.ExportTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, out := range castTx.ExportedOuts {
			out.InitCtx(w.ctx)
		}
	case *txs.BaseTx:
		for _, utxo := range castTx.Outs {
			utxo.Out.InitCtx(w.ctx)
		}
		for _, out := range castTx.Outs {
			out.InitCtx(w.ctx)
		}
	default:
	}
}

func (w *Writer) ParseJSON(txBytes []byte) ([]byte, error) {
	tx, err := parseTx(w.codec, txBytes)
	if err != nil {
		return nil, err
	}
	w.initCtx(tx)
	return json.Marshal(tx)
}

func (w *Writer) Bootstrap(ctx context.Context, conns *utils.Connections, persist db.Persist) error {
	var (
		err                  error
		platformGenesisBytes []byte
	)

	defer func() {
		if err != nil {
			w.ctx.Log.Warn(fmt.Sprintf("bootstrap %v", err))
			return
		}
	}()

	// Get platform genesis block
	platformGenesisBytes, _, err = genesis.FromConfig(genesis.GetConfig(w.networkID))
	if err != nil {
		return stacktrace.Propagate(err, "Failed to get platform genesis bytes")
	}

	platformGenesis, err := p_genesis.Parse(platformGenesisBytes)

	if err != nil {
		return stacktrace.Propagate(err, "Failed to initialize platform genesis")
	}

	// Scan chains in platform genesis until we find the singular AVM chain, which
	// is the X chain, and then we're done
	for _, chain := range platformGenesis.Chains {
		createChainTx, ok := chain.Unsigned.(*p_txs.CreateChainTx)
		if !ok {
			return stacktrace.Propagate(ErrIncorrectGenesisChainTxType, "Platform genesis contains invalid Chains")
		}

		if createChainTx.VMID != constants.AVMID {
			continue
		}

		job := conns.Stream().NewJob("bootstrap")
		dbSess := conns.DB().NewSessionForEventReceiver(job)
		cCtx := services.NewConsumerContext(ctx, dbSess, int64(platformGenesis.Timestamp), 0, persist)
		err = w.insertGenesis(cCtx, createChainTx.GenesisData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) ConsumeConsensus(ctx context.Context, conns *utils.Connections, c services.Consumable, persist db.Persist) error {
	var (
		job  = conns.Stream().NewJob("index-consensus")
		sess = conns.DB().NewSessionForEventReceiver(job)
	)

	var err error

	defer func() {
		if err != nil {
			w.ctx.Log.Warn(fmt.Sprintf("consume consensus %v", err))
			return
		}
	}()

	var vert vertex.StatelessVertex
	vert, err = vertex.Parse(c.Body())
	if err != nil {
		return err
	}
	txs := vert.Txs()

	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	cCtx := services.NewConsumerContext(ctx, dbTx, c.Timestamp(), c.Nanosecond(), persist)

	for _, tx := range txs {
		var txID ids.ID
		txID, err = ids.ToID(hashing.ComputeHash256(tx))
		if err != nil {
			return err
		}

		transactionsEpoch := &db.TransactionsEpoch{
			ID:        txID.String(),
			Epoch:     vert.Epoch(),
			VertexID:  vert.ID().String(),
			CreatedAt: cCtx.Time(),
		}
		err = cCtx.Persist().InsertTransactionsEpoch(cCtx.Ctx(), cCtx.DB(), transactionsEpoch, cfg.PerformUpdates)
		if err != nil {
			return err
		}
	}
	return dbTx.Commit()
}

func (w *Writer) Consume(ctx context.Context, conns *utils.Connections, i services.Consumable, persist db.Persist) error {
	var (
		err  error
		job  = conns.Stream().NewJob("avm-index")
		sess = conns.DB().NewSessionForEventReceiver(job)
	)

	defer func() {
		if err == nil {
			return
		}
		if !utils.ErrIsLockError(err) {
			w.ctx.Log.Warn(fmt.Sprintf("consume %v", err))
			return
		}
	}()

	// Create db tx
	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Ingest the tx and commit
	err = w.insertTx(services.NewConsumerContext(ctx, dbTx, i.Timestamp(), i.Nanosecond(), persist), i.Body())
	if err != nil {
		return err
	}

	return dbTx.Commit()
}

func (w *Writer) insertGenesis(ctx services.ConsumerCtx, genesisBytes []byte) error {
	avmGenesis := &avm.Genesis{}
	ver, err := w.codec.Unmarshal(genesisBytes, avmGenesis)
	if err != nil {
		return stacktrace.Propagate(err, "Failed to parse avm genesis bytes")
	}

	for i, tx := range avmGenesis.Txs {
		txBytes, err := w.codec.Marshal(ver, &txs.Tx{Unsigned: &tx.CreateAssetTx})
		if err != nil {
			return stacktrace.Propagate(err, "Failed to serialize wrapped avm genesis tx %d", i)
		}
		unsignedBytes, err := w.codec.Marshal(ver, &tx.CreateAssetTx)
		if err != nil {
			return err
		}
		tx.Initialize(unsignedBytes)

		fullTx := &txs.Tx{
			Unsigned: &tx.CreateAssetTx,
		}
		if err := x.Parser.InitializeGenesisTx(fullTx); err != nil {
			return err
		}

		if err = w.insertCreateAssetTx(ctx, txBytes, fullTx.ID(), &tx.CreateAssetTx, nil, tx.Alias, true); err != nil {
			return stacktrace.Propagate(err, "Failed to index avm genesis tx %d", i)
		}
	}
	return nil
}

func (w *Writer) insertTx(ctx services.ConsumerCtx, txBytes []byte) error {
	tx, err := parseTx(w.codec, txBytes)
	if err != nil {
		return err
	}
	return w.insertTxInternal(ctx, tx, txBytes)
}

func (w *Writer) insertTxInternal(ctx services.ConsumerCtx, tx *txs.Tx, txBytes []byte) error {
	// Finish processing with a type-specific ingestion routine
	verifiables := w.getVerifiables(tx.Creds)
	txID := tx.ID()

	switch castTx := tx.Unsigned.(type) {
	case *txs.CreateAssetTx:
		return w.insertCreateAssetTx(ctx, txBytes, txID, castTx, verifiables, "", false)
	case *txs.OperationTx:
		return w.insertOperationTx(ctx, txBytes, txID, castTx, verifiables, false)
	case *txs.ImportTx:
		return w.avax.InsertTransaction(
			ctx,
			txBytes,
			txID,
			tx.Unsigned.Bytes(),
			&castTx.BaseTx.BaseTx,
			verifiables,
			models.TransactionTypeAVMImport,
			&avax.AddInsContainer{Ins: castTx.ImportedIns, ChainID: castTx.SourceChain.String()},
			nil,
			0,
			false,
		)
	case *txs.ExportTx:
		return w.avax.InsertTransaction(
			ctx,
			txBytes,
			txID,
			tx.Unsigned.Bytes(),
			&castTx.BaseTx.BaseTx,
			verifiables,
			models.TransactionTypeAVMExport,
			nil,
			&avax.AddOutsContainer{Outs: castTx.ExportedOuts, ChainID: castTx.DestinationChain.String()},
			0,
			false,
		)
	case *txs.BaseTx:
		return w.avax.InsertTransaction(
			ctx,
			txBytes,
			txID,
			tx.Unsigned.Bytes(),
			&castTx.BaseTx,
			verifiables,
			models.TransactionTypeBase,
			nil,
			nil,
			0,
			false,
		)
	default:
		return fmt.Errorf("unknown tx type %s", reflect.TypeOf(castTx))
	}
}

func (w *Writer) insertOperationTx(
	ctx services.ConsumerCtx,
	txBytes []byte,
	txID ids.ID,
	tx *txs.OperationTx,
	creds []verify.Verifiable,
	genesis bool,
) error {
	var (
		err         error
		outputCount uint32
		amount      uint64
		totalout    uint64 = 0
	)

	// we must process the Outs to get the outputCount updated
	// before working on the Ops
	// the outs get processed again in InsertTransaction
	for _, out := range tx.Outs {
		_, err = w.avax.InsertTransactionOuts(outputCount, ctx, 0, out, txID, w.chainID, false, false)
		if err != nil {
			return err
		}
		outputCount++
	}

	addIns := &avax.AddInsContainer{
		ChainID: w.chainID,
	}
	for _, txOps := range tx.Ops {
		for _, u := range txOps.UTXOIDs {
			ti := &avalancheGoAvax.TransferableInput{
				Asset:  txOps.Asset,
				UTXOID: *u,
				In:     &secp256k1fx.TransferInput{},
			}
			addIns.Ins = append(addIns.Ins, ti)
		}

		for _, out := range txOps.Op.Outs() {
			amount, totalout, err = w.avax.ProcessStateOut(ctx, out, txID, outputCount, txOps.AssetID(), amount, totalout, w.chainID, false, false)
			if err != nil {
				return err
			}
			outputCount++
		}
	}

	return w.avax.InsertTransaction(ctx, txBytes, txID, tx.Bytes(), &tx.BaseTx.BaseTx, creds, models.TransactionTypeOperation, addIns, nil, totalout, genesis)
}

func (w *Writer) insertCreateAssetTx(ctx services.ConsumerCtx, txBytes []byte, txID ids.ID, tx *txs.CreateAssetTx, creds []verify.Verifiable, alias string, genesis bool) error {
	var (
		err         error
		outputCount uint32
		amount      uint64
		totalout    uint64 = 0
	)

	// we must process the Outs to get the outputCount updated
	// before working on the states
	// the outs get processed again in InsertTransaction
	for _, out := range tx.Outs {
		_, err = w.avax.InsertTransactionOuts(outputCount, ctx, 0, out, txID, w.chainID, false, false)
		if err != nil {
			return err
		}
		outputCount++
	}

	for _, state := range tx.States {
		for _, out := range state.Outs {
			amount, totalout, err = w.avax.ProcessStateOut(ctx, out, txID, outputCount, txID, amount, totalout, w.chainID, false, false)
			if err != nil {
				return err
			}
			outputCount++
		}
	}

	asset := &db.Assets{
		ID:            txID.String(),
		ChainID:       w.chainID,
		Name:          tx.Name,
		Symbol:        tx.Symbol,
		Denomination:  tx.Denomination,
		Alias:         alias,
		CurrentSupply: amount,
		CreatedAt:     ctx.Time(),
	}

	err = ctx.Persist().InsertAssets(ctx.Ctx(), ctx.DB(), asset, cfg.PerformUpdates)
	if err != nil {
		return err
	}

	return w.avax.InsertTransaction(ctx, txBytes, txID, tx.Bytes(), &tx.BaseTx.BaseTx, creds, models.TransactionTypeCreateAsset, nil, nil, totalout, genesis)
}

// create list of verifiables from within creds
func (w *Writer) getVerifiables(creds []*fxs.FxCredential) []verify.Verifiable {
	verifiables := []verify.Verifiable{}
	for _, cred := range creds {
		verifiables = append(verifiables, cred.Verifiable)
	}
	return verifiables
}
