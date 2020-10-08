// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"errors"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/database/nodb"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/utils/codec"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// MaxSerializationLen is the maximum number of bytes a canonically
	// serialized tx can be stored as in the database.
	MaxSerializationLen = 64000

	// MaxMemoLen is the maximum number of bytes a memo can be in the database
	MaxMemoLen = 2048
)

var (
	// ErrSerializationTooLong is returned when trying to ingest data with a
	// serialization larger than our max
	ErrSerializationTooLong = errors.New("serialization is too long")

	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")

	ecdsaRecoveryFactory = crypto.FactorySECP256K1R{}
)

type Writer struct {
	db        *services.DB
	chainID   string
	networkID uint32
	codec     codec.Codec
	stream    *health.Stream
}

func NewWriter(conns *services.Connections, networkID uint32, chainID string) (*Writer, error) {
	codec, err := newAVMCodec(networkID, chainID)
	if err != nil {
		return nil, err
	}

	return &Writer{
		codec:     codec,
		chainID:   chainID,
		networkID: networkID,
		stream:    conns.Stream(),
		db:        services.NewDB(conns.Stream(), conns.DB()),
	}, nil
}

func (*Writer) Name() string { return "avm-index" }

func (w *Writer) Bootstrap(ctx context.Context) error {
	var (
		err                  error
		platformGenesisBytes []byte
		job                  = w.stream.NewJob("bootstrap")
	)
	job.KeyValue("chain_id", w.chainID)

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	platformGenesisBytes, _, err = genesis.Genesis(w.networkID)
	if err != nil {
		return err
	}

	platformGenesis := &platformvm.Genesis{}
	if err = platformvm.Codec.Unmarshal(platformGenesisBytes, platformGenesis); err != nil {
		return err
	}
	if err = platformGenesis.Initialize(); err != nil {
		return err
	}

	for _, chain := range platformGenesis.Chains {
		createChainTx, ok := chain.UnsignedTx.(*platformvm.UnsignedCreateChainTx)
		if !ok {
			return ErrIncorrectGenesisChainTxType
		}
		if createChainTx.VMID.Equals(avm.ID) {
			dbSess := w.db.NewSessionForEventReceiver(job)
			cCtx := services.NewConsumerContext(ctx, job, dbSess, int64(platformGenesis.Timestamp))
			return w.ingestTx(cCtx, createChainTx.GenesisData)
		}
	}
	return nil
}

func (w *Writer) Consume(ctx context.Context, i services.Consumable) error {
	var (
		err  error
		job  = w.stream.NewJob("index")
		sess = w.db.NewSessionForEventReceiver(job)
	)
	job.KeyValue("id", i.ID())
	job.KeyValue("chain_id", i.ChainID())

	defer func() {
		if err != nil {
			job.CompleteKv(health.Error, health.Kvs{"err": err.Error()})
			return
		}
		job.Complete(health.Success)
	}()

	// Create db tx
	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	// Ingest the tx and commit
	err = w.ingestTx(services.NewConsumerContext(ctx, job, dbTx, i.Timestamp()), i.Body())
	if err != nil {
		return err
	}

	err = dbTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) Close(ctx context.Context) error {
	w.stream.Event("close")
	return nil
}

func (w *Writer) ingestTx(ctx services.ConsumerCtx, txBytes []byte) error {
	tx, err := parseTx(w.codec, txBytes)
	if err != nil {
		return err
	}

	// fire and forget..
	// update the created_at on the state table if we have an earlier date in ctx.Time().
	// which means we need to re-run aggregation calculations from this earlier date.
	_, _ = models.UpdateAvmAssetAggregationLiveStateTimestamp(ctx.Ctx(), ctx.DB(), ctx.Time())

	// Finish processing with a type-specific ingestion routine
	switch castTx := tx.UnsignedTx.(type) {
	case *avm.GenesisAsset:
		return w.ingestCreateAssetTx(ctx, txBytes, &castTx.CreateAssetTx, castTx.Alias, false)
	case *avm.CreateAssetTx:
		return w.ingestCreateAssetTx(ctx, txBytes, castTx, "", false)
	case *avm.OperationTx:
		// 	db.ingestOperationTx(ctx, tx)
	case *avm.ImportTx:
		return w.ingestBaseTx(ctx, txBytes, tx, &castTx.BaseTx, models.TXTypeImport)
	case *avm.ExportTx:
		return w.ingestBaseTx(ctx, txBytes, tx, &castTx.BaseTx, models.TXTypeExport)
	case *avm.BaseTx:
		return w.ingestBaseTx(ctx, txBytes, tx, castTx, models.TXTypeBase)
	default:
		return errors.New("unknown tx type")
	}
	return nil
}

func (w *Writer) ingestCreateAssetTx(ctx services.ConsumerCtx, txBytes []byte, tx *avm.CreateAssetTx, alias string, bootstrap bool) error {
	var err error
	var txID ids.ID
	if bootstrap {
		wrappedTxBytes, err := w.codec.Marshal(&avm.Tx{UnsignedTx: tx})
		if err != nil {
			return err
		}
		txID = ids.NewID(hashing.ComputeHash256Array(wrappedTxBytes))
	} else {
		txID = tx.ID()
	}

	var outputCount uint32
	var amount uint64
	for _, state := range tx.States {
		for _, out := range state.Outs {
			outputCount++

			switch outtype := out.(type) {
			case *nftfx.TransferOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				w.ingestOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTTransferOutput, outtype.GroupID, outtype.Payload)
			case *nftfx.MintOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				w.ingestOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTMint, outtype.GroupID, nil)
			case *secp256k1fx.MintOutput:
				xOut := &secp256k1fx.TransferOutput{Amt: 0, OutputOwners: outtype.OutputOwners}
				w.ingestOutput(ctx, txID, outputCount-1, txID, xOut, true, models.OutputTypesNFTMint, 0, nil)
			case *secp256k1fx.TransferOutput:
				w.ingestOutput(ctx, txID, outputCount-1, txID, outtype, true, models.OutputTypesSECP2556K1Transfer, 0, nil)
				amount, err = math.Add64(amount, outtype.Amount())
				if err != nil {
					_ = ctx.Job().EventErr("add_to_amount", err)
				}
			default:
				_ = ctx.Job().EventErr("assertion_to_output", errors.New("Output is not known"))
			}
		}
	}

	_, err = ctx.DB().
		InsertInto("avm_assets").
		Pair("id", txID.String()).
		Pair("chain_Id", w.chainID).
		Pair("name", tx.Name).
		Pair("symbol", tx.Symbol).
		Pair("denomination", tx.Denomination).
		Pair("alias", alias).
		Pair("current_supply", amount).
		Pair("created_at", ctx.Time()).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return err
	}

	// If the tx or memo is too big we can't store it in the db
	if len(txBytes) > MaxSerializationLen {
		txBytes = []byte{}
	}

	if len(tx.Memo) > MaxMemoLen {
		tx.Memo = nil
	}

	_, err = ctx.DB().
		InsertInto("avm_transactions").
		Pair("id", txID.String()).
		Pair("chain_id", w.chainID).
		Pair("type", models.TXTypeCreateAsset).
		Pair("memo", tx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return err
	}
	return nil
}

func (w *Writer) ingestBaseTx(ctx services.ConsumerCtx, txBytes []byte, uniqueTx *avm.Tx, baseTx *avm.BaseTx, txType models.TransactionType) error {
	var (
		err   error
		total uint64 = 0
		creds        = uniqueTx.Credentials()
	)

	unsignedTxBytes, err := w.codec.Marshal(&uniqueTx.UnsignedTx)
	if err != nil {
		return err
	}

	redeemedOutputs := make([]string, 0, 2*len(baseTx.Ins))
	for i, in := range baseTx.Ins {
		total, err = math.Add64(total, in.Input().Amount())
		if err != nil {
			return err
		}

		inputID := in.TxID.Prefix(uint64(in.OutputIndex))

		// Save id so we can mark this output as consumed
		redeemedOutputs = append(redeemedOutputs, inputID.String())

		// Upsert this input as an output in case we haven't seen the parent tx
		w.ingestOutput(ctx, in.UTXOID.TxID, in.UTXOID.OutputIndex, in.AssetID(), &secp256k1fx.TransferOutput{
			Amt: in.In.Amount(),
			OutputOwners: secp256k1fx.OutputOwners{
				// We leave Addrs blank because we ingested them above with their signatures
				Addrs: []ids.ShortID{},
			},
		}, false, models.OutputTypesSECP2556K1Transfer, 0, nil)

		// For each signature we recover the public key and the data to the db
		cred, ok := creds[i].(*secp256k1fx.Credential)
		if !ok {
			return nil
		}
		for _, sig := range cred.Sigs {
			publicKey, err := ecdsaRecoveryFactory.RecoverPublicKey(unsignedTxBytes, sig[:])
			if err != nil {
				return err
			}

			w.ingestAddressFromPublicKey(ctx, publicKey)
			w.ingestOutputAddress(ctx, inputID, publicKey.Address(), sig[:])
		}
	}

	// Mark all inputs as redeemed
	if len(redeemedOutputs) > 0 {
		_, err = ctx.DB().
			Update("avm_outputs").
			Set("redeemed_at", dbr.Now).
			Set("redeeming_transaction_id", baseTx.ID().String()).
			Where("id IN ?", redeemedOutputs).
			ExecContext(ctx.Ctx())
		if err != nil {
			return err
		}
	}

	// If the tx or memo is too big we can't store it in the db
	if len(txBytes) > MaxSerializationLen {
		txBytes = []byte{}
	}

	if len(baseTx.Memo) > MaxMemoLen {
		baseTx.Memo = nil
	}

	// Add baseTx to the table
	_, err = ctx.DB().
		InsertInto("avm_transactions").
		Pair("id", baseTx.ID().String()).
		Pair("chain_id", baseTx.BlockchainID.String()).
		Pair("type", txType).
		Pair("memo", baseTx.Memo).
		Pair("created_at", ctx.Time()).
		Pair("canonical_serialization", txBytes).
		ExecContext(ctx.Ctx())
	if err != nil && !errIsDuplicateEntryError(err) {
		return err
	}

	// Process baseTx outputs by adding to the outputs table
	for idx, out := range baseTx.Outs {
		xOut, ok := out.Output().(*secp256k1fx.TransferOutput)
		if !ok {
			continue
		}
		w.ingestOutput(ctx, baseTx.ID(), uint32(idx), out.AssetID(), xOut, true, models.OutputTypesSECP2556K1Transfer, 0, nil)
	}
	return nil
}

func (w *Writer) ingestOutput(ctx services.ConsumerCtx, txID ids.ID, idx uint32, assetID ids.ID, out *secp256k1fx.TransferOutput, upd bool, outputType models.OutputType, groupID uint32, payload []byte) {
	outputID := txID.Prefix(uint64(idx))

	var err error
	_, err = ctx.DB().
		InsertInto("avm_outputs").
		Pair("id", outputID.String()).
		Pair("chain_id", w.chainID).
		Pair("transaction_id", txID.String()).
		Pair("output_index", idx).
		Pair("asset_id", assetID.String()).
		Pair("output_type", outputType).
		Pair("amount", out.Amount()).
		Pair("created_at", ctx.Time()).
		Pair("locktime", out.Locktime).
		Pair("threshold", out.Threshold).
		Pair("group_id", groupID).
		Pair("payload", payload).
		ExecContext(ctx.Ctx())

	if err != nil {
		// We got an error and it's not a duplicate entry error, so log it
		if !errIsDuplicateEntryError(err) {
			_ = w.stream.EventErr("ingest_output.insert", err)
			// We got a duplicate entry error and we want to update
		} else if upd {
			if _, err = ctx.DB().
				Update("avm_outputs").
				Set("chain_id", w.chainID).
				Set("output_type", outputType).
				Set("amount", out.Amount()).
				Set("locktime", out.Locktime).
				Set("threshold", out.Threshold).
				Set("group_id", groupID).
				Set("payload", payload).
				Where("avm_outputs.id = ?", outputID.String()).
				ExecContext(ctx.Ctx()); err != nil {
				_ = w.stream.EventErr("ingest_output.update", err)
			}
		}
	}

	// Ingest each Output Address
	for _, addr := range out.Addresses() {
		addrBytes := [20]byte{}
		copy(addrBytes[:], addr)
		w.ingestOutputAddress(ctx, outputID, ids.NewShortID(addrBytes), nil)
	}
}

func (w *Writer) ingestAddressFromPublicKey(ctx services.ConsumerCtx, publicKey crypto.PublicKey) {
	_, err := ctx.DB().
		InsertInto("addresses").
		Pair("address", publicKey.Address().String()).
		Pair("public_key", publicKey.Bytes()).
		ExecContext(ctx.Ctx())

	if err != nil && !errIsDuplicateEntryError(err) {
		_ = ctx.Job().EventErr("ingest_address_from_public_key", err)
	}
}

func (w *Writer) ingestOutputAddress(ctx services.ConsumerCtx, outputID ids.ID, address ids.ShortID, sig []byte) {
	builder := ctx.DB().
		InsertInto("avm_output_addresses").
		Pair("output_id", outputID.String()).
		Pair("address", address.String()).
		Pair("created_at", ctx.Time())

	if sig != nil {
		builder = builder.Pair("redeeming_signature", sig)
	}

	_, err := builder.ExecContext(ctx.Ctx())
	switch {
	case err == nil:
		return
	case !errIsDuplicateEntryError(err):
		_ = ctx.Job().EventErr("ingest_output_address", err)
		return
	case sig == nil:
		return
	}

	_, err = ctx.DB().
		Update("avm_output_addresses").
		Set("redeeming_signature", sig).
		Where("output_id = ? and address = ?", outputID.String(), address.String()).
		ExecContext(ctx.Ctx())
	if err != nil {
		_ = ctx.Job().EventErr("ingest_output_address", err)
		return
	}
}

func errIsDuplicateEntryError(err error) bool {
	return db.ErrIsDuplicateEntryError(err)
}

func parseTx(c codec.Codec, bytes []byte) (*avm.Tx, error) {
	tx := &avm.Tx{}
	err := c.Unmarshal(bytes, tx)
	if err != nil {
		return nil, err
	}
	unsignedBytes, err := c.Marshal(&tx.UnsignedTx)
	if err != nil {
		return nil, err
	}

	tx.Initialize(unsignedBytes, bytes)
	return tx, nil

	// utx := &avm.UniqueTx{
	// 	TxState: &avm.TxState{
	// 		Tx: tx,
	// 	},
	// 	txID: tx.ID(),
	// }
	//
	// return utx, nil
}

// newAVMCodec creates codec that can parse avm objects
func newAVMCodec(networkID uint32, chainID string) (codec.Codec, error) {
	g, err := genesis.VMGenesis(networkID, avm.ID)
	if err != nil {
		return nil, err
	}

	createChainTx, ok := g.UnsignedTx.(*platformvm.UnsignedCreateChainTx)
	if !ok {
		return nil, ErrIncorrectGenesisChainTxType
	}

	bcLookup := &ids.Aliaser{}
	bcLookup.Initialize()
	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, err
	}
	if err = bcLookup.Alias(id, "X"); err != nil {
		return nil, err
	}

	var (
		fxIDs = createChainTx.FxIDs
		fxs   = make([]*common.Fx, 0, len(fxIDs))
		ctx   = &snow.Context{
			NetworkID: networkID,
			ChainID:   id,
			Log:       logging.NoLog{},
			Metrics:   prometheus.NewRegistry(),
			BCLookup:  bcLookup,
		}
	)
	for _, fxID := range fxIDs {
		switch {
		case fxID.Equals(secp256k1fx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		case fxID.Equals(nftfx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &nftfx.Fx{},
				ID: fxID,
			})
		default:
			// return nil, fmt.Errorf("Unknown FxID: %s", fxID)
		}
	}

	// Initialize an producer to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err = vm.Initialize(ctx, &nodb.Database{}, createChainTx.GenesisData, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, err
	}

	return vm.Codec(), nil
}
