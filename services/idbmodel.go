package services

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
	"github.com/palantir/stacktrace"
)

type Persist interface {
	InsertTransaction(
		context.Context,
		dbr.SessionRunner,
		*Transaction,
		bool,
	) error

	QueryTransaction(
		context.Context,
		dbr.SessionRunner,
	) (*Transaction, error)

	QueryOutputsRedeeming(
		context.Context,
		dbr.SessionRunner,
	) (*OutputsRedeeming, error)

	InsertOutputsRedeeming(
		context.Context,
		dbr.SessionRunner,
		*OutputsRedeeming,
		bool,
	) error

	QueryOutputs(
		context.Context,
		dbr.SessionRunner,
	) (*Outputs, error)

	InsertOutputs(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *Outputs,
		upd bool,
	) error
}

type persist struct {
}

func New() Persist {
	return &persist{}
}

type Transaction struct {
	ID                     string
	ChainID                string
	TxType                 string
	Memo                   []byte
	CanonicalSerialization []byte
	Txfee                  uint64
	Genesis                bool
	CreatedAt              time.Time
}

func (p *persist) QueryTransaction(
	ctx context.Context,
	sess dbr.SessionRunner,
) (*Transaction, error) {
	v := &Transaction{}
	err := sess.Select(
		"id",
		"chain_id",
		"type",
		"memo",
		"created_at",
		"canonical_serialization",
		"txfee",
		"genesis",
	).From("avm_transactions").LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertTransaction(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *Transaction,
	upd bool,
) error {
	_, err := sess.
		InsertInto("avm_transactions").
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("type", v.TxType).
		Pair("memo", v.Memo).
		Pair("created_at", v.CreatedAt).
		Pair("canonical_serialization", v.CanonicalSerialization).
		Pair("txfee", v.Txfee).
		Pair("genesis", v.Genesis).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, "avm_transactions.insert")
	}
	if upd {
		_, err = sess.
			Update("avm_transactions").
			Set("chain_id", v.ChainID).
			Set("type", v.TxType).
			Set("memo", v.Memo).
			Set("canonical_serialization", v.CanonicalSerialization).
			Set("txfee", v.Txfee).
			Set("genesis", v.Genesis).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, "avm_transactions.update")
		}
	}
	return nil
}

type OutputsRedeeming struct {
	ID                     string
	RedeemedAt             time.Time
	RedeemingTransactionID string
	Amount                 uint64
	OutputIndex            uint32
	InTX                   string
	AssetID                string
	ChainID                string
	CreatedAt              time.Time
}

func (p *persist) QueryOutputsRedeeming(
	ctx context.Context,
	sess dbr.SessionRunner,
) (*OutputsRedeeming, error) {
	v := &OutputsRedeeming{}
	err := sess.Select(
		"id",
		"redeemed_at",
		"redeeming_transaction_id",
		"amount",
		"output_index",
		"intx",
		"asset_id",
		"chain_id",
		"created_at",
	).From("avm_outputs_redeeming").LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputsRedeeming(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *OutputsRedeeming,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto("avm_outputs_redeeming").
		Pair("id", v.ID).
		Pair("redeemed_at", v.RedeemedAt).
		Pair("redeeming_transaction_id", v.RedeemingTransactionID).
		Pair("amount", v.Amount).
		Pair("output_index", v.OutputIndex).
		Pair("intx", v.InTX).
		Pair("asset_id", v.AssetID).
		Pair("created_at", v.CreatedAt).
		Pair("chain_id", v.ChainID).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, "avm_outputs_redeeming.insert")
	}
	if upd {
		_, err = sess.
			Update("avm_outputs_redeeming").
			Set("redeeming_transaction_id", v.RedeemingTransactionID).
			Set("amount", v.Amount).
			Set("output_index", v.OutputIndex).
			Set("intx", v.InTX).
			Set("asset_id", v.AssetID).
			Set("chain_id", v.ChainID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, "avm_outputs_redeeming.update")
		}
	}
	return nil
}

type Outputs struct {
	ID            string
	ChainID       string
	TransactionID string
	OutputIndex   uint32
	AssetID       string
	OutputType    models.OutputType
	Amount        uint64
	Locktime      uint64
	Threshold     uint32
	GroupID       uint32
	Payload       []byte
	StakeLocktime uint64
	Stake         bool
	CreatedAt     time.Time
}

func (p *persist) QueryOutputs(
	ctx context.Context,
	sess dbr.SessionRunner,
) (*Outputs, error) {
	v := &Outputs{}
	err := sess.Select(
		"id",
		"chain_id",
		"transaction_id",
		"output_index",
		"asset_id",
		"output_type",
		"amount",
		"locktime",
		"threshold",
		"group_id",
		"payload",
		"stake_locktime",
		"stake",
		"created_at",
	).From("outputs").LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputs(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *Outputs,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto("avm_outputs").
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("transaction_id", v.TransactionID).
		Pair("output_index", v.OutputIndex).
		Pair("asset_id", v.AssetID).
		Pair("output_type", v.OutputType).
		Pair("amount", v.Amount).
		Pair("locktime", v.Locktime).
		Pair("threshold", v.Threshold).
		Pair("group_id", v.GroupID).
		Pair("payload", v.Payload).
		Pair("stake_locktime", v.StakeLocktime).
		Pair("stake", v.Stake).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, "avm_outputs.insert")
	}
	if upd {
		_, err = sess.
			Update("avm_outputs").
			Set("chain_id", v.ChainID).
			Set("transaction_id", v.TransactionID).
			Set("output_index", v.OutputIndex).
			Set("asset_id", v.AssetID).
			Set("output_type", v.OutputType).
			Set("amount", v.Amount).
			Set("locktime", v.Locktime).
			Set("threshold", v.Threshold).
			Set("group_id", v.GroupID).
			Set("payload", v.Payload).
			Set("stake_locktime", v.StakeLocktime).
			Set("stake", v.Stake).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, "avm_outputs.update")
		}
	}
	return nil
}
