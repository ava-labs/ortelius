package services

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
	"github.com/palantir/stacktrace"
)

type Persist interface {
	InsertTransaction(
		context.Context,
		dbr.SessionRunner,
		time.Time,
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
		time.Time,
		*OutputsRedeeming,
		bool,
	) error
}

type persist struct {
}

func New() Persist {
	return &persist{}
}

type Transaction struct {
	TxID      string    `json:"id"`
	ChainID   string    `json:"chainID"`
	TxType    string    `json:"txType"`
	Memo      []byte    `json:"memo"`
	TxBytes   []byte    `json:"txByte"`
	Txfee     uint64    `json:"txFee"`
	Genesis   bool      `json:"genesis"`
	CreatedAt time.Time `json:"createdAt"`
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
	time time.Time,
	v *Transaction,
	upd bool,
) error {
	_, err := sess.
		InsertInto("avm_transactions").
		Pair("id", v.TxID).
		Pair("chain_id", v.ChainID).
		Pair("type", v.TxType).
		Pair("memo", v.Memo).
		Pair("created_at", time).
		Pair("canonical_serialization", v.TxBytes).
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
			Set("canonical_serialization", v.TxBytes).
			Set("txfee", v.Txfee).
			Set("genesis", v.Genesis).
			Where("id = ?", v.TxID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, "avm_transactions.update")
		}
	}
	return nil
}

type OutputsRedeeming struct {
	InputID     string    `json:"id"`
	RedeemedAt  time.Time `json:"redeemedAt"`
	TxID        string    `json:"redeemingTransactionId"`
	Amount      uint64    `json:"amount"`
	OutputIndex uint32    `json:"outputIndex"`
	InTxID      string    `json:"intx"`
	AssetID     string    `json:"assetId"`
	ChainID     string    `json:"chainId"`
	CreatedAt   time.Time `json:"createdAt"`
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
		"created_at",
		"chain_id",
	).From("avm_transactions").LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputsRedeeming(
	ctx context.Context,
	sess dbr.SessionRunner,
	time time.Time,
	v *OutputsRedeeming,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto("avm_outputs_redeeming").
		Pair("id", v.InputID).
		Pair("redeemed_at", time).
		Pair("redeeming_transaction_id", v.TxID).
		Pair("amount", v.Amount).
		Pair("output_index", v.OutputIndex).
		Pair("intx", v.InTxID).
		Pair("asset_id", v.AssetID).
		Pair("created_at", time).
		Pair("chain_id", v.ChainID).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, "avm_outputs_redeeming.insert")
	}
	if cfg.PerformUpdates {
		_, err = sess.
			Update("avm_outputs_redeeming").
			Set("redeeming_transaction_id", v.TxID).
			Set("amount", v.Amount).
			Set("output_index", v.OutputIndex).
			Set("intx", v.InTxID).
			Set("asset_id", v.AssetID).
			Set("chain_id", v.ChainID).
			Where("id = ?", v.InputID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, "avm_outputs_redeeming.update")
		}
	}
	return nil
}