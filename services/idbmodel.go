package services

import (
	"context"
	"time"

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
}

type persist struct {
}

func New() Persist {
	return &persist{}
}

type Transaction struct {
	TxID    string `json:"id"`
	ChainID string `json:"chainID"`
	TxType  string `json:"txType"`
	Memo    []byte `json:"memo"`
	TxBytes []byte `json:"txByte"`
	Txfee   uint64 `json:"txFee"`
	Genesis bool   `json:"genesis"`
}

func (p *persist) QueryTransaction(
	ctx context.Context,
	sess dbr.SessionRunner,
) (*Transaction, error) {
	t := &Transaction{}
	err := sess.Select(
		"id",
		"chain_id",
		"type",
		"memo",
		"created_at",
		"canonical_serialization",
		"txfee",
		"genesis",
	).From("avm_transactions").LoadOneContext(ctx, t)
	return t, err
}

func (p *persist) InsertTransaction(
	ctx context.Context,
	sess dbr.SessionRunner,
	time time.Time,
	t *Transaction,
	upd bool,
) error {
	_, err := sess.
		InsertInto("avm_transactions").
		Pair("id", t.TxID).
		Pair("chain_id", t.ChainID).
		Pair("type", t.TxType).
		Pair("memo", t.Memo).
		Pair("created_at", time).
		Pair("canonical_serialization", t.TxBytes).
		Pair("txfee", t.Txfee).
		Pair("genesis", t.Genesis).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, "avm_transactions.insert")
	}
	if upd {
		_, err = sess.
			Update("avm_transactions").
			Set("chain_id", t.ChainID).
			Set("type", t.TxType).
			Set("memo", t.Memo).
			Set("canonical_serialization", t.TxBytes).
			Set("txfee", t.Txfee).
			Set("genesis", t.Genesis).
			Where("id = ?", t.TxID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, "avm_transactions.update")
		}
	}
	return nil
}
