package services

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
	"github.com/palantir/stacktrace"
)

const TableTransactions = "avm_transactions"
const TableOutputsRedeeming = "avm_outputs_redeeming"
const TableOutputs = "avm_outputs"
const TableAssets = "avm_assets"
const TableAddresses = "addresses"
const TableAddressChain = "address_chain"
const TableOutputAddresses = "avm_output_addresses"

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
		*Transaction,
	) (*Transaction, error)

	QueryOutputsRedeeming(
		context.Context,
		dbr.SessionRunner,
		*OutputsRedeeming,
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
		*Outputs,
	) (*Outputs, error)

	InsertOutputs(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *Outputs,
		upd bool,
	) error

	QueryAssets(
		context.Context,
		dbr.SessionRunner,
		*Assets,
	) (*Assets, error)

	InsertAssets(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *Assets,
		upd bool,
	) error

	QueryAddresses(
		context.Context,
		dbr.SessionRunner,
		*Addresses,
	) (*Addresses, error)

	InsertAddresses(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *Addresses,
		upd bool,
	) error

	QueryAddressChain(
		context.Context,
		dbr.SessionRunner,
		*AddressChain,
	) (*AddressChain, error)

	InsertAddressChain(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *AddressChain,
		upd bool,
	) error

	QueryOutputAddresses(
		context.Context,
		dbr.SessionRunner,
		*OutputAddresses,
	) (*OutputAddresses, error)

	InsertOutputAddresses(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *OutputAddresses,
		upd bool,
	) error

	UpdateOutputAddresses(
		ctx context.Context,
		sess dbr.SessionRunner,
		v *OutputAddresses,
	) error
}

type persist struct {
}

func NewPersist() Persist {
	return &persist{}
}

type Transaction struct {
	ID                     string
	ChainID                string
	Type                   string
	Memo                   []byte
	CanonicalSerialization []byte
	Txfee                  uint64
	Genesis                bool
	CreatedAt              time.Time
}

func (p *persist) QueryTransaction(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *Transaction,
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
	).From(TableTransactions).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertTransaction(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *Transaction,
	upd bool,
) error {
	_, err := sess.
		InsertInto(TableTransactions).
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("type", v.Type).
		Pair("memo", v.Memo).
		Pair("created_at", v.CreatedAt).
		Pair("canonical_serialization", v.CanonicalSerialization).
		Pair("txfee", v.Txfee).
		Pair("genesis", v.Genesis).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, TableTransactions+".insert")
	}
	if upd {
		_, err = sess.
			Update(TableTransactions).
			Set("chain_id", v.ChainID).
			Set("type", v.Type).
			Set("memo", v.Memo).
			Set("canonical_serialization", v.CanonicalSerialization).
			Set("txfee", v.Txfee).
			Set("genesis", v.Genesis).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, TableTransactions+".update")
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
	Intx                   string
	AssetID                string
	ChainID                string
	CreatedAt              time.Time
}

func (p *persist) QueryOutputsRedeeming(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *OutputsRedeeming,
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
	).From(TableOutputsRedeeming).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
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
		InsertInto(TableOutputsRedeeming).
		Pair("id", v.ID).
		Pair("redeemed_at", v.RedeemedAt).
		Pair("redeeming_transaction_id", v.RedeemingTransactionID).
		Pair("amount", v.Amount).
		Pair("output_index", v.OutputIndex).
		Pair("intx", v.Intx).
		Pair("asset_id", v.AssetID).
		Pair("created_at", v.CreatedAt).
		Pair("chain_id", v.ChainID).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, TableOutputsRedeeming+".insert")
	}
	if upd {
		_, err = sess.
			Update(TableOutputsRedeeming).
			Set("redeeming_transaction_id", v.RedeemingTransactionID).
			Set("amount", v.Amount).
			Set("output_index", v.OutputIndex).
			Set("intx", v.Intx).
			Set("asset_id", v.AssetID).
			Set("chain_id", v.ChainID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, TableOutputsRedeeming+".update")
		}
	}
	return nil
}

type Outputs struct {
	ID            string
	ChainID       string
	TransactionID string
	OutputIndex   uint32
	OutputType    models.OutputType
	AssetID       string
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
	q *Outputs,
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
	).From(TableOutputs).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
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
		InsertInto(TableOutputs).
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
		return stacktrace.Propagate(err, TableOutputs+".insert")
	}
	if upd {
		_, err = sess.
			Update(TableOutputs).
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
			return stacktrace.Propagate(err, TableOutputs+".update")
		}
	}
	return nil
}

type Assets struct {
	ID            string
	ChainID       string
	Name          string
	Symbol        string
	Denomination  byte
	Alias         string
	CurrentSupply uint64
	CreatedAt     time.Time
}

func (p *persist) QueryAssets(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *Assets,
) (*Assets, error) {
	v := &Assets{}
	err := sess.Select(
		"id",
		"chain_id",
		"name",
		"symbol",
		"denomination",
		"alias",
		"current_supply",
		"created_at",
	).From(TableAssets).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAssets(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *Assets,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto("avm_assets").
		Pair("id", v.ID).
		Pair("chain_Id", v.ChainID).
		Pair("name", v.Name).
		Pair("symbol", v.Symbol).
		Pair("denomination", v.Denomination).
		Pair("alias", v.Alias).
		Pair("current_supply", v.CurrentSupply).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, TableAssets+".insert")
	}
	if upd {
		_, err = sess.
			Update("avm_assets").
			Set("chain_Id", v.ChainID).
			Set("name", v.Name).
			Set("symbol", v.Symbol).
			Set("denomination", v.Denomination).
			Set("alias", v.Alias).
			Set("current_supply", v.CurrentSupply).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, TableAssets+".update")
		}
	}
	return nil
}

type Addresses struct {
	Address   string
	PublicKey []byte
	CreatedAt time.Time
}

func (p *persist) QueryAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *Addresses,
) (*Addresses, error) {
	v := &Addresses{}
	err := sess.Select(
		"address",
		"public_key",
		"created_at",
	).From(TableAddresses).
		Where("address=?", q.Address).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *Addresses,
	upd bool,
) error {
	_, err := sess.
		InsertInto(TableAddresses).
		Pair("address", v.Address).
		Pair("public_key", v.PublicKey).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, TableAddresses+".insert")
	}
	if upd {
		_, err = sess.
			Update(TableAddresses).
			Set("public_key", v.PublicKey).
			Where("address = ?", v.Address).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, TableAddresses+".update")
		}
	}

	return nil
}

type AddressChain struct {
	Address   string
	ChainID   string
	CreatedAt time.Time
}

func (p *persist) QueryAddressChain(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *AddressChain,
) (*AddressChain, error) {
	v := &AddressChain{}
	err := sess.Select(
		"address",
		"chain_id",
		"created_at",
	).From(TableAddressChain).
		Where("address=? and chain_id=?", q.Address, q.ChainID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAddressChain(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *AddressChain,
	_ bool,
) error {
	_, err := sess.
		InsertInto("address_chain").
		Pair("address", v.Address).
		Pair("chain_id", v.ChainID).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, TableAddressChain+".insert")
	}
	return nil
}

type OutputAddresses struct {
	OutputID           string
	Address            string
	RedeemingSignature []byte
	CreatedAt          time.Time
}

func (p *persist) QueryOutputAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *OutputAddresses,
) (*OutputAddresses, error) {
	v := &OutputAddresses{}
	err := sess.Select(
		"output_id",
		"address",
		"redeeming_signature",
		"created_at",
	).From(TableOutputAddresses).
		Where("output_id=? and address=?", q.OutputID, q.Address).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *OutputAddresses,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableOutputAddresses).
		Pair("output_id", v.OutputID).
		Pair("address", v.Address).
		Pair("redeeming_signature", v.RedeemingSignature).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return stacktrace.Propagate(err, TableOutputAddresses+".insert")
	}
	if v.RedeemingSignature != nil && upd {
		_, err = sess.
			Update(TableOutputAddresses).
			Set("redeeming_signature", v.RedeemingSignature).
			Where("output_id = ? and address=?", v.OutputID, v.Address).
			ExecContext(ctx)
		if err != nil {
			return stacktrace.Propagate(err, TableOutputAddresses+".update")
		}
	}
	return nil
}

func (p *persist) UpdateOutputAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *OutputAddresses,
) error {
	var err error
	_, err = sess.
		Update(TableOutputAddresses).
		Set("redeeming_signature", v.RedeemingSignature).
		Where("output_id = ? and address=?", v.OutputID, v.Address).
		ExecContext(ctx)
	if err != nil {
		return stacktrace.Propagate(err, TableOutputAddresses+".update")
	}
	return nil
}
