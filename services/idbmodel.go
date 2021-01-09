package services

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
)

const (
	TableTransactions          = "avm_transactions"
	TableOutputsRedeeming      = "avm_outputs_redeeming"
	TableOutputs               = "avm_outputs"
	TableAssets                = "avm_assets"
	TableAddresses             = "addresses"
	TableAddressChain          = "address_chain"
	TableOutputAddresses       = "avm_output_addresses"
	TableTransactionsEpochs    = "transactions_epoch"
	TableCvmAddresses          = "cvm_addresses"
	TableCvmTransactions       = "cvm_transactions"
	TablePvmBlocks             = "pvm_blocks"
	TableRewards               = "rewards"
	TableTransactionsValidator = "transactions_validator"
	TableTransactionsBlock     = "transactions_block"
)

type Persist interface {
	QueryTransactions(
		context.Context,
		dbr.SessionRunner,
		*Transactions,
	) (*Transactions, error)
	InsertTransaction(
		context.Context,
		dbr.SessionRunner,
		*Transactions,
		bool,
	) error

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
		context.Context,
		dbr.SessionRunner,
		*Outputs,
		bool,
	) error

	QueryAssets(
		context.Context,
		dbr.SessionRunner,
		*Assets,
	) (*Assets, error)
	InsertAssets(
		context.Context,
		dbr.SessionRunner,
		*Assets,
		bool,
	) error

	QueryAddresses(
		context.Context,
		dbr.SessionRunner,
		*Addresses,
	) (*Addresses, error)
	InsertAddresses(
		context.Context,
		dbr.SessionRunner,
		*Addresses,
		bool,
	) error

	QueryAddressChain(
		context.Context,
		dbr.SessionRunner,
		*AddressChain,
	) (*AddressChain, error)

	InsertAddressChain(
		context.Context,
		dbr.SessionRunner,
		*AddressChain,
		bool,
	) error

	QueryOutputAddresses(
		context.Context,
		dbr.SessionRunner,
		*OutputAddresses,
	) (*OutputAddresses, error)
	InsertOutputAddresses(
		context.Context,
		dbr.SessionRunner,
		*OutputAddresses,
		bool,
	) error
	UpdateOutputAddresses(
		context.Context,
		dbr.SessionRunner,
		*OutputAddresses,
	) error

	QueryTransactionsEpoch(
		context.Context,
		dbr.SessionRunner,
		*TransactionsEpoch,
	) (*TransactionsEpoch, error)
	InsertTransactionsEpoch(
		context.Context,
		dbr.SessionRunner,
		*TransactionsEpoch,
		bool,
	) error

	QueryCvmAddresses(
		context.Context,
		dbr.SessionRunner,
		*CvmAddresses,
	) (*CvmAddresses, error)
	InsertCvmAddresses(
		context.Context,
		dbr.SessionRunner,
		*CvmAddresses,
		bool,
	) error

	QueryCvmTransactions(
		context.Context,
		dbr.SessionRunner,
		*CvmTransactions,
	) (*CvmTransactions, error)
	InsertCvmTransactions(
		context.Context,
		dbr.SessionRunner,
		*CvmTransactions,
		bool,
	) error

	QueryPvmBlocks(
		context.Context,
		dbr.SessionRunner,
		*PvmBlocks,
	) (*PvmBlocks, error)
	InsertPvmBlocks(
		context.Context,
		dbr.SessionRunner,
		*PvmBlocks,
		bool,
	) error

	QueryRewards(
		context.Context,
		dbr.SessionRunner,
		*Rewards,
	) (*Rewards, error)
	InsertRewards(
		context.Context,
		dbr.SessionRunner,
		*Rewards,
		bool,
	) error

	QueryTransactionsValidator(
		context.Context,
		dbr.SessionRunner,
		*TransactionsValidator,
	) (*TransactionsValidator, error)
	InsertTransactionsValidator(
		context.Context,
		dbr.SessionRunner,
		*TransactionsValidator,
		bool,
	) error

	QueryTransactionsBlock(
		context.Context,
		dbr.SessionRunner,
		*TransactionsBlock,
	) (*TransactionsBlock, error)

	InsertTransactionsBlock(
		context.Context,
		dbr.SessionRunner,
		*TransactionsBlock,
		bool,
	) error
}

type persist struct {
}

func NewPersist() Persist {
	return &persist{}
}

func EventErr(t string, err error) error {
	return fmt.Errorf("%w (%s)", err, t)
}

type Transactions struct {
	ID                     string
	ChainID                string
	Type                   string
	Memo                   []byte
	CanonicalSerialization []byte
	Txfee                  uint64
	Genesis                bool
	CreatedAt              time.Time
}

func (p *persist) QueryTransactions(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *Transactions,
) (*Transactions, error) {
	v := &Transactions{}
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
	v *Transactions,
	upd bool,
) error {
	var err error
	_, err = sess.
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
		return EventErr(TableTransactionsBlock, err)
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
			return EventErr(TableTransactionsBlock, err)
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
		return EventErr(TableOutputsRedeeming, err)
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
			return EventErr(TableOutputsRedeeming, err)
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
	Frozen        bool
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
		"frozen",
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
		Pair("frozen", v.Frozen).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableOutputs, err)
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
			Set("frozen", v.Frozen).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableOutputs, err)
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
		InsertInto(TableAssets).
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
		return EventErr(TableAssets, err)
	}
	if upd {
		_, err = sess.
			Update(TableAssets).
			Set("chain_Id", v.ChainID).
			Set("name", v.Name).
			Set("symbol", v.Symbol).
			Set("denomination", v.Denomination).
			Set("alias", v.Alias).
			Set("current_supply", v.CurrentSupply).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableAssets, err)
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
	var err error
	_, err = sess.
		InsertInto(TableAddresses).
		Pair("address", v.Address).
		Pair("public_key", v.PublicKey).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableAddresses, err)
	}
	if upd {
		_, err = sess.
			Update(TableAddresses).
			Set("public_key", v.PublicKey).
			Where("address = ?", v.Address).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableAddresses, err)
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
	var err error
	_, err = sess.
		InsertInto(TableAddressChain).
		Pair("address", v.Address).
		Pair("chain_id", v.ChainID).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableAddressChain, err)
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
	stmt := sess.
		InsertInto(TableOutputAddresses).
		Pair("output_id", v.OutputID).
		Pair("address", v.Address).
		Pair("created_at", v.CreatedAt)
	if v.RedeemingSignature != nil {
		stmt = stmt.Pair("redeeming_signature", v.RedeemingSignature)
	}
	_, err = stmt.ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableOutputAddresses, err)
	}
	if v.RedeemingSignature != nil && upd {
		_, err = sess.
			Update(TableOutputAddresses).
			Set("redeeming_signature", v.RedeemingSignature).
			Where("output_id = ? and address=?", v.OutputID, v.Address).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableOutputAddresses, err)
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
		return EventErr(TableOutputAddresses, err)
	}
	return nil
}

type TransactionsEpoch struct {
	ID        string
	Epoch     uint32
	VertexID  string
	CreatedAt time.Time
}

func (p *persist) QueryTransactionsEpoch(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *TransactionsEpoch,
) (*TransactionsEpoch, error) {
	v := &TransactionsEpoch{}
	err := sess.Select(
		"id",
		"epoch",
		"vertex_id",
		"created_at",
	).From(TableTransactionsEpochs).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertTransactionsEpoch(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *TransactionsEpoch,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableTransactionsEpochs).
		Pair("id", v.ID).
		Pair("epoch", v.Epoch).
		Pair("vertex_id", v.VertexID).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableTransactionsEpochs, err)
	}
	if upd {
		_, err = sess.
			Update(TableTransactionsEpochs).
			Set("epoch", v.Epoch).
			Set("vertex_id", v.VertexID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableTransactionsEpochs, err)
		}
	}

	return nil
}

type CvmAddresses struct {
	ID            string
	Type          models.CChainType
	Idx           uint64
	TransactionID string
	Address       string
	AssetID       string
	Amount        uint64
	Nonce         uint64
	CreatedAt     time.Time
}

func (p *persist) QueryCvmAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *CvmAddresses,
) (*CvmAddresses, error) {
	v := &CvmAddresses{}
	err := sess.Select(
		"id",
		"type",
		"idx",
		"transaction_id",
		"address",
		"asset_id",
		"amount",
		"nonce",
		"created_at",
	).From(TableCvmAddresses).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertCvmAddresses(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *CvmAddresses,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableCvmAddresses).
		Pair("id", v.ID).
		Pair("type", v.Type).
		Pair("idx", v.Idx).
		Pair("transaction_id", v.TransactionID).
		Pair("address", v.Address).
		Pair("asset_id", v.AssetID).
		Pair("amount", v.Amount).
		Pair("nonce", v.Nonce).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableCvmAddresses, err)
	}
	if upd {
		_, err = sess.
			Update(TableCvmAddresses).
			Set("type", v.Type).
			Set("idx", v.Idx).
			Set("transaction_id", v.TransactionID).
			Set("address", v.Address).
			Set("asset_id", v.AssetID).
			Set("amount", v.Amount).
			Set("nonce", v.Nonce).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableCvmAddresses, err)
		}
	}
	return nil
}

type CvmTransactions struct {
	ID           string
	Type         models.CChainType
	BlockchainID string
	Block        string
	CreatedAt    time.Time
}

func (p *persist) QueryCvmTransactions(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *CvmTransactions,
) (*CvmTransactions, error) {
	v := &CvmTransactions{}
	err := sess.Select(
		"id",
		"type",
		"blockchain_id",
		"cast(block as char) as block",
		"created_at",
	).From(TableCvmTransactions).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertCvmTransactions(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *CvmTransactions,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertBySql("insert into "+TableCvmTransactions+" (id,type,blockchain_id,created_at,block) values(?,?,?,?,"+v.Block+")",
			v.ID, v.Type, v.BlockchainID, v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableCvmTransactions, err)
	}
	if upd {
		_, err = sess.
			UpdateBySql("update "+TableCvmTransactions+" set type=?,blockchain_id=?,block="+v.Block+" where id=?",
				v.Type, v.BlockchainID, v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableCvmTransactions, err)
		}
	}
	return nil
}

type PvmBlocks struct {
	ID            string
	ChainID       string
	Type          models.BlockType
	ParentID      string
	Serialization []byte
	CreatedAt     time.Time
}

func (p *persist) QueryPvmBlocks(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *PvmBlocks,
) (*PvmBlocks, error) {
	v := &PvmBlocks{}
	err := sess.Select(
		"id",
		"chain_id",
		"type",
		"parent_id",
		"serialization",
		"created_at",
	).From(TablePvmBlocks).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertPvmBlocks(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *PvmBlocks,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TablePvmBlocks).
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("type", v.Type).
		Pair("parent_id", v.ParentID).
		Pair("created_at", v.CreatedAt).
		Pair("serialization", v.Serialization).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TablePvmBlocks, err)
	}
	if upd {
		_, err = sess.
			Update(TablePvmBlocks).
			Set("chain_id", v.ChainID).
			Set("type", v.Type).
			Set("parent_id", v.ParentID).
			Set("serialization", v.Serialization).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TablePvmBlocks, err)
		}
	}

	return nil
}

type Rewards struct {
	ID                 string
	BlockID            string
	Txid               string
	Shouldprefercommit bool
	CreatedAt          time.Time
}

func (p *persist) QueryRewards(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *Rewards,
) (*Rewards, error) {
	v := &Rewards{}
	err := sess.Select(
		"id",
		"block_id",
		"txid",
		"shouldprefercommit",
		"created_at",
	).From(TableRewards).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertRewards(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *Rewards,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableRewards).
		Pair("id", v.ID).
		Pair("block_id", v.BlockID).
		Pair("txid", v.Txid).
		Pair("shouldprefercommit", v.Shouldprefercommit).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableRewards, err)
	}
	if upd {
		_, err = sess.
			Update(TableRewards).
			Set("block_id", v.BlockID).
			Set("txid", v.Txid).
			Set("shouldprefercommit", v.Shouldprefercommit).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableRewards, err)
		}
	}

	return nil
}

type TransactionsValidator struct {
	ID        string
	NodeID    string
	Start     uint64
	End       uint64
	CreatedAt time.Time
}

func (p *persist) QueryTransactionsValidator(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *TransactionsValidator,
) (*TransactionsValidator, error) {
	v := &TransactionsValidator{}
	err := sess.Select(
		"id",
		"node_id",
		"start",
		"end",
		"created_at",
	).From(TableTransactionsValidator).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertTransactionsValidator(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *TransactionsValidator,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableTransactionsValidator).
		Pair("id", v.ID).
		Pair("node_id", v.NodeID).
		Pair("start", v.Start).
		Pair("end", v.End).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableTransactionsValidator, err)
	}
	if upd {
		_, err = sess.
			Update(TableTransactionsValidator).
			Set("node_id", v.NodeID).
			Set("start", v.Start).
			Set("end", v.End).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableTransactionsValidator, err)
		}
	}
	return nil
}

type TransactionsBlock struct {
	ID        string
	TxBlockID string
	CreatedAt time.Time
}

func (p *persist) QueryTransactionsBlock(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *TransactionsBlock,
) (*TransactionsBlock, error) {
	v := &TransactionsBlock{}
	err := sess.Select(
		"id",
		"tx_block_id",
		"created_at",
	).From(TableTransactionsBlock).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertTransactionsBlock(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *TransactionsBlock,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableTransactionsBlock).
		Pair("id", v.ID).
		Pair("tx_block_id", v.TxBlockID).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableTransactionsBlock, err)
	}
	if upd {
		_, err = sess.
			Update(TableTransactionsBlock).
			Set("tx_block_id", v.TxBlockID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableTransactionsBlock, err)
		}
	}
	return nil
}
