package services

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
)

const (
	TableTransactions                   = "avm_transactions"
	TableOutputsRedeeming               = "avm_outputs_redeeming"
	TableOutputs                        = "avm_outputs"
	TableAssets                         = "avm_assets"
	TableAddresses                      = "addresses"
	TableAddressChain                   = "address_chain"
	TableOutputAddresses                = "avm_output_addresses"
	TableTransactionsEpochs             = "transactions_epoch"
	TableCvmAddresses                   = "cvm_addresses"
	TableCvmTransactions                = "cvm_transactions"
	TableCvmTransactionsTxdata          = "cvm_transactions_txdata"
	TablePvmBlocks                      = "pvm_blocks"
	TableRewards                        = "rewards"
	TableTransactionsValidator          = "transactions_validator"
	TableTransactionsBlock              = "transactions_block"
	TableAddressBech32                  = "addresses_bech32"
	TableOutputAddressAccumulateOut     = "output_addresses_accumulate_out"
	TableOutputAddressAccumulateIn      = "output_addresses_accumulate_in"
	TableOutputTxsAccumulate            = "output_txs_accumulate"
	TableAccumulateBalancesReceived     = "accumulate_balances_received"
	TableAccumulateBalancesSent         = "accumulate_balances_sent"
	TableAccumulateBalancesTransactions = "accumulate_balances_transactions"
)

type Persist interface {
	QueryTransactions(
		context.Context,
		dbr.SessionRunner,
		*Transactions,
	) (*Transactions, error)
	InsertTransactions(
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

	QueryCvmTransactionsTxdata(
		context.Context,
		dbr.SessionRunner,
		*CvmTransactionsTxdata,
	) (*CvmTransactionsTxdata, error)
	InsertCvmTransactionsTxdata(
		context.Context,
		dbr.SessionRunner,
		*CvmTransactionsTxdata,
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

	QueryAddressBech32(
		context.Context,
		dbr.SessionRunner,
		*AddressBech32,
	) (*AddressBech32, error)
	InsertAddressBech32(
		context.Context,
		dbr.SessionRunner,
		*AddressBech32,
		bool,
	) error

	QueryOutputAddressAccumulateOut(
		context.Context,
		dbr.SessionRunner,
		*OutputAddressAccumulate,
	) (*OutputAddressAccumulate, error)
	InsertOutputAddressAccumulateOut(
		context.Context,
		dbr.SessionRunner,
		*OutputAddressAccumulate,
	) error

	QueryOutputAddressAccumulateIn(
		context.Context,
		dbr.SessionRunner,
		*OutputAddressAccumulate,
	) (*OutputAddressAccumulate, error)
	InsertOutputAddressAccumulateIn(
		context.Context,
		dbr.SessionRunner,
		*OutputAddressAccumulate,
	) error

	QueryOutputTxsAccumulate(
		context.Context,
		dbr.SessionRunner,
		*OutputTxsAccumulate,
	) (*OutputTxsAccumulate, error)
	InsertOutputTxsAccumulate(
		context.Context,
		dbr.SessionRunner,
		*OutputTxsAccumulate,
	) error

	QueryAccumulateBalancesReceived(
		context.Context,
		dbr.SessionRunner,
		*AccumulateBalancesAmount,
	) (*AccumulateBalancesAmount, error)
	InsertAccumulateBalancesReceived(
		context.Context,
		dbr.SessionRunner,
		*AccumulateBalancesAmount,
	) error

	QueryAccumulateBalancesSent(
		context.Context,
		dbr.SessionRunner,
		*AccumulateBalancesAmount,
	) (*AccumulateBalancesAmount, error)
	InsertAccumulateBalancesSent(
		context.Context,
		dbr.SessionRunner,
		*AccumulateBalancesAmount,
	) error

	QueryAccumulateBalancesTransactions(
		context.Context,
		dbr.SessionRunner,
		*AccumulateBalancesTransactions,
	) (*AccumulateBalancesTransactions, error)
	InsertAccumulateBalancesTransactions(
		context.Context,
		dbr.SessionRunner,
		*AccumulateBalancesTransactions,
	) error
}

type persist struct {
}

func NewPersist() Persist {
	return &persist{}
}

func EventErr(t string, upd bool, err error) error {
	updmsg := ""
	if upd {
		updmsg = " upd"
	}
	return fmt.Errorf("%w (%s%s)", err, t, updmsg)
}

type Transactions struct {
	ID                     string
	ChainID                string
	Type                   string
	Memo                   []byte
	CanonicalSerialization []byte
	Txfee                  uint64
	NetworkID              uint32
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
		"network_id",
	).From(TableTransactions).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertTransactions(
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
		Pair("network_id", v.NetworkID).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableTransactions, false, err)
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
			Set("network_id", v.NetworkID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableTransactions, true, err)
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
		return EventErr(TableOutputsRedeeming, false, err)
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
			return EventErr(TableOutputsRedeeming, true, err)
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
	Stakeableout  bool
	Genesisutxo   bool
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
		"stakeableout",
		"genesisutxo",
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
		Pair("stakeableout", v.Stakeableout).
		Pair("genesisutxo", v.Genesisutxo).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableOutputs, false, err)
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
			Set("stakeableout", v.Stakeableout).
			Set("genesisutxo", v.Genesisutxo).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableOutputs, true, err)
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
		return EventErr(TableAssets, false, err)
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
			return EventErr(TableAssets, true, err)
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
		return EventErr(TableAddresses, false, err)
	}
	if upd {
		_, err = sess.
			Update(TableAddresses).
			Set("public_key", v.PublicKey).
			Where("address = ?", v.Address).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableAddresses, true, err)
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
		return EventErr(TableAddressChain, false, err)
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
		return EventErr(TableOutputAddresses, false, err)
	}
	if v.RedeemingSignature != nil && upd {
		_, err = sess.
			Update(TableOutputAddresses).
			Set("redeeming_signature", v.RedeemingSignature).
			Where("output_id = ? and address=?", v.OutputID, v.Address).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableOutputAddresses, true, err)
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
		return EventErr(TableOutputAddresses, true, err)
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
		return EventErr(TableTransactionsEpochs, false, err)
	}
	if upd {
		_, err = sess.
			Update(TableTransactionsEpochs).
			Set("epoch", v.Epoch).
			Set("vertex_id", v.VertexID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableTransactionsEpochs, true, err)
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
		return EventErr(TableCvmAddresses, false, err)
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
			return EventErr(TableCvmAddresses, true, err)
		}
	}
	return nil
}

type CvmTransactions struct {
	ID            string
	TransactionID string
	Type          models.CChainType
	BlockchainID  string
	Block         string
	CreatedAt     time.Time
	Serialization []byte
	TxTime        time.Time
	Nonce         uint64
}

func (p *persist) QueryCvmTransactions(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *CvmTransactions,
) (*CvmTransactions, error) {
	v := &CvmTransactions{}
	err := sess.Select(
		"id",
		"transaction_id",
		"type",
		"blockchain_id",
		"cast(block as char) as block",
		"created_at",
		"serialization",
		"tx_time",
		"nonce",
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
		InsertBySql("insert into "+TableCvmTransactions+" (id,transaction_id,type,blockchain_id,created_at,block,serialization,tx_time,nonce) values(?,?,?,?,?,"+v.Block+",?,?,?)",
			v.ID, v.TransactionID, v.Type, v.BlockchainID, v.CreatedAt, v.Serialization, v.TxTime, v.Nonce).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableCvmTransactions, false, err)
	}
	if upd {
		_, err = sess.
			UpdateBySql("update "+TableCvmTransactions+" set transaction_id=?,type=?,blockchain_id=?,block="+v.Block+",serialization=?,tx_time=?,nonce=? where id=?",
				v.TransactionID, v.Type, v.BlockchainID, v.Serialization, v.TxTime, v.Nonce, v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableCvmTransactions, true, err)
		}
	}
	return nil
}

type CvmTransactionsTxdata struct {
	Block         string
	Idx           uint64
	Hash          string
	Rcpt          string
	Nonce         uint64
	Serialization []byte
	CreatedAt     time.Time
}

func (p *persist) QueryCvmTransactionsTxdata(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *CvmTransactionsTxdata,
) (*CvmTransactionsTxdata, error) {
	v := &CvmTransactionsTxdata{}
	err := sess.Select(
		"block",
		"idx",
		"hash",
		"rcpt",
		"nonce",
		"serialization",
		"created_at",
	).From(TableCvmTransactionsTxdata).
		Where("block="+q.Block+" and idx=?", q.Idx).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertCvmTransactionsTxdata(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *CvmTransactionsTxdata,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertBySql("insert into "+TableCvmTransactionsTxdata+" (block,idx,hash,rcpt,nonce,serialization,created_at) values("+v.Block+",?,?,?,?,?,?)",
			v.Idx, v.Hash, v.Rcpt, v.Nonce, v.Serialization, v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableCvmTransactionsTxdata, false, err)
	}
	if upd {
		_, err = sess.
			UpdateBySql("update "+TableCvmTransactionsTxdata+" set hash=?,rcpt=?,nonce=?,serialization=?,created_at=? where block="+v.Block+" and idx=?",
				v.Hash, v.Rcpt, v.Nonce, v.Serialization, v.CreatedAt, v.Idx).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableCvmTransactionsTxdata, true, err)
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
		return EventErr(TablePvmBlocks, false, err)
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
			return EventErr(TablePvmBlocks, true, err)
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
		return EventErr(TableRewards, false, err)
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
			return EventErr(TableRewards, true, err)
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
		return EventErr(TableTransactionsValidator, false, err)
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
			return EventErr(TableTransactionsValidator, true, err)
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
		return EventErr(TableTransactionsBlock, false, err)
	}
	if upd {
		_, err = sess.
			Update(TableTransactionsBlock).
			Set("tx_block_id", v.TxBlockID).
			Where("id = ?", v.ID).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableTransactionsBlock, true, err)
		}
	}
	return nil
}

type AddressBech32 struct {
	Address       string
	Bech32Address string
}

func (p *persist) QueryAddressBech32(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *AddressBech32,
) (*AddressBech32, error) {
	v := &AddressBech32{}
	err := sess.Select(
		"address",
		"bech32_address",
	).From(TableAddressBech32).
		Where("address=?", q.Address).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAddressBech32(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *AddressBech32,
	upd bool,
) error {
	var err error
	_, err = sess.
		InsertInto(TableAddressBech32).
		Pair("address", v.Address).
		Pair("bech32_address", v.Bech32Address).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableAddressBech32, false, err)
	}
	if upd {
		_, err = sess.
			Update(TableAddressBech32).
			Set("bech32_address", v.Bech32Address).
			Where("address = ?", v.Address).
			ExecContext(ctx)
		if err != nil {
			return EventErr(TableAddressBech32, true, err)
		}
	}
	return nil
}

type OutputAddressAccumulate struct {
	ID        string
	OutputID  string
	Address   string
	Processed int
	CreatedAt time.Time
}

func (b *OutputAddressAccumulate) ComputeID() error {
	idsv := fmt.Sprintf("%s:%s", b.OutputID, b.Address)
	id, err := ids.ToID(hashing.ComputeHash256([]byte(idsv)))
	if err != nil {
		return err
	}
	b.ID = id.String()
	return nil
}

func (p *persist) QueryOutputAddressAccumulateOut(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *OutputAddressAccumulate,
) (*OutputAddressAccumulate, error) {
	v := &OutputAddressAccumulate{}
	err := sess.Select(
		"id",
		"output_id",
		"address",
		"processed",
		"created_at",
	).From(TableOutputAddressAccumulateOut).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputAddressAccumulateOut(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *OutputAddressAccumulate,
) error {
	var err error
	_, err = sess.
		InsertInto(TableOutputAddressAccumulateOut).
		Pair("id", v.ID).
		Pair("output_id", v.OutputID).
		Pair("address", v.Address).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableOutputAddressAccumulateOut, false, err)
	}

	return nil
}

func (p *persist) QueryOutputAddressAccumulateIn(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *OutputAddressAccumulate,
) (*OutputAddressAccumulate, error) {
	v := &OutputAddressAccumulate{}
	err := sess.Select(
		"id",
		"output_id",
		"address",
		"processed",
		"created_at",
	).From(TableOutputAddressAccumulateIn).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputAddressAccumulateIn(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *OutputAddressAccumulate,
) error {
	var err error
	_, err = sess.
		InsertInto(TableOutputAddressAccumulateIn).
		Pair("id", v.ID).
		Pair("output_id", v.OutputID).
		Pair("address", v.Address).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableOutputAddressAccumulateIn, false, err)
	}

	return nil
}

type OutputTxsAccumulate struct {
	ID            string
	ChainID       string
	AssetID       string
	Address       string
	TransactionID string
	Processed     int
	CreatedAt     time.Time
}

func (b *OutputTxsAccumulate) ComputeID() error {
	idsv := fmt.Sprintf("%s:%s:%s:%s", b.ChainID, b.AssetID, b.Address, b.TransactionID)
	id, err := ids.ToID(hashing.ComputeHash256([]byte(idsv)))
	if err != nil {
		return err
	}
	b.ID = id.String()
	return nil
}

func (p *persist) QueryOutputTxsAccumulate(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *OutputTxsAccumulate,
) (*OutputTxsAccumulate, error) {
	v := &OutputTxsAccumulate{}
	err := sess.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"transaction_id",
		"processed",
		"created_at",
	).From(TableOutputTxsAccumulate).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertOutputTxsAccumulate(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *OutputTxsAccumulate,
) error {
	var err error
	_, err = sess.
		InsertInto(TableOutputTxsAccumulate).
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("asset_id", v.AssetID).
		Pair("address", v.Address).
		Pair("transaction_id", v.TransactionID).
		Pair("created_at", v.CreatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableOutputTxsAccumulate, false, err)
	}

	return nil
}

type AccumulateBalancesAmount struct {
	ID          string
	ChainID     string
	AssetID     string
	Address     string
	TotalAmount string
	UtxoCount   string
	UpdatedAt   time.Time
}

func (b *AccumulateBalancesAmount) ComputeID() error {
	idsv := fmt.Sprintf("%s:%s:%s", b.ChainID, b.AssetID, b.Address)
	id, err := ids.ToID(hashing.ComputeHash256([]byte(idsv)))
	if err != nil {
		return err
	}
	b.ID = id.String()
	return nil
}

func (p *persist) QueryAccumulateBalancesReceived(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *AccumulateBalancesAmount,
) (*AccumulateBalancesAmount, error) {
	v := &AccumulateBalancesAmount{}
	err := sess.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"cast(total_amount as char) total_amount",
		"cast(utxo_count as char) utxo_count",
		"updated_at",
	).From(TableAccumulateBalancesReceived).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAccumulateBalancesReceived(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *AccumulateBalancesAmount,
) error {
	var err error
	_, err = sess.
		InsertInto(TableAccumulateBalancesReceived).
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("asset_id", v.AssetID).
		Pair("address", v.Address).
		Pair("updated_at", v.UpdatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableAccumulateBalancesReceived, false, err)
	}

	return nil
}

func (p *persist) QueryAccumulateBalancesSent(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *AccumulateBalancesAmount,
) (*AccumulateBalancesAmount, error) {
	v := &AccumulateBalancesAmount{}
	err := sess.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"cast(total_amount as char) total_amount",
		"cast(utxo_count as char) utxo_count",
		"updated_at",
	).From(TableAccumulateBalancesSent).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAccumulateBalancesSent(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *AccumulateBalancesAmount,
) error {
	var err error
	_, err = sess.
		InsertInto(TableAccumulateBalancesSent).
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("asset_id", v.AssetID).
		Pair("address", v.Address).
		Pair("updated_at", v.UpdatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableAccumulateBalancesSent, false, err)
	}

	return nil
}

type AccumulateBalancesTransactions struct {
	ID               string
	ChainID          string
	AssetID          string
	Address          string
	TransactionCount string
	UpdatedAt        time.Time
}

func (b *AccumulateBalancesTransactions) ComputeID() error {
	idsv := fmt.Sprintf("%s:%s:%s", b.ChainID, b.AssetID, b.Address)
	id, err := ids.ToID(hashing.ComputeHash256([]byte(idsv)))
	if err != nil {
		return err
	}
	b.ID = id.String()
	return nil
}

func (p *persist) QueryAccumulateBalancesTransactions(
	ctx context.Context,
	sess dbr.SessionRunner,
	q *AccumulateBalancesTransactions,
) (*AccumulateBalancesTransactions, error) {
	v := &AccumulateBalancesTransactions{}
	err := sess.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"cast(transaction_count as char) transaction_count",
		"updated_at",
	).From(TableAccumulateBalancesTransactions).
		Where("id=?", q.ID).
		LoadOneContext(ctx, v)
	return v, err
}

func (p *persist) InsertAccumulateBalancesTransactions(
	ctx context.Context,
	sess dbr.SessionRunner,
	v *AccumulateBalancesTransactions,
) error {
	var err error
	_, err = sess.
		InsertInto(TableAccumulateBalancesTransactions).
		Pair("id", v.ID).
		Pair("chain_id", v.ChainID).
		Pair("asset_id", v.AssetID).
		Pair("address", v.Address).
		Pair("updated_at", v.UpdatedAt).
		ExecContext(ctx)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return EventErr(TableAccumulateBalancesTransactions, false, err)
	}

	return nil
}
