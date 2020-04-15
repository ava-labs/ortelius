// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/crypto"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/ava-labs/gecko/vms/components/codec"
	"github.com/ava-labs/gecko/vms/platformvm"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"
)

const (
	// MaxSerializationLen is the maximum number of bytes a canonically
	// serialized tx can be stored as in the database.
	MaxSerializationLen = 16_384

	PaginationLimit = 500
)

var (
	ErrSerializationTooLong = errors.New("serialization is too long")
)

// DBIndex is a services.Accumulator backed by a rdmbs
type DBIndex struct {
	chainID ids.ID
	codec   codec.Codec
	stream  *health.Stream
	db      *dbr.Connection
}

// NewDBIndex creates a new DBIndex for the given config
func NewDBIndex(stream *health.Stream, db *dbr.Connection, chainID ids.ID, codec codec.Codec) *DBIndex {
	return &DBIndex{
		stream:  stream,
		db:      db,
		chainID: chainID,
		codec:   codec,
	}
}

//
// Getters
//

type validator struct{}

func (i *DBIndex) GetValidatorsForSubnet(id ids.ID) ([]validator, error) {
	validators := []validator{}
	_, err := i.newDBSession("get_validators").
		Select("*").
		From("pvm_validators").
		Where("subnet_id = ?", id.Bytes()).
		Limit(100).
		Load(&validators)
	return validators, err
}

func (i *DBIndex) GetCurrentValidatorsForSubnet(id ids.ID) (interface{}, error) {
	now := time.Now()
	validators := []validator{}
	_, err := i.newDBSession("get_validators").
		Select("*").
		From("pvm_validators").
		Where("subnet_id = ?", id.Bytes()).
		Where("start_time <= ?", now).
		Where("end_time > ?", now).
		Limit(100).
		Load(&validators)
	return validators, err
}

//
// Ingestion routines
//

type ingestableTx struct {
	ID                     []byte
	ChainID                []byte
	TxType                 TxType
	Amount                 uint64
	SenderAccount          []byte
	SenderSig              []byte
	Nonce                  uint64
	CanonicalSerialization []byte
	JSONSerialization      []byte
	IngestedAt             time.Time
}

func (r *DBIndex) ingestCreateChainTx(tx *platformvm.CreateChainTx) (err error) {
	txBytes, err := r.codec.Marshal(tx)
	if err != nil {
		return err
	}

	txJSONBytes, err := json.Marshal(tx)
	if err != nil {
		return err
	}

	txID := tx.ID()
	if txID.IsZero() {
		txID = ids.NewID(hashing.ComputeHash256Array(txBytes))
	}

	payerAddr := []byte("genesis")
	if !tx.PayerAddress.IsZero() {
		payerAddr = tx.PayerAddress.Bytes()
	}

	db := r.db.NewSession(r.stream.NewJob("ingest_create_chain_tx"))
	_, err = db.
		InsertInto("pvm_transactions").
		Pair("id", txID.Bytes()).
		Pair("chain_id", r.chainID.Bytes()).
		Pair("type", TxTypeCreateChainTx).
		Pair("account", payerAddr).
		Pair("signature", tx.PayerSig[:]).
		Pair("amount", 0).
		Pair("nonce", tx.Nonce).
		Pair("canonical_serialization", txBytes).
		Pair("json_serialization", txJSONBytes).
		Pair("ingested_at", time.Now()).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		panic(err)
		return err
	}

	// Add to chains table
	_, err = db.
		InsertInto("pvm_chains").
		Pair("transaction_id", txID.Bytes()).
		Pair("subnet_id", tx.SubnetID.Bytes()).
		Pair("name", tx.ChainName).
		Pair("vm_type", tx.VMID.Bytes()).
		Pair("genesis_data", tx.GenesisData).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		panic(err)
		return err
	}

	// Add chain fx ids
	for _, fxID := range tx.FxIDs {
		_, err = db.
			InsertInto("pvm_chains_fx_ids").
			Pair("chain_id", r.chainID.Bytes()).
			Pair("fx_id", fxID.Bytes()).
			Exec()
		if err != nil && !errIsNotDuplicateEntryError(err) {
			return err
		}

	}
	return nil
}

func (r *DBIndex) ingestableFromAddDefaultSubnetValidatorTx(tx *platformvm.AddDefaultSubnetValidatorTx) (*ingestableTx, error) {
	txBytes, err := r.codec.Marshal(tx)
	if err != nil {
		return nil, err
	}

	txJSONBytes, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}

	txID := tx.ID()
	if txID.IsZero() {
		txID = ids.NewID(hashing.ComputeHash256Array(txBytes))
	}

	return &ingestableTx{
		ID:                     txID.Bytes(),
		ChainID:                r.chainID.Bytes(),
		TxType:                 TxTypeAddDefaultSubnetValidator,
		SenderAccount:          []byte{},
		SenderSig:              tx.Sig[:],
		Nonce:                  tx.Nonce,
		CanonicalSerialization: txBytes,
		JSONSerialization:      txJSONBytes,
		IngestedAt:             time.Now(),
	}, nil
}

func (r *DBIndex) ingestableFromAddNonDefaultSubnetValidatorTx(tx *platformvm.AddNonDefaultSubnetValidatorTx) (*ingestableTx, error) {
	txBytes, err := r.codec.Marshal(tx)
	if err != nil {
		return nil, err
	}

	txJSONBytes, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}

	factory := crypto.FactorySECP256K1R{}
	pubkey, err := factory.RecoverPublicKey(txBytes, tx.PayerSig[:])
	if err != nil {
		panic(err)
		return nil, err
	}

	return &ingestableTx{
		ID:                     tx.ID().Bytes(),
		ChainID:                r.chainID.Bytes(),
		TxType:                 TxTypeAddDefaultSubnetValidator,
		SenderAccount:          pubkey.Address().Bytes(),
		SenderSig:              tx.PayerSig[:],
		Nonce:                  tx.Nonce,
		CanonicalSerialization: txBytes,
		JSONSerialization:      txJSONBytes,
	}, nil
}

func (r *DBIndex) ingestAddDefaultSubnetValidatorTx(tx *platformvm.AddDefaultSubnetValidatorTx) (err error) {
	it, err := r.ingestableFromAddDefaultSubnetValidatorTx(tx)
	if err != nil {
		return err
	}
	return r.ingestTx(it)
}

func (r *DBIndex) ingestAddNonDefaultSubnetValidatorTx(tx *platformvm.AddNonDefaultSubnetValidatorTx) (err error) {
	it, err := r.ingestableFromAddNonDefaultSubnetValidatorTx(tx)
	if err != nil {
		return err
	}
	return r.ingestTx(it)
}

func (r *DBIndex) ingestTx(tx *ingestableTx) error {
	_, err := r.db.NewSession(r.stream.NewJob("ingest_tx")).
		InsertInto("pvm_transactions").
		Pair("id", tx.ID).
		Pair("chain_id", tx.ChainID).
		Pair("type", tx.TxType).
		Pair("account", tx.SenderAccount).
		Pair("amount", tx.Amount).
		Pair("nonce", tx.Nonce).
		Pair("signature", tx.SenderSig).
		Pair("canonical_serialization", tx.CanonicalSerialization).
		Pair("json_serialization", tx.JSONSerialization).
		Pair("ingested_at", tx.IngestedAt).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err

	}
	return nil
}

// func (r *DBIndex) ingestAddNonDefaultSubnetValidatorTx(tx *platformvm.AddNonDefaultSubnetValidatorTx) (err error) {
// 	return r.ingestAddValidatorTx(tx, TxTypeAddNonDefaultSubnetValidator, tx.PayerSig[:])
// }

func (r *DBIndex) ingestAddValidatorTx(tx *platformvm.AddNonDefaultSubnetValidatorTx, typ TxType, signature []byte) (err error) {
	txBytes, err := r.codec.Marshal(tx)
	if err != nil {
		return err
	}

	txJSONBytes, err := json.Marshal(tx)
	if err != nil {
		return err
	}

	db := r.db.NewSession(r.stream.NewJob("ingest_add_validator_tx"))
	_, err = db.
		InsertInto("pvm_transactions").
		Pair("id", tx.ID()).
		Pair("chain_id", r.chainID.Bytes()).
		Pair("type", typ).
		Pair("account", tx.Vdr().ID()).
		Pair("amount", 0).
		Pair("nonce", tx.Nonce).
		Pair("signature", signature).
		Pair("canonical_serialization", txBytes).
		Pair("json_serialization", txJSONBytes).
		Pair("ingested_at", time.Now().Unix()).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	// Add to chains table
	_, err = db.
		InsertInto("pvm_validators").
		Pair("node_id", tx.Vdr().ID().Bytes()).
		Pair("subnet_id", tx.SubnetID().Bytes()).
		Pair("start_time", tx.StartTime).
		Pair("end_time", tx.EndTime).
		Pair("weight", tx.Weight).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	return nil
}

func (r *DBIndex) ingestAddDefaultSubnetDelegatorTx(tx *platformvm.UnsignedAddDefaultSubnetDelegatorTx, typ TxType) (err error) {
	txBytes, err := r.codec.Marshal(tx)
	if err != nil {
		return err
	}

	txJSONBytes, err := json.Marshal(tx)
	if err != nil {
		return err
	}

	db := r.db.NewSession(r.stream.NewJob("ingest_add_validator_tx"))
	_, err = db.
		InsertInto("pvm_transactions").
		Pair("id", tx.ID()).
		Pair("chain_id", r.chainID.Bytes()).
		Pair("type", TxTypeAddDefaultSubnetDelegator).
		Pair("account", tx.Vdr().ID()).
		Pair("amount", 0).
		Pair("nonce", tx.Nonce).
		Pair("canonical_serialization", txBytes).
		Pair("json_serialization", txJSONBytes).
		Pair("ingested_at", time.Now().Unix()).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	// Add to chains table
	_, err = db.
		InsertInto("pvm_validators").
		Pair("node_id", tx.Vdr().ID().Bytes()).
		Pair("subnet_id", ids.Empty.Bytes()).
		Pair("start_time", tx.StartTime).
		Pair("end_time", tx.EndTime).
		Pair("weight", tx.Weight).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	return nil
}

func (r *DBIndex) ingestAtomicBlock(blk *platformvm.AtomicBlock) error {
	blk.Tx
	// _, err := r.db.NewSession(r.stream.NewJob("ingest_tx")).
	// 	InsertInto("pvm_transactions").
	// 	Pair("id", tx.ID).
	// 	Pair("chain_id", tx.ChainID).
	// 	Pair("type", tx.TxType).
	// 	Pair("account", tx.SenderAccount).
	// 	Pair("amount", tx.Amount).
	// 	Pair("nonce", tx.Nonce).
	// 	Pair("signature", tx.SenderSig).
	// 	Pair("canonical_serialization", tx.CanonicalSerialization).
	// 	Pair("json_serialization", tx.JSONSerialization).
	// 	Pair("ingested_at", tx.IngestedAt).
	// 	Exec()
	// if err != nil && !errIsNotDuplicateEntryError(err) {
	// 	return err
	// }

	return nil
}

func (i *DBIndex) newDBSession(name string) *dbr.Session {
	return i.db.NewSession(i.stream.NewJob(name))
}

func errIsNotDuplicateEntryError(err error) bool {
	return strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}
