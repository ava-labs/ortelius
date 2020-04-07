// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/math"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"
)

const (
	// MaxCanonicalSerializationLen is the maximum number of bytes a canonically
	// serialized tx can be stored as in the database.
	MaxCanonicalSerializationLen = 16_384
)

var (
	ErrCanonicalSerializationTooLong = errors.New("canonical serialization is too long")
)

// DB is a services.Accumulator backed by redis
type DB struct {
	stream *health.Stream
	db     *dbr.Connection
}

// NewDB creates a new DB for the given config
func NewDB(stream *health.Stream, db *dbr.Connection) *DB {
	return &DB{stream, db}
}

// AddTx ingests a transaction and adds it to the services
func (r *DB) AddTx(chainID ids.ID, txID ids.ID, body []byte) error {
	// Create db tx
	sess := r.newDBSession("add_tx")
	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}

	// Ingest the tx and commit
	defer dbTx.RollbackUnlessCommitted()
	if err = ingestTx(dbTx, chainID, txID, body); err != nil {
		return err
	}
	return dbTx.Commit()
}

// GetRecentTxs returns a list of the N most recent transactions
func (r *DB) GetTxCount() (count int64, err error) {
	sess := r.newDBSession("get_transaction")
	return count, sess.
		Select("COUNT(1)").
		From("avm_transactions").
		LoadOne(&count)
}

func (r *DB) GetTx(chainID ids.ID, _ ids.ID) ([]byte, error) {
	bytes := []byte{}
	err := r.newDBSession("get_tx").
		Select("canonical_serialization").
		From("avm_transactions").
		Where("chain_id = ?", chainID.Bytes()).
		Limit(1).
		LoadOne(&bytes)
	return bytes, err
}

func (r *DB) GetTxsForAddr(chainID ids.ID, addr ids.ShortID) ([]json.RawMessage, error) {
	sess := r.newDBSession("get_txs_for_address")

	bytes := []json.RawMessage{}
	_, err := sess.
		SelectBySql(`
			SELECT avm_transactions.canonical_serialization
			FROM avm_transactions
			LEFT JOIN avm_output_addresses AS oa1 ON avm_transactions.id = oa1.transaction_id
			LEFT JOIN avm_output_addresses AS oa2 ON avm_transactions.id = oa2.transaction_id
			WHERE
        avm_transactions.chain_id = ?
        AND
				oa1.output_index < oa2.output_index
				AND
				oa1.address = ?`, chainID.Bytes(), addr.Bytes()).
		Load(&bytes)
	return bytes, err
}

func (r *DB) GetTXOsForAddr(chainID ids.ID, addr ids.ShortID, spent *bool) ([]output, error) {
	sess := r.newDBSession("get_transaction")

	builder := sess.
		Select("*").
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_outputs.transaction_id = avm_output_addresses.transaction_id").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_output_addresses.transaction_id").
		Where("avm_output_addresses.address = ?", addr.Bytes()).
		Where("avm_transactions.chain_id = ?", chainID.Bytes())

	if spent != nil {
		builder = builder.Where("spent = ?", *spent)
	}

	outputs := []output{}
	_, err := builder.Load(&outputs)

	// TODO: Get addresses and add to outputs

	return outputs, err
}

// GetRecentTxs returns a list of the N most recent transactions
func (r *DB) GetRecentTxs(_ ids.ID, _ int64) ([]ids.ID, error) {
	return nil, nil
}

func (r *DB) newDBSession(name string) *dbr.Session {
	return r.db.NewSession(r.stream.NewJob(name))
}

func ingestTx(db dbr.SessionRunner, chainID ids.ID, txID ids.ID, body []byte) error {
	// Parse the body into an AVM transfer tx
	if len(body) > MaxCanonicalSerializationLen {
		return ErrCanonicalSerializationTooLong
	}
	tx := &AVMTransferTx{}
	if err := json.Unmarshal(body, tx); err != nil {
		return err
	}

	// Process tx inputs by calculating the tx volume and marking the outpoints
	// as spent
	var (
		err   error
		total uint64 = 0
	)

	redeemOutputsConditions := []dbr.Builder{}
	for _, in := range tx.UnsignedTx.Inputs {
		total, err = math.Add64(total, in.Input.Amount)
		if err != nil {
			return err
		}

		txID, err := ids.FromString(in.TxID)
		if err != nil {
			return err
		}

		redeemOutputsConditions = append(redeemOutputsConditions, dbr.And(
			dbr.Expr("transaction_id = ?", txID.Bytes()),
			dbr.Eq("output_index", in.OutputIndex),
		))

		// db.Update("output_addresses").Set("redeeming_signature", in.In.SigIndices)
	}

	_, err = db.
		Update("avm_outputs").
		Set("redeeming_transaction_id", txID.Bytes()).
		Where(dbr.Or(redeemOutputsConditions...)).
		Exec()
	if err != nil {
		return err
	}

	// Add tx to the table
	_, err = db.
		InsertInto("avm_transactions").
		Pair("id", txID.Bytes()).
		Pair("network_id", 12345).
		Pair("chain_id", chainID.Bytes()).
		Pair("canonical_serialization", body).
		Pair("input_count", len(tx.UnsignedTx.Inputs)).
		Pair("output_count", len(tx.UnsignedTx.Outputs)).
		Pair("amount", total).
		Exec()
	if err != nil && !errIsNotDuplicateEntryError(err) {
		return err
	}

	// Process tx outputs by adding to the outputs table
	for idx, out := range tx.UnsignedTx.Outputs {
		assetID, err := ids.FromString(out.AssetID)
		if err != nil {
			return err
		}

		_, err = db.
			InsertInto("avm_outputs").
			Pair("transaction_id", txID.Bytes()).
			Pair("output_index", idx).
			Pair("asset_id", assetID.Bytes()).
			Pair("output_type", AVMOutputTypesSECP2556K1Transfer).
			Pair("amount", out.Output.Amount).
			Pair("locktime", out.Output.Locktime).
			Pair("threshold", out.Output.Threshold).
			Exec()
		if err != nil && !errIsNotDuplicateEntryError(err) {
			return err
		}

		for _, addr := range out.Output.Addresses {
			addrID, err := ids.ShortFromString(addr)
			if err != nil {
				return err
			}
			_, err = db.
				InsertInto("avm_output_addresses").
				Pair("transaction_id", txID.Bytes()).
				Pair("output_index", idx).
				Pair("address", addrID.Bytes()).
				Exec()
			if err != nil && !errIsNotDuplicateEntryError(err) {
				return err
			}
		}
	}
	return nil
}

func errIsNotDuplicateEntryError(err error) bool {
	return strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}
