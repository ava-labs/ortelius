// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"
)

func (r *DBIndex) GetTxCount() (count int64, err error) {
	err = r.newDBSession("get_tx_count").
		Select("COUNT(1)").
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.Bytes()).
		LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetTx(id ids.ID) (*displayTx, error) {
	tx := &displayTx{}
	err := r.newDBSession("get_tx").
		Select("id", "json_serialization", "ingested_at").
		From("avm_transactions").
		Where("id = ?", id.Bytes()).
		Where("chain_id = ?", r.chainID.Bytes()).
		Limit(1).
		LoadOne(tx)
	return tx, err
}

func (r *DBIndex) GetTxs(params *ListTxParams) ([]*displayTx, error) {
	builder := params.Apply(r.newDBSession("get_txs").
		Select("id", "json_serialization", "ingested_at").
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.Bytes()))

	txs := []*displayTx{}
	_, err := builder.Load(&txs)
	return txs, err
}

func (r *DBIndex) GetTxsForAddr(addr ids.ShortID, params *ListTxParams) ([]*displayTx, error) {
	builder := params.Apply(r.newDBSession("get_txs_for_address").
		SelectBySql(`
			SELECT id, json_serialization, ingested_at
			FROM avm_transactions
			LEFT JOIN avm_output_addresses AS oa1 ON avm_transactions.id = oa1.transaction_id
			LEFT JOIN avm_output_addresses AS oa2 ON avm_transactions.id = oa2.transaction_id
			WHERE
        avm_transactions.chain_id = ?
        AND
				oa1.output_index < oa2.output_index
				AND
				oa1.address = ?`, r.chainID.Bytes(), addr.Bytes()))

	txs := []*displayTx{}
	_, err := builder.Load(&txs)
	return txs, err
}

func (r *DBIndex) GetTxCountForAddr(addr ids.ShortID) (uint64, error) {
	builder := r.newDBSession("get_tx_count_for_address").
		SelectBySql(`
			SELECT COUNT(DISTINCT(avm_output_addresses))
			FROM avm_transactions
			LEFT JOIN avm_output_addresses AS oa1 ON avm_transactions.id = oa1.transaction_id
			WHERE
        avm_transactions.chain_id = ?
        AND
				oa1.address = ?`, r.chainID.Bytes(), addr.Bytes())

	var count uint64
	err := builder.LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetTxsForAsset(assetID ids.ID, params *ListTxParams) ([]json.RawMessage, error) {
	bytes := []json.RawMessage{}
	builder := params.Apply(r.newDBSession("get_txs_for_asset").
		SelectBySql(`
			SELECT avm_transactions.canonical_serialization
			FROM avm_transactions
			LEFT JOIN avm_output_addresses AS oa1 ON avm_transactions.id = oa1.transaction_id
			LEFT JOIN avm_output_addresses AS oa2 ON avm_transactions.id = oa2.transaction_id
			LEFT JOIN avm_outputs ON avm_outputs.transaction_id = oa1.transaction_id AND avm_outputs.output_index = oa1.output_index
			WHERE
        avm_outputs.asset_id = ?
        AND
        avm_transactions.chain_id = ?
        AND
				oa1.output_index < oa2.output_index`,
			assetID.Bytes, r.chainID.Bytes()))

	_, err := builder.Load(&bytes)
	return bytes, err

}

func (r *DBIndex) GetTXO(id ids.ID) (*output, error) {
	out := &output{}
	err := r.newDBSession("get_txo").Select("*").From("avm_outputs").Where("id = ?", id.Bytes()).LoadOne(out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *DBIndex) GetTXOsForAddr(addr ids.ShortID, params *ListTXOParams) ([]output, error) {
	builder := params.Apply(r.newDBSession("get_transaction").
		Select("*").
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_outputs.transaction_id = avm_output_addresses.transaction_id").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_output_addresses.transaction_id").
		Where("avm_output_addresses.address = ?", addr.Bytes()).
		Where("avm_transactions.chain_id = ?", r.chainID.Bytes()))

	if params.spent != nil {
		if *params.spent {
			builder = builder.Where("avm_outputs.redeeming_transaction_id IS NOT NULL")
		} else {
			builder = builder.Where("avm_outputs.redeeming_transaction_id IS NULL")
		}
	}

	// TODO: Get addresses and add to outputs
	outputs := []output{}
	_, err := builder.Load(&outputs)
	return outputs, err
}

func (r *DBIndex) GetTXOCountAndValueForAddr(addr ids.ShortID, spent *bool) (uint64, uint64, error) {
	builder := r.newDBSession("get_txo_count_for_addr").
		Select("COUNT(1) AS count", "SUM(avm_outputs.amount) AS value").
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_outputs.transaction_id = avm_output_addresses.transaction_id").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_output_addresses.transaction_id").
		Where("avm_output_addresses.address = ?", addr.Bytes()).
		Where("avm_transactions.chain_id = ?", r.chainID.Bytes())

	if spent != nil {
		if *spent {
			builder = builder.Where("avm_outputs.redemming_signature IS NOT NULL")
		} else {
			builder = builder.Where("avm_outputs.redemming_signature IS NULL")
		}
	}

	results := &struct {
		Count uint64
		Value uint64
	}{}
	err := builder.LoadOne(&results)
	if err != nil {
		return 0, 0, nil
	}
	return results.Count, results.Value, nil
}

func (r *DBIndex) GetAssetCount() (count int64, err error) {
	err = r.newDBSession("get_asset_count").
		Select("COUNT(1)").
		From("avm_assets").
		Where("chain_id = ?", r.chainID.Bytes()).
		LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetAssets(params *ListParams) ([]asset, error) {
	assets := []asset{}
	builder := params.Apply(r.newDBSession("get_assets").
		Select("*").
		From("avm_assets").
		Where("chain_id = ?", r.chainID.Bytes()))
	_, err := builder.Load(&assets)
	return assets, err
}

func (r *DBIndex) GetAsset(aliasOrID string) (asset, error) {
	a := asset{}
	query := r.newDBSession("get_asset").
		Select("*").
		From("avm_assets").
		Where("chain_id = ?", r.chainID.Bytes()).
		Limit(1)

	id, err := ids.FromString(aliasOrID)
	if err != nil {
		query = query.Where("alias = ?", aliasOrID)
	} else {
		query = query.Where("id = ?", id.Bytes())
	}

	err = query.LoadOne(&a)
	return a, err
}

func (r *DBIndex) GetAddressCount() (count int64, err error) {
	err = r.newDBSession("get_address_count").
		Select("COUNT(DISTINCT(address))").
		From("avm_output_addresses").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_output_addresses.transaction_id").
		Where("chain_id = ?", r.chainID.Bytes()).
		LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetAddress(id ids.ShortID) (*address, error) {
	spent := false
	utxos, err := r.GetTXOsForAddr(id, &ListTXOParams{
		spent:      &spent,
		ListParams: &ListParams{limit: 50},
	})
	if err != nil {
		return nil, err
	}

	utxoCount, utxosValue, err := r.GetTXOCountAndValueForAddr(id, &spent)
	if err != nil {
		return nil, err
	}

	_, ltvValue, err := r.GetTXOCountAndValueForAddr(id, nil)
	if err != nil {
		return nil, err
	}

	txCount, err := r.GetTxCountForAddr(id)
	if err != nil {
		return nil, err
	}

	return &address{
		ID:               id,
		TransactionCount: txCount,

		Balance: utxosValue,
		LTV:     ltvValue,

		UTXOCount: utxoCount,
		UTXOs:     utxos,
	}, nil
}

func (r *DBIndex) GetTransactionOutputCount(onlySpent bool) (count int64, err error) {
	builder := r.newDBSession("get_address_count").
		Select("COUNT(1)").
		From("avm_outputs").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_outputs.transaction_id").
		Where("avm_transactions.chain_id = ?", r.chainID.Bytes())

	if onlySpent {
		builder = builder.Where("avm_outputs.redeeming_transaction_id IS NULL")
	}

	err = builder.LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetTxCounts(assetID ids.ID) (counts *transactionCounts, err error) {
	db := r.newDBSession("get_tx_counts")
	counts = &transactionCounts{}

	if counts.Minute, err = r.getTransactionCountSince(db, 1, assetID); err != nil {
		return nil, err
	}
	if counts.Hour, err = r.getTransactionCountSince(db, 60, assetID); err != nil {
		return nil, err
	}
	if counts.Day, err = r.getTransactionCountSince(db, 1440, assetID); err != nil {
		return nil, err
	}
	if counts.Week, err = r.getTransactionCountSince(db, 10080, assetID); err != nil {
		return nil, err
	}
	if counts.Month, err = r.getTransactionCountSince(db, 43200, assetID); err != nil {
		return nil, err
	}
	if counts.Year, err = r.getTransactionCountSince(db, 525600, assetID); err != nil {
		return nil, err
	}
	if counts.All, err = r.getTransactionCountSince(db, 0, assetID); err != nil {
		return nil, err
	}
	return counts, nil
}

func (r *DBIndex) getTransactionCountSince(db *dbr.Session, minutes uint64, assetID ids.ID) (count uint64, err error) {
	builder := db.
		Select("COUNT(DISTINCT(avm_transactions.id))").
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.Bytes())

	if minutes > 0 {
		builder = builder.Where("ingested_at >= DATE_SUB(NOW(), INTERVAL ? MINUTE)", minutes)
	}

	if !assetID.Equals(ids.Empty) {
		builder = builder.
			LeftJoin("avm_outputs", "avm_outputs.transaction_id = avm_transactions.id").
			Where("avm_outputs.asset_id = ?", assetID.Bytes())
	}

	err = builder.LoadOne(&count)
	return count, err
}

func (r *DBIndex) Search(params SearchParams) (*searchResults, error) {
	results := &searchResults{
		Count:   1,
		Results: make([]searchResult, 1),
	}

	// If the ID is a shortID then it must be an address
	shortID, err := ids.ShortFromString(params.Query)
	if err == nil {
		addr, err := r.GetAddress(shortID)
		if err != nil {
			return nil, err
		}
		results.Results[0].ResultType = ResultTypeTx
		results.Results[0].Data = addr
		return results, nil
	}

	// If query isn't an ID string then there's nothing to find
	id, err := ids.FromString(params.Query)
	if err != nil {
		return nil, nil
	}

	tx, err := r.GetTx(id)
	if err == nil {
		results.Results[0].ResultType = ResultTypeTx
		results.Results[0].Data = tx
		return results, nil
	}

	txo, err := r.GetTXO(id)
	if err == nil {
		results.Results[0].ResultType = ResultTypeOutput
		results.Results[0].Data = txo
		return results, nil
	}

	return nil, nil
}
