// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"
)

const (
	MaxAggregateIntervalCount = 20000
)

var (
	ErrAggregateIntervalCountTooLarge = errors.New("requesting too many intervals")
)

func (r *DBIndex) GetTxCount() (count int64, err error) {
	err = r.newDBSession("get_tx_count").
		Select("COUNT(1)").
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.String()).
		LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetTx(id ids.ID) (*displayTx, error) {
	tx := &displayTx{}
	err := r.newDBSession("get_tx").
		Select("id", "json_serialization", "created_at").
		From("avm_transactions").
		Where("id = ?", id.Bytes()).
		Where("chain_id = ?", r.chainID.Bytes()).
		Limit(1).
		LoadOne(tx)
	return tx, err
}

func (r *DBIndex) GetTxs(params *ListTxParams) ([]*displayTx, error) {
	builder := params.Apply(r.newDBSession("get_txs").
		Select("id", "json_serialization", "created_at").
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.Bytes()))

	txs := []*displayTx{}
	_, err := builder.Load(&txs)
	return txs, err
}

func (r *DBIndex) GetTxsForAddr(addr ids.ShortID, params *ListTxParams) ([]*displayTx, error) {
	builder := params.Apply(r.newDBSession("get_txs_for_address").
		SelectBySql(`
			SELECT avm_transactions.id, avm_transactions.json_serialization, avm_transactions.created_at
			FROM avm_transactions
			LEFT JOIN avm_outputs ao1 ON ao1.transaction_id = avm_transactions.id OR ao1.redeeming_transaction_id = avm_transactions.id
			LEFT JOIN avm_outputs ao2 ON ao2.transaction_id = ao1.transaction_id
			LEFT JOIN avm_output_addresses AS aoa ON aoa.output_id = ao1.output_id
			WHERE
        avm_transactions.chain_id = ?
        AND
				oa1.output_index < oa2.output_index
				AND
				aoa.address = ?`, r.chainID.String(), addr.String()))

	txs := []*displayTx{}
	_, err := builder.Load(&txs)
	return txs, err
}

func (r *DBIndex) GetTxCountForAddr(addr ids.ShortID) (uint64, error) {
	builder := r.newDBSession("get_tx_count_for_address").
		SelectBySql(`
			SELECT COUNT(DISTINCT(avm_transactions.id))
			FROM avm_transactions
			LEFT JOIN avm_output_addresses AS oa1 ON avm_transactions.id = oa1.transaction_id
			WHERE
        avm_transactions.chain_id = ?
        AND
				oa1.address = ?`, r.chainID.String(), addr.String())

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
			LEFT JOIN avm_outputs ao1 ON ao1.transaction_id = avm_transactions.id
			LEFT JOIN avm_outputs ao2 ON ao2.transaction_id = ao1.transaction_id
			WHERE
        avm_transactions.chain_id = ?
				AND
        ao1.asset_id = ?
        AND
				oa1.output_index < oa2.output_index`,
			assetID.Bytes, r.chainID.String()))

	_, err := builder.Load(&bytes)
	return bytes, err

}

func (r *DBIndex) GetTXO(id ids.ID) (*output, error) {
	out := &output{}
	err := r.newDBSession("get_txo").Select("*").From("avm_outputs").Where("id = ?", id.String()).LoadOne(out)
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
		Where("avm_output_addresses.address = ?", addr.String()).
		Where("avm_transactions.chain_id = ?", r.chainID.String()))

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
		Where("avm_output_addresses.address = ?", addr.String()).
		Where("avm_transactions.chain_id = ?", r.chainID.String())

	if spent != nil {
		if *spent {
			builder = builder.Where("avm_outputs.redeeming_signature IS NOT NULL")
		} else {
			builder = builder.Where("avm_outputs.redeeming_signature IS NULL")
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
		Where("chain_id = ?", r.chainID.String()).
		LoadOne(&count)
	return count, err
}

func (r *DBIndex) GetAssets(params *ListParams) ([]asset, error) {
	// TODO: Add 24h volume and allow sorting by it
	assets := []asset{}
	builder := params.Apply(r.newDBSession("get_assets").
		Select("*").
		From("avm_assets").
		Where("chain_id = ?", r.chainID.String()))
	_, err := builder.Load(&assets)
	return assets, err
}

func (r *DBIndex) GetAsset(aliasOrID string) (asset, error) {
	a := asset{}
	query := r.newDBSession("get_asset").
		Select("*").
		From("avm_assets").
		Where("chain_id = ?", r.chainID.String()).
		Limit(1)

	id, err := ids.FromString(aliasOrID)
	if err != nil {
		query = query.Where("alias = ?", aliasOrID)
	} else {
		query = query.Where("id = ?", id.String())
	}

	err = query.LoadOne(&a)
	return a, err
}

func (r *DBIndex) GetAddressCount() (count int64, err error) {
	err = r.newDBSession("get_address_count").
		Select("COUNT(DISTINCT(address))").
		From("avm_output_addresses").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_output_addresses.transaction_id").
		Where("chain_id = ?", r.chainID.String()).
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
		Where("avm_transactions.chain_id = ?", r.chainID.String())

	if onlySpent {
		builder = builder.Where("avm_outputs.redeeming_transaction_id IS NULL")
	}

	err = builder.LoadOne(&count)
	return count, err
}

func (r *DBIndex) getTransactionCountSince(db *dbr.Session, minutes uint64, assetID ids.ID) (count uint64, err error) {
	builder := db.
		Select("COUNT(DISTINCT(avm_transactions.id))").
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.String())

	if minutes > 0 {
		builder = builder.Where("created_at >= DATE_SUB(NOW(), INTERVAL ? MINUTE)", minutes)
	}

	if !assetID.Equals(ids.Empty) {
		builder = builder.
			LeftJoin("avm_outputs", "avm_outputs.transaction_id = avm_transactions.id").
			Where("avm_outputs.asset_id = ?", assetID.String())
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
		results.Results[0].ResultType = ResultTypeAddress
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

func (r *DBIndex) GetTransactionAggregates(params GetTransactionAggregatesParams) (*TransactionAggregatesHistogram, error) {
	// Validate params and set defaults if necessary
	if params.StartTime.IsZero() {
		var err error
		params.StartTime, err = r.GetFirstTransactionTime()
		if err != nil {
			return nil, err
		}
	}
	if params.EndTime.IsZero() {
		params.EndTime = time.Now().UTC()
	}

	// Ensure the interval count requested isn't too large
	intervalSeconds := int(params.IntervalSize.Seconds())
	requestedIntervalCount := 0
	if intervalSeconds != 0 {
		requestedIntervalCount = int(math.Ceil(params.EndTime.Sub(params.StartTime).Seconds() / params.IntervalSize.Seconds()))
		if requestedIntervalCount > MaxAggregateIntervalCount {
			return nil, ErrAggregateIntervalCountTooLarge
		}
		if requestedIntervalCount < 1 {
			requestedIntervalCount = 1
		}
	}

	// Build the query
	db := r.newDBSession("get_transaction_aggregates_histogram")

	columns := []string{
		"COUNT(DISTINCT(avm_outputs.transaction_id)) AS tx_count",
		"COALESCE(SUM(avm_outputs.amount), 0) AS tx_volume",
		"COUNT(avm_outputs.id) AS outputs_count",
		"COUNT(DISTINCT(avm_output_addresses.address)) AS addr_count",
		"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
	}

	if requestedIntervalCount > 0 {
		columns = append(columns, fmt.Sprintf(
			"FLOOR((UNIX_TIMESTAMP(avm_outputs.created_at)-%d) / %d) AS idx",
			params.StartTime.Unix(),
			intervalSeconds))
	}

	builder := db.
		Select(columns...).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		Where("avm_outputs.created_at >= ?", params.StartTime).
		Where("avm_outputs.created_at < ?", params.EndTime)

	if requestedIntervalCount > 0 {
		builder.
			GroupBy("idx").
			OrderAsc("idx").
			Limit(uint64(requestedIntervalCount))
	}

	// Load data. Intervals without any data will not return anything so we pad
	// our results with empty counts so we're always returning every requested
	// interval. In production this should have rarely if ever but will be common
	// in testing/dev/staging scenarios.
	//
	// We also add the start and end times of each interval to that interval
	intervals := []TransactionAggregatesInterval{}
	_, err := builder.Load(&intervals)
	if err != nil {
		return nil, err
	}

	aggs := &TransactionAggregatesHistogram{
		StartTime:    params.StartTime,
		EndTime:      params.EndTime,
		IntervalSize: params.IntervalSize,
	}

	// If no intervals were requested we're done
	if requestedIntervalCount == 0 {
		if len(intervals) > 0 {
			aggs.Aggregates = intervals[0].Aggregates
		}
		return aggs, nil
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.Aggregates = TransactionAggregates{}

	formatInterval := func(i TransactionAggregatesInterval) TransactionAggregatesInterval {
		// An interval's start time is its index time the interval size, plus the
		// starting time. The end time is the interval size - 1 second after the
		// start time.
		startTimeTS := int64(intervalSeconds)*int64(i.Idx) + params.StartTime.Unix()
		i.StartTime = time.Unix(startTimeTS, 0).UTC()
		i.EndTime = time.Unix(startTimeTS+int64(intervalSeconds)-1, 0).UTC()

		// Add to the overall count
		aggs.Aggregates.TXCount += i.Aggregates.TXCount
		aggs.Aggregates.TXVolume += i.Aggregates.TXVolume
		aggs.Aggregates.OutputCount += i.Aggregates.OutputCount
		aggs.Aggregates.AddrCount += i.Aggregates.AddrCount
		aggs.Aggregates.AssetCount += i.Aggregates.AssetCount

		return i
	}

	padTo := func(slice []TransactionAggregatesInterval, to int) []TransactionAggregatesInterval {
		for len(slice) < to {
			slice = append(slice, formatInterval(TransactionAggregatesInterval{
				Idx: len(slice),
			}))
		}
		return slice
	}

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]TransactionAggregatesInterval, 0, requestedIntervalCount)
	for _, interval := range intervals {
		aggs.Intervals = padTo(aggs.Intervals, interval.Idx)
		aggs.Intervals = append(aggs.Intervals, formatInterval(interval))
	}

	// Add missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	return aggs, nil
}

func (r *DBIndex) GetFirstTransactionTime() (time.Time, error) {
	var ts int64
	err := r.newDBSession("get_first_transaction_time").
		Select("UNIX_TIMESTAMP(MIN(created_at))").
		From("avm_transactions").
		LoadOne(&ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0).UTC(), nil
}
