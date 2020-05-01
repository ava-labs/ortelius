// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"errors"
	"fmt"
	"math"
	"sort"
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

func (r *DB) Search(params SearchParams) (*SearchResults, error) {
	// Get search results for each class of object
	transactionResults, err := r.ListTransactions(&ListTransactionsParams{
		ListParams: params.ListParams,
		Query:      params.Query,
	})
	if err != nil {
		return nil, err
	}

	assetResults, err := r.ListAssets(&ListAssetsParams{
		ListParams: params.ListParams,
		Query:      params.Query,
	})
	if err != nil {
		return nil, err
	}

	addressResults, err := r.ListAddresses(&ListAddressesParams{
		ListParams: params.ListParams,
		Query:      params.Query,
	})
	if err != nil {
		return nil, err
	}

	outputResults, err := r.ListOutputs(&ListOutputsParams{
		ListParams: params.ListParams,
		Query:      params.Query,
	})
	if err != nil {
		return nil, err
	}

	// Build overall SearchResults object from our pieces
	returnedResultCount := len(assetResults.Assets) + len(addressResults.Addresses) + len(transactionResults.Transactions) + len(outputResults.Outputs)
	if returnedResultCount > PaginationMaxLimit {
		returnedResultCount = PaginationMaxLimit
	}
	results := &SearchResults{
		// The overall count is the sum of the counts for each class
		Count: assetResults.Count + addressResults.Count + transactionResults.Count + outputResults.Count,

		// Create a container for our combined results
		Results: make([]SearchResult, 0, returnedResultCount),
	}

	// Add each result to the list
	for _, result := range assetResults.Assets {
		results.Results = append(results.Results, SearchResult{
			SearchResultType: ResultTypeAsset,
			Data:             result,
			Score:            result.Score,
		})
	}
	for _, result := range addressResults.Addresses {
		results.Results = append(results.Results, SearchResult{
			SearchResultType: ResultTypeAddress,
			Data:             result,
			Score:            result.Score,
		})
	}
	for _, result := range transactionResults.Transactions {
		results.Results = append(results.Results, SearchResult{
			SearchResultType: ResultTypeTransaction,
			Data:             result,
			Score:            result.Score,
		})
	}
	for _, result := range outputResults.Outputs {
		results.Results = append(results.Results, SearchResult{
			SearchResultType: ResultTypeOutput,
			Data:             result,
			Score:            result.Score,
		})
	}

	// Sort each result against each other now that they're in a single list
	sort.Sort(results.Results)
	results.Results = results.Results[:returnedResultCount]

	return results, nil
}

func (r *DB) Aggregate(params AggregateParams) (*AggregatesHistogram, error) {
	// Validate params and set defaults if necessary
	if params.StartTime.IsZero() {
		var err error
		params.StartTime, err = r.getFirstTransactionTime()
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
	db := r.newSession("get_transaction_aggregates_histogram")

	columns := []string{
		"COALESCE(SUM(avm_outputs.amount), 0) AS transaction_volume",

		"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
		"COUNT(DISTINCT(avm_output_addresses.address)) AS address_count",
		"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
		"COUNT(avm_outputs.id) AS output_count",
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
	intervals := []Aggregates{}
	_, err := builder.Load(&intervals)
	if err != nil {
		return nil, err
	}

	aggs := &AggregatesHistogram{
		IntervalSize: params.IntervalSize,
	}

	// If no intervals were requested we're done
	if requestedIntervalCount == 0 {
		if len(intervals) > 0 {
			aggs.Aggregates = intervals[0]
		}
		return aggs, nil
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.Aggregates = Aggregates{
		StartTime: params.StartTime,
		EndTime:   params.EndTime,
	}

	formatInterval := func(iAgg Aggregates) Aggregates {
		// An interval's start time is its index time the interval size, plus the
		// starting time. The end time is the interval size - 1 second after the
		// start time.
		startTimeTS := int64(intervalSeconds)*int64(iAgg.Idx) + params.StartTime.Unix()
		iAgg.StartTime = time.Unix(startTimeTS, 0).UTC()
		iAgg.EndTime = time.Unix(startTimeTS+int64(intervalSeconds)-1, 0).UTC()

		// Add to the overall count
		aggs.Aggregates.TransactionCount += iAgg.TransactionCount
		aggs.Aggregates.TransactionVolume += iAgg.TransactionVolume
		aggs.Aggregates.OutputCount += iAgg.OutputCount
		aggs.Aggregates.AddressCount += iAgg.AddressCount
		aggs.Aggregates.AssetCount += iAgg.AssetCount

		return iAgg
	}

	padTo := func(slice []Aggregates, to int) []Aggregates {
		for len(slice) < to {
			slice = append(slice, formatInterval(Aggregates{
				Idx: len(slice),
			}))
		}
		return slice
	}

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]Aggregates, 0, requestedIntervalCount)
	for _, interval := range intervals {
		aggs.Intervals = padTo(aggs.Intervals, interval.Idx)
		aggs.Intervals = append(aggs.Intervals, formatInterval(interval))
	}

	// Add missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	return aggs, nil
}

func (r *DB) ListTransactions(params *ListTransactionsParams) (*TransactionList, error) {
	db := r.newSession("get_transactions")

	columns := []string{"avm_transactions.id", "avm_transactions.chain_id", "avm_transactions.type", "avm_transactions.created_at", "avm_transactions.json_serialization AS raw_message"}

	if params.Query != "" {
		scoreColumn, err := r.newScoreColumnString("avm_transactions.id", params.Query)
		if err != nil {
			return nil, err
		}
		columns = append(columns, scoreColumn)
	}

	// Load the base transactions
	txs := []*Transaction{}
	builder := params.Apply(db.
		Select(columns...).
		From("avm_transactions").
		Where("chain_id = ?", r.chainID.String()))

	if params.Query != "" {
		builder = builder.
			Where("INSTR(avm_transactions.id, ?) > 0", params.Query).
			OrderAsc("score")
	}

	_, err := builder.Load(&txs)
	if err != nil {
		return nil, err
	}

	count := uint64(params.Offset) + uint64(len(txs))
	if len(txs) >= params.Limit {
		params.ListParams = ListParams{}
		err = params.Apply(db.
			Select("COUNT(avm_transactions.id)").
			From("avm_transactions").
			Where("chain_id = ?", r.chainID.String())).
			LoadOne(&count)
		if err != nil {
			return nil, err
		}
	}

	// Add all the addition information we might want
	if err = r.dressTransactions(db, txs); err != nil {
		return nil, err
	}

	return &TransactionList{ListMetadata{count}, txs}, nil
}

func (r *DB) ListAssets(params *ListAssetsParams) (*AssetList, error) {
	columns := []string{"avm_assets.*"}

	if params.Query != "" {
		scoreColumn, err := r.newScoreColumnString("avm_assets.id", params.Query)
		if err != nil {
			return nil, err
		}
		columns = append(columns, scoreColumn)
	}

	db := r.newSession("list_assets")
	assets := []*Asset{}
	builder := params.Apply(db.
		Select(columns...).
		From("avm_assets").
		Where("chain_id = ?", r.chainID.String()))

	if params.Query != "" {
		builder = builder.
			Where("INSTR(avm_assets.id, ?) > 0", builder.Query).
			OrderAsc("score")
	}

	_, err := builder.Load(&assets)

	count := uint64(params.Offset) + uint64(len(assets))
	if len(assets) >= params.Limit {
		params.ListParams = ListParams{}
		err = params.Apply(db.
			Select("COUNT(avm_assets.id)").
			From("avm_assets").
			Where("chain_id = ?", r.chainID.String())).
			LoadOne(&count)
		if err != nil {
			return nil, err
		}
	}

	return &AssetList{ListMetadata{count}, assets}, err
}

func (r *DB) ListAddresses(params *ListAddressesParams) (*AddressList, error) {
	db := r.newSession("list_addresses")

	columns := []string{
		"avm_output_addresses.address",
		"MIN(avm_transactions.chain_id) AS chain_id",
		"addresses.public_key",

		"COUNT(avm_transactions.id) AS transaction_count",
		"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
		"COALESCE(SUM(CASE WHEN avm_outputs.redeeming_transaction_id != '' THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
		"COALESCE(SUM(CASE WHEN avm_outputs.redeeming_transaction_id = '' THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
		"COALESCE(SUM(CASE WHEN avm_outputs.redeeming_transaction_id = '' THEN 1 ELSE 0 END), 0) AS utxo_count",
	}

	if params.Query != "" {
		scoreColumn, err := r.newScoreColumnString("avm_output_addresses.address", params.Query)
		if err != nil {
			return nil, err
		}
		columns = append(columns, scoreColumn)
	}

	addresses := []*Address{}
	builder := params.Apply(db.
		Select(columns...).
		From("avm_output_addresses").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address").
		LeftJoin("avm_outputs", "avm_output_addresses.output_id = avm_outputs.id").
		LeftJoin("avm_transactions", "avm_outputs.transaction_id = avm_transactions.id OR avm_outputs.redeeming_transaction_id = avm_transactions.id").
		Where("avm_transactions.chain_id = ?", r.chainID.String()).
		GroupBy("avm_output_addresses.address"))

	if params.Query != "" {
		builder = builder.
			Where("INSTR(avm_output_addresses.Address, ?) > 0", params.Query).
			OrderAsc("score")
	}

	_, err := builder.Load(&addresses)
	if err != nil {
		return nil, err
	}

	count := uint64(params.Offset) + uint64(len(addresses))
	if len(addresses) >= params.Limit {
		params.ListParams = ListParams{}
		err = params.Apply(db.
			Select("COUNT(avm_output_addresses.address)").
			From("avm_output_addresses").
			LeftJoin("addresses", "addresses.address = avm_output_addresses.address").
			LeftJoin(dbr.I("avm_outputs").As("all_outputs"), "avm_output_addresses.output_id = all_outputs.id").
			LeftJoin("avm_transactions", "all_outputs.transaction_id = avm_transactions.id OR all_outputs.redeeming_transaction_id = avm_transactions.id").
			LeftJoin(dbr.I("avm_outputs").As("unspent_outputs"), "avm_output_addresses.output_id = unspent_outputs.id AND unspent_outputs.redeeming_transaction_id IS NULL").
			GroupBy("avm_output_addresses.address")).
			LoadOne(&count)
		if err != nil {
			return nil, err
		}
	}

	return &AddressList{ListMetadata{count}, addresses}, nil
}

func (r *DB) ListOutputs(params *ListOutputsParams) (*OutputList, error) {
	columns := []string{"avm_outputs.*"}

	if params.Query != "" {
		scoreColumn, err := r.newScoreColumnString("avm_outputs.id", params.Query)
		if err != nil {
			return nil, err
		}
		columns = append(columns, scoreColumn)
	}

	db := r.newSession("list_transaction_outputs")
	builder := params.Apply(db.
		Select(columns...).
		From("avm_outputs").
		LeftJoin("avm_transactions", "avm_transactions.id = avm_outputs.transaction_id").
		Where("avm_transactions.chain_id = ?", r.chainID.String()))

	if params.Query != "" {
		builder = builder.
			Where("INSTR(avm_outputs.id, ?) > 0", params.Query).
			OrderAsc("score")
	}

	outputs := []*Output{}
	_, err := builder.Load(&outputs)
	if err != nil {
		return nil, err
	}

	if len(outputs) < 1 {
		return &OutputList{Outputs: outputs}, nil
	}

	outputIDs := make([]stringID, len(outputs))
	outputMap := make(map[stringID]*Output, len(outputs))
	for i, output := range outputs {
		outputIDs[i] = output.ID
		outputMap[output.ID] = output
	}

	addresses := []*OutputAddress{}
	_, err = db.
		Select("*").
		From("avm_output_addresses").
		Where("output_id IN ?", outputIDs).
		Load(&addresses)
	if err != nil {
		return nil, err
	}

	for _, address := range addresses {
		output := outputMap[address.OutputID]
		if output == nil {
			continue
		}
		output.Addresses = append(output.Addresses, address.Address)
	}

	count := uint64(params.Offset) + uint64(len(outputs))
	if len(outputs) >= params.Limit {
		params.ListParams = ListParams{}
		err = params.Apply(db.
			Select("COUNT(avm_outputs.id)").
			From("avm_outputs").
			LeftJoin("avm_transactions", "avm_transactions.id = avm_outputs.transaction_id").
			Where("avm_transactions.chain_id = ?", r.chainID.String())).
			LoadOne(&count)
		if err != nil {
			return nil, err
		}
	}

	return &OutputList{ListMetadata{count}, outputs}, err
}

//
// Helpers
//

func (r *DB) getTransactionCountSince(db *dbr.Session, minutes uint64, assetID ids.ID) (count uint64, err error) {
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

func (r *DB) getFirstTransactionTime() (time.Time, error) {
	var ts int64
	err := r.newSession("get_first_transaction_time").
		Select("UNIX_TIMESTAMP(MIN(created_at))").
		From("avm_transactions").
		LoadOne(&ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0).UTC(), nil
}

func (r *DB) dressTransactions(db dbr.SessionRunner, txs []*Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	// Get the IDs returned so we can get Input/Output data
	txIDs := make([]stringID, len(txs))
	for i, tx := range txs {
		txIDs[i] = tx.ID
	}

	// Load each Transaction Output for the tx, both inputs and outputs
	outputs := []*Output{}
	_, err := db.
		Select("*").
		From("avm_outputs").
		Where("avm_outputs.transaction_id IN ? OR avm_outputs.redeeming_transaction_id IN ?", txIDs, txIDs).
		Load(&outputs)
	if err != nil {
		return err
	}

	// Load all Output addresses for this Transaction
	outputAddresses := make([]OutputAddress, 0, 2)
	_, err = db.
		Select(
			"avm_output_addresses.output_id AS output_id",
			"avm_output_addresses.address AS address",
			"addresses.public_key AS public_key",
			"avm_output_addresses.redeeming_signature AS signature",
		).
		From("avm_output_addresses").
		LeftJoin("avm_outputs", "avm_outputs.id = avm_output_addresses.output_id").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address").
		Where("avm_outputs.redeeming_transaction_id IN ? OR avm_outputs.transaction_id IN ?", txIDs, txIDs).
		Load(&outputAddresses)
	if err != nil {
		return err
	}

	// Create maps for all Transaction outputs and Input Transaction outputs
	outputMap := map[stringID]*Output{}

	inputsMap := map[stringID][]*Input{}
	inputTotalsMap := map[stringID]AssetTokenCounts{}

	outputsMap := map[stringID][]*Output{}
	outputTotalsMap := map[stringID]AssetTokenCounts{}

	for _, out := range outputs {
		outputMap[out.ID] = out

		if _, ok := inputsMap[out.RedeemingTransactionID]; !ok {
			inputsMap[out.RedeemingTransactionID] = []*Input{}
		}

		if _, ok := inputTotalsMap[out.RedeemingTransactionID]; !ok {
			inputTotalsMap[out.RedeemingTransactionID] = AssetTokenCounts{}
		}
		if _, ok := outputsMap[out.TransactionID]; !ok {
			outputsMap[out.TransactionID] = []*Output{}
		}

		if _, ok := outputTotalsMap[out.TransactionID]; !ok {
			outputTotalsMap[out.TransactionID] = AssetTokenCounts{}
		}

		inputsMap[out.RedeemingTransactionID] = append(inputsMap[out.RedeemingTransactionID], &Input{Output: out})
		inputTotalsMap[out.RedeemingTransactionID][out.AssetID] += out.Amount

		outputsMap[out.TransactionID] = append(outputsMap[out.TransactionID], out)
		outputTotalsMap[out.TransactionID][out.AssetID] += out.Amount
	}

	// Collect the addresses into a list on each Output
	var input *Input
	for _, outputAddress := range outputAddresses {
		output, ok := outputMap[outputAddress.OutputID]
		if !ok {
			continue
		}
		output.Addresses = append(output.Addresses, outputAddress.Address)

		// If this Address didn't sign any txs then we're done
		if len(outputAddress.Signature) == 0 {
			continue
		}

		inputs, ok := inputsMap[output.RedeemingTransactionID]
		if !ok {
			continue
		}

		// Get the Input and add the credentials for this Address
		for _, input = range inputs {
			if input.Output.ID.Equals(outputAddress.OutputID) {
				input.Creds = append(input.Creds, InputCredentials{
					Address:   outputAddress.Address,
					PublicKey: outputAddress.PublicKey,
					Signature: outputAddress.Signature,
				})
				break
			}
		}
	}

	// Add the data we've built up for each transaction
	for _, tx := range txs {
		tx.InputTotals = inputTotalsMap[tx.ID]
		tx.OutputTotals = outputTotalsMap[tx.ID]
		tx.Inputs = append(tx.Inputs, inputsMap[tx.ID]...)
		tx.Outputs = append(tx.Outputs, outputsMap[tx.ID]...)
	}

	return nil
}

func (r *DB) newScoreColumnString(column string, q string) (string, error) {
	return dbr.InterpolateForDialect("INSTR("+column+", ?) AS score", []interface{}{q}, r.db.Dialect)
}
