// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"

	"github.com/ava-labs/ortelius/services/models"
	"github.com/ava-labs/ortelius/services/params"
)

const (
	MaxAggregateIntervalCount = 20000
	MinSearchQueryLength      = 1
)

var (
	ErrAggregateIntervalCountTooLarge = errors.New("requesting too many intervals")
	ErrFailedToParseStringAsBigInt    = errors.New("failed to parse string to big.Int")
	ErrSearchQueryTooShort            = errors.New("search query too short")
)
var (
	outputSelectColumns = []string{
		"avm_outputs.id",
		"avm_outputs.transaction_id",
		"avm_outputs.output_index",
		"avm_outputs.asset_id",
		"avm_outputs.output_type",
		"avm_outputs.amount",
		"avm_outputs.locktime",
		"avm_outputs.threshold",
		"avm_outputs.created_at",
		"avm_outputs.redeeming_transaction_id",
	}

	outputSelectColumnsString = strings.Join(outputSelectColumns, ", ")

	outputSelectForDressTransactionsQuery = fmt.Sprintf(`
		SELECT %s FROM avm_outputs WHERE avm_outputs.transaction_id IN ?
		UNION
		SELECT %s FROM avm_outputs WHERE avm_outputs.redeeming_transaction_id IN ?`,
		outputSelectColumnsString, outputSelectColumnsString)
)

func (r *DB) Search(ctx context.Context, p *SearchParams) (*SearchResults, error) {
	if len(p.Query) < MinSearchQueryLength {
		return nil, ErrSearchQueryTooShort
	}

	// See if the query string is an id or shortID. If so we can search on them
	// directly. Otherwise we treat the query as a normal query-string.
	if shortID, err := addressFromString(p.Query); err == nil {
		return r.searchByShortID(ctx, shortID)
	}
	if id, err := ids.FromString(p.Query); err == nil {
		return r.searchByID(ctx, id)
	}

	// The query string was not an id/shortid so perform a regular search against
	// all models
	assets, err := r.ListAssets(ctx, &ListAssetsParams{ListParams: p.ListParams, Query: p.Query})
	if err != nil {
		return nil, err
	}
	if len(assets.Assets) >= p.Limit {
		return collateSearchResults(assets, nil, nil, nil)
	}

	transactions, err := r.ListTransactions(ctx, &ListTransactionsParams{ListParams: p.ListParams, Query: p.Query})
	if err != nil {
		return nil, err
	}
	if len(transactions.Transactions) >= p.Limit {
		return collateSearchResults(assets, nil, transactions, nil)
	}

	addresses, err := r.ListAddresses(ctx, &ListAddressesParams{ListParams: p.ListParams, Query: p.Query})
	if err != nil {
		return nil, err
	}
	if len(addresses.Addresses) >= p.Limit {
		return collateSearchResults(assets, addresses, transactions, nil)
	}

	return collateSearchResults(assets, addresses, transactions, nil)
}

func (r *DB) Aggregate(ctx context.Context, params *AggregateParams) (*AggregatesHistogram, error) {
	// Validate params and set defaults if necessary
	if params.StartTime.IsZero() {
		var err error
		params.StartTime, err = r.getFirstTransactionTime(ctx)
		if err != nil {
			return nil, err
		}
	}
	if params.EndTime.IsZero() {
		params.EndTime = time.Now().UTC()
	}

	// Ensure the interval count requested isn't too large
	intervalSeconds := int64(params.IntervalSize.Seconds())
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

	// Build the query and load the base data
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

	if params.AssetID != nil {
		builder.Where("avm_outputs.asset_id = ?", params.AssetID.String())
	}

	if requestedIntervalCount > 0 {
		builder.
			GroupBy("idx").
			OrderAsc("idx").
			Limit(uint64(requestedIntervalCount))
	}

	intervals := []Aggregates{}
	_, err := builder.LoadContext(ctx, &intervals)
	if err != nil {
		return nil, err
	}

	// If no intervals were requested then the total aggregate is equal to the
	// first (and only) interval, and we're done
	if requestedIntervalCount == 0 {
		// This check should never fail if the SQL query is correct, but added for
		// robustness to prevent panics if the invariant does not hold.
		if len(intervals) > 0 {
			intervals[0].StartTime = params.StartTime
			intervals[0].EndTime = params.EndTime
			return &AggregatesHistogram{Aggregates: intervals[0]}, nil
		}
		return &AggregatesHistogram{}, nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &AggregatesHistogram{IntervalSize: params.IntervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		// An interval's start Time is its index Time the interval size, plus the
		// starting Time. The end Time is (interval size - 1) seconds after the
		// start Time.
		startTS = params.StartTime.Unix() + (int64(intervalIdx) * intervalSeconds)
		return time.Unix(startTS, 0).UTC(),
			time.Unix(startTS+intervalSeconds-1, 0).UTC()
	}

	padTo := func(slice []Aggregates, to int) []Aggregates {
		for i := len(slice); i < to; i = len(slice) {
			slice = append(slice, Aggregates{Idx: i})
			slice[i].StartTime, slice[i].EndTime = timesForInterval(i)
		}
		return slice
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.Aggregates = Aggregates{StartTime: params.StartTime, EndTime: params.EndTime}
	var (
		bigIntFromStringOK bool
		totalVolume        = big.NewInt(0)
		intervalVolume     = big.NewInt(0)
	)

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]Aggregates, 0, requestedIntervalCount)
	for _, interval := range intervals {
		// Pad up to this interval's position
		aggs.Intervals = padTo(aggs.Intervals, interval.Idx)

		// Format this interval
		interval.StartTime, interval.EndTime = timesForInterval(interval.Idx)

		// Parse volume into a big.Int
		_, bigIntFromStringOK = intervalVolume.SetString(string(interval.TransactionVolume), 10)
		if !bigIntFromStringOK {
			return nil, ErrFailedToParseStringAsBigInt
		}

		// Add to the overall aggregates counts
		totalVolume.Add(totalVolume, intervalVolume)
		aggs.Aggregates.TransactionCount += interval.TransactionCount
		aggs.Aggregates.OutputCount += interval.OutputCount
		aggs.Aggregates.AddressCount += interval.AddressCount
		aggs.Aggregates.AssetCount += interval.AssetCount

		// Add to the list of intervals
		aggs.Intervals = append(aggs.Intervals, interval)
	}
	// Add total aggregated token amounts
	aggs.Aggregates.TransactionVolume = TokenAmount(totalVolume.String())

	// Add any missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	return aggs, nil
}

func (r *DB) ListTransactions(ctx context.Context, p *ListTransactionsParams) (*TransactionList, error) {
	db := r.newSession("get_transactions")

	txs := []*Transaction{}
	builder := p.Apply(db.
		Select("avm_transactions.id", "avm_transactions.chain_id", "avm_transactions.type", "avm_transactions.created_at").
		From("avm_transactions").
		Where("avm_transactions.chain_id = ?", r.chainID))

	var applySort func(sort TransactionSort)
	applySort = func(sort TransactionSort) {
		if p.Query != "" {
			return
		}
		switch sort {
		case TransactionSortTimestampAsc:
			builder.OrderAsc("avm_transactions.created_at")
		case TransactionSortTimestampDesc:
			builder.OrderDesc("avm_transactions.created_at")
		default:
			applySort(TransactionSortDefault)
		}
	}
	applySort(p.Sort)

	if _, err := builder.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}

	count := uint64(p.Offset) + uint64(len(txs))
	if len(txs) >= p.Limit {
		p.ListParams = params.ListParams{}
		err := p.Apply(db.
			Select("COUNT(avm_transactions.id)").
			From("avm_transactions")).
			LoadOneContext(ctx, &count)
		if err != nil {
			return nil, err
		}
	}

	// Add all the addition information we might want
	if err := r.dressTransactions(ctx, db, txs); err != nil {
		return nil, err
	}

	return &TransactionList{ListMetadata{count}, txs}, nil
}

func (r *DB) ListAssets(ctx context.Context, p *ListAssetsParams) (*AssetList, error) {
	db := r.newSession("list_assets")

	assets := []*Asset{}
	_, err := p.Apply(db.
		Select("id", "chain_id", "name", "symbol", "alias", "denomination", "current_supply", "created_at").
		From("avm_assets").
		Where("chain_id = ?", r.chainID)).
		LoadContext(ctx, &assets)
	if err != nil {
		return nil, err
	}

	count := uint64(p.Offset) + uint64(len(assets))
	if len(assets) >= p.Limit {
		p.ListParams = params.ListParams{}
		err := p.Apply(db.
			Select("COUNT(avm_assets.id)").
			From("avm_assets").
			Where("chain_id = ?", r.chainID)).
			LoadOneContext(ctx, &count)
		if err != nil {
			return nil, err
		}
	}

	return &AssetList{ListMetadata{count}, assets}, nil
}

func (r *DB) ListAddresses(ctx context.Context, p *ListAddressesParams) (*AddressList, error) {
	db := r.newSession("list_addresses")

	addresses := []*Address{}
	_, err := p.Apply(db.
		Select("avm_output_addresses.address", "addresses.public_key").
		From("addresses").
		LeftJoin("avm_output_addresses", "addresses.address = avm_output_addresses.address")).
		LoadContext(ctx, &addresses)
	if err != nil {
		return nil, err
	}

	count := uint64(p.Offset) + uint64(len(addresses))
	if len(addresses) >= p.Limit {
		p.ListParams = params.ListParams{}
		err = p.Apply(db.
			Select("COUNT(addresses.address)").
			From("addresses")).
			LoadOneContext(ctx, &count)
		if err != nil {
			return nil, err
		}
	}

	// Add all the addition information we might want
	if err = r.dressAddresses(ctx, db, addresses); err != nil {
		return nil, err
	}

	return &AddressList{ListMetadata{count}, addresses}, nil
}

func (r *DB) ListOutputs(ctx context.Context, p *ListOutputsParams) (*OutputList, error) {
	db := r.newSession("list_transaction_outputs")

	outputs := []*Output{}
	_, err := p.Apply(db.
		Select(outputSelectColumns...).
		From("avm_outputs")).LoadContext(ctx, &outputs)
	if err != nil {
		return nil, err
	}

	if len(outputs) < 1 {
		return &OutputList{Outputs: outputs}, nil
	}

	outputIDs := make([]models.StringID, len(outputs))
	outputMap := make(map[models.StringID]*Output, len(outputs))
	for i, output := range outputs {
		outputIDs[i] = output.ID
		outputMap[output.ID] = output
	}

	addresses := []*OutputAddress{}
	_, err = db.
		Select(
			"avm_output_addresses.output_id",
			"avm_output_addresses.address",
			"avm_output_addresses.redeeming_signature AS signature",
			"avm_output_addresses.created_at",
		).
		From("avm_output_addresses").
		Where("avm_output_addresses.output_id IN ?", outputIDs).
		LoadContext(ctx, &addresses)
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

	count := uint64(p.Offset) + uint64(len(outputs))
	if len(outputs) >= p.Limit {
		p.ListParams = params.ListParams{}
		err = p.Apply(db.
			Select("COUNT(avm_outputs.id)").
			From("avm_outputs")).
			LoadOneContext(ctx, &count)
		if err != nil {
			return nil, err
		}
	}

	return &OutputList{ListMetadata{count}, outputs}, err
}

//
// Helpers
//

func (r *DB) getTransactionCountSince(ctx context.Context, db *dbr.Session, minutes uint64, assetID ids.ID) (count uint64, err error) {
	builder := db.
		Select("COUNT(DISTINCT(avm_transactions.id))").
		From("avm_transactions")

	if minutes > 0 {
		builder = builder.Where("created_at >= DATE_SUB(NOW(), INTERVAL ? MINUTE)", minutes)
	}

	if !assetID.Equals(ids.Empty) {
		builder = builder.
			LeftJoin("avm_outputs", "avm_outputs.transaction_id = avm_transactions.id").
			Where("avm_outputs.asset_id = ?", assetID.String())
	}

	err = builder.LoadOneContext(ctx, &count)
	return count, err
}

func (r *DB) getFirstTransactionTime(ctx context.Context) (time.Time, error) {
	var ts int64
	err := r.newSession("get_first_transaction_time").
		Select("COALESCE(UNIX_TIMESTAMP(MIN(created_at)), 0)").
		From("avm_transactions").
		LoadOneContext(ctx, &ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0).UTC(), nil
}

func (r *DB) dressTransactions(ctx context.Context, db dbr.SessionRunner, txs []*Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	// Get the IDs returned so we can get Input/Output data
	txIDs := make([]models.StringID, len(txs))
	for i, tx := range txs {
		txIDs[i] = tx.ID
	}

	// Load each Transaction Output for the tx, both inputs and outputs
	outputs := []*Output{}
	_, err := db.SelectBySql(outputSelectForDressTransactionsQuery, txIDs, txIDs).
		LoadContext(ctx, &outputs)
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
		Where("avm_outputs.transaction_id IN ? OR avm_outputs.redeeming_transaction_id IN ?", txIDs, txIDs).
		LoadContext(ctx, &outputAddresses)
	if err != nil {
		return err
	}

	// Create maps for all Transaction outputs and Input Transaction outputs
	outputMap := map[models.StringID]*Output{}

	inputsMap := map[models.StringID][]*Input{}
	inputTotalsMap := map[models.StringID]map[models.StringID]*big.Int{}

	outputsMap := map[models.StringID][]*Output{}
	outputTotalsMap := map[models.StringID]map[models.StringID]*big.Int{}

	addToBigIntMap := func(m map[models.StringID]*big.Int, assetID models.StringID, amt *big.Int) {
		prevAmt := m[assetID]
		if prevAmt == nil {
			prevAmt = big.NewInt(0)
		}
		m[assetID] = prevAmt.Add(amt, prevAmt)
	}

	for _, out := range outputs {
		outputMap[out.ID] = out

		out.Addresses = []models.StringShortID{}

		if _, ok := inputsMap[out.RedeemingTransactionID]; !ok {
			inputsMap[out.RedeemingTransactionID] = []*Input{}
		}

		if _, ok := inputTotalsMap[out.RedeemingTransactionID]; !ok {
			inputTotalsMap[out.RedeemingTransactionID] = map[models.StringID]*big.Int{}
		}
		if _, ok := outputsMap[out.TransactionID]; !ok {
			outputsMap[out.TransactionID] = []*Output{}
		}

		if _, ok := outputTotalsMap[out.TransactionID]; !ok {
			outputTotalsMap[out.TransactionID] = map[models.StringID]*big.Int{}
		}

		bigAmt := new(big.Int)
		if _, ok := bigAmt.SetString(string(out.Amount), 10); !ok {
			return errors.New("invalid amount")
		}

		inputsMap[out.RedeemingTransactionID] = append(inputsMap[out.RedeemingTransactionID], &Input{Output: out})
		addToBigIntMap(inputTotalsMap[out.RedeemingTransactionID], out.AssetID, bigAmt)

		outputsMap[out.TransactionID] = append(outputsMap[out.TransactionID], out)
		addToBigIntMap(outputTotalsMap[out.TransactionID], out.AssetID, bigAmt)
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
		tx.Inputs = append(tx.Inputs, inputsMap[tx.ID]...)
		tx.Outputs = append(tx.Outputs, outputsMap[tx.ID]...)

		tx.InputTotals = make(AssetTokenCounts, len(inputTotalsMap[tx.ID]))
		for k, v := range inputTotalsMap[tx.ID] {
			tx.InputTotals[k] = TokenAmount(v.String())
		}

		tx.OutputTotals = make(AssetTokenCounts, len(outputTotalsMap[tx.ID]))
		for k, v := range outputTotalsMap[tx.ID] {
			tx.OutputTotals[k] = TokenAmount(v.String())
		}
	}

	return nil
}

func (r *DB) dressAddresses(ctx context.Context, db dbr.SessionRunner, addrs []*Address) error {
	if len(addrs) == 0 {
		return nil
	}

	// Create a list of ids for querying, and a map for accumulating results later
	addrIDs := make([]models.StringShortID, len(addrs))
	addrsByID := make(map[models.StringShortID]*Address, len(addrs))
	for i, addr := range addrs {
		addrIDs[i] = addr.Address
		addrsByID[addr.Address] = addr

		addr.Assets = make(map[models.StringID]AssetInfo, 1)
	}

	// Load each Transaction Output for the tx, both inputs and outputs
	rows := []*struct {
		Address models.StringShortID `json:"address"`
		AssetInfo
	}{}

	_, err := db.
		Select(
			"avm_output_addresses.address",
			"avm_outputs.asset_id",
			"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
			"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
			"COALESCE(SUM(CASE WHEN avm_outputs.redeeming_transaction_id != '' THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
			"COALESCE(SUM(CASE WHEN avm_outputs.redeeming_transaction_id = '' THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
			"COALESCE(SUM(CASE WHEN avm_outputs.redeeming_transaction_id = '' THEN 1 ELSE 0 END), 0) AS utxo_count",
		).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		Where("avm_output_addresses.address IN ?", addrIDs).
		GroupBy("avm_output_addresses.address", "avm_outputs.asset_id").
		LoadContext(ctx, &rows)
	if err != nil {
		return err
	}

	// Accumulate rows into addresses
	for _, row := range rows {
		addr, ok := addrsByID[row.Address]
		if !ok {
			continue
		}
		addr.Assets[row.AssetID] = row.AssetInfo
	}

	return nil
}

func (r *DB) searchByID(ctx context.Context, id ids.ID) (*SearchResults, error) {
	if assets, err := r.ListAssets(ctx, &ListAssetsParams{ID: &id}); err != nil {
		return nil, err
	} else if len(assets.Assets) > 0 {
		return collateSearchResults(assets, nil, nil, nil)
	}

	if txs, err := r.ListTransactions(ctx, &ListTransactionsParams{ID: &id}); err != nil {
		return nil, err
	} else if len(txs.Transactions) > 0 {
		return collateSearchResults(nil, nil, txs, nil)
	}

	return &SearchResults{}, nil
}

func (r *DB) searchByShortID(ctx context.Context, id ids.ShortID) (*SearchResults, error) {
	if addrs, err := r.ListAddresses(ctx, &ListAddressesParams{Address: &id}); err != nil {
		return nil, err
	} else if len(addrs.Addresses) > 0 {
		return collateSearchResults(nil, addrs, nil, nil)
	}

	return &SearchResults{}, nil
}

func collateSearchResults(assetResults *AssetList, addressResults *AddressList, transactionResults *TransactionList, outputResults *OutputList) (*SearchResults, error) {
	var (
		assets       []*Asset
		addresses    []*Address
		transactions []*Transaction
		outputs      []*Output
	)

	if assetResults != nil {
		assets = assetResults.Assets
	}

	if addressResults != nil {
		addresses = addressResults.Addresses
	}

	if transactionResults != nil {
		transactions = transactionResults.Transactions
	}

	if outputResults != nil {
		outputs = outputResults.Outputs
	}

	// Build overall SearchResults object from our pieces
	returnedResultCount := len(assets) + len(addresses) + len(transactions) + len(outputs)
	if returnedResultCount > params.PaginationMaxLimit {
		returnedResultCount = params.PaginationMaxLimit
	}

	collatedResults := &SearchResults{
		Count: uint64(returnedResultCount),

		// Create a container for our combined results
		Results: make([]SearchResult, 0, returnedResultCount),
	}

	// Add each result to the list
	for _, result := range assets {
		collatedResults.Results = append(collatedResults.Results, SearchResult{
			SearchResultType: ResultTypeAsset,
			Data:             result,
		})
	}
	for _, result := range addresses {
		collatedResults.Results = append(collatedResults.Results, SearchResult{
			SearchResultType: ResultTypeAddress,
			Data:             result,
		})
	}
	for _, result := range transactions {
		collatedResults.Results = append(collatedResults.Results, SearchResult{
			SearchResultType: ResultTypeTransaction,
			Data:             result,
		})
	}

	return collatedResults, nil
}
