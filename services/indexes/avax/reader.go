// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"
)

const (
	MaxAggregateIntervalCount = 20000

	MinSearchQueryLength = 1

	RequestTimeout = 30 * time.Second
)

var (
	ErrAggregateIntervalCountTooLarge = errors.New("requesting too many intervals")
	ErrFailedToParseStringAsBigInt    = errors.New("failed to parse string to big.Int")
	ErrSearchQueryTooShort            = errors.New("search query too short")

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
		"case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id",
		"avm_outputs.group_id",
		"avm_outputs.payload",
	}
)

type Reader struct {
	conns *services.Connections
}

func NewReader(conns *services.Connections) *Reader {
	return &Reader{
		conns: conns,
		//chainIDs: chainIDs,
	}
}

func (r *Reader) Search(ctx context.Context, p *params.SearchParams, avaxAssetID ids.ID) (*models.SearchResults, error) {
	p.ListParams.DisableCounting = true

	if len(p.ListParams.Query) < MinSearchQueryLength {
		return nil, ErrSearchQueryTooShort
	}

	// See if the query string is an id or shortID. If so we can search on them
	// directly. Otherwise we treat the query as a normal query-string.
	if shortID, err := params.AddressFromString(p.ListParams.Query); err == nil {
		return r.searchByShortID(ctx, shortID)
	}
	if id, err := ids.FromString(p.ListParams.Query); err == nil {
		return r.searchByID(ctx, id, avaxAssetID)
	}

	// The query string was not an id/shortid so perform an ID prefix search
	// against transactions and addresses.
	transactions, err := r.ListTransactions(ctx, &params.ListTransactionsParams{ListParams: p.ListParams}, avaxAssetID)
	if err != nil {
		return nil, err
	}
	if len(transactions.Transactions) >= p.ListParams.Limit {
		return collateSearchResults(nil, transactions)
	}

	addresses, err := r.ListAddresses(ctx, &params.ListAddressesParams{ListParams: p.ListParams})
	if err != nil {
		return nil, err
	}
	if len(addresses.Addresses) >= p.ListParams.Limit {
		return collateSearchResults(addresses, transactions)
	}

	return collateSearchResults(addresses, transactions)
}

func (r *Reader) Aggregate(ctx context.Context, params *params.AggregateParams) (*models.AggregatesHistogram, error) {
	// Validate params and set defaults if necessary
	if params.ListParams.StartTime.IsZero() {
		var err error
		params.ListParams.StartTime, err = r.getFirstTransactionTime(ctx, params.ChainIDs)
		if err != nil {
			return nil, err
		}
	}

	var intervals []models.Aggregates

	// Ensure the interval count requested isn't too large
	intervalSeconds := int64(params.IntervalSize.Seconds())
	requestedIntervalCount := 0
	if intervalSeconds != 0 {
		requestedIntervalCount = int(math.Ceil(params.ListParams.EndTime.Sub(params.ListParams.StartTime).Seconds() / params.IntervalSize.Seconds()))
		if requestedIntervalCount > MaxAggregateIntervalCount {
			return nil, ErrAggregateIntervalCountTooLarge
		}
		if requestedIntervalCount < 1 {
			requestedIntervalCount = 1
		}
	}

	// Build the query and load the base data
	dbRunner, err := r.conns.DB().NewSession("get_transaction_aggregates_histogram", RequestTimeout)
	if err != nil {
		return nil, err
	}

	var builder *dbr.SelectStmt

	switch params.Version {
	// new requests v=1 use the avm_asset_aggregation tables
	case 2:
		fallthrough
	case 1:
		columns := []string{
			"CAST(COALESCE(SUM(avm_asset_aggregation.transaction_volume),0) AS CHAR) as transaction_volume",
			"SUM(avm_asset_aggregation.transaction_count) AS transaction_count",
			"SUM(avm_asset_aggregation.address_count) AS address_count",
			"SUM(avm_asset_aggregation.asset_count) AS asset_count",
			"SUM(avm_asset_aggregation.output_count) AS output_count",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(avm_asset_aggregation.aggregate_ts)-%d) / %d) AS idx",
				params.ListParams.StartTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("avm_asset_aggregation").
			Where("avm_asset_aggregation.aggregate_ts >= ?", params.ListParams.StartTime).
			Where("avm_asset_aggregation.aggregate_ts < ?", params.ListParams.EndTime)

		if params.AssetID != nil {
			builder.Where("avm_asset_aggregation.asset_id = ?", params.AssetID.String())
		}
	default:
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
				params.ListParams.StartTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
			Where("avm_outputs.created_at >= ?", params.ListParams.StartTime).
			Where("avm_outputs.created_at < ?", params.ListParams.EndTime)

		if params.AssetID != nil {
			builder.Where("avm_outputs.asset_id = ?", params.AssetID.String())
		}
	}

	if requestedIntervalCount > 0 {
		builder.
			GroupBy("idx").
			OrderAsc("idx").
			Limit(uint64(requestedIntervalCount))
	}

	_, err = builder.LoadContext(ctx, &intervals)
	if err != nil {
		return nil, err
	}

	// If no intervals were requested then the total aggregate is equal to the
	// first (and only) interval, and we're done
	if requestedIntervalCount == 0 {
		// This check should never fail if the SQL query is correct, but added for
		// robustness to prevent panics if the invariant does not hold.
		if len(intervals) > 0 {
			intervals[0].StartTime = params.ListParams.StartTime
			intervals[0].EndTime = params.ListParams.EndTime
			return &models.AggregatesHistogram{
				Aggregates: intervals[0],
				StartTime:  params.ListParams.StartTime,
				EndTime:    params.ListParams.EndTime,
			}, nil
		}
		return &models.AggregatesHistogram{
			StartTime: params.ListParams.StartTime,
			EndTime:   params.ListParams.EndTime,
		}, nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &models.AggregatesHistogram{IntervalSize: params.IntervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		startTS = params.ListParams.StartTime.Unix() + (int64(intervalIdx) * intervalSeconds)
		return time.Unix(startTS, 0).UTC(),
			time.Unix(startTS+intervalSeconds-1, 0).UTC()
	}

	padTo := func(slice []models.Aggregates, to int) []models.Aggregates {
		for i := len(slice); i < to; i = len(slice) {
			slice = append(slice, models.Aggregates{Idx: i})
			slice[i].StartTime, slice[i].EndTime = timesForInterval(i)
		}
		return slice
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.Aggregates = models.Aggregates{StartTime: params.ListParams.StartTime, EndTime: params.ListParams.EndTime}
	var (
		bigIntFromStringOK bool
		totalVolume        = big.NewInt(0)
		intervalVolume     = big.NewInt(0)
	)

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]models.Aggregates, 0, requestedIntervalCount)
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
	aggs.Aggregates.TransactionVolume = models.TokenAmount(totalVolume.String())

	// Add any missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	aggs.StartTime = params.ListParams.StartTime
	aggs.EndTime = params.ListParams.EndTime

	return aggs, nil
}

func (r *Reader) ListTransactions(ctx context.Context, p *params.ListTransactionsParams, avaxAssetID ids.ID) (*models.TransactionList, error) {
	dbRunner, err := r.conns.DB().NewSession("get_transactions", RequestTimeout)
	if err != nil {
		return nil, err
	}

	var txs []*models.Transaction
	builder := p.Apply(dbRunner.
		Select("avm_transactions.id", "avm_transactions.chain_id", "avm_transactions.type", "avm_transactions.memo", "avm_transactions.created_at", "avm_transactions.txfee", "avm_transactions.genesis").
		From("avm_transactions"))

	var applySort func(sort params.TransactionSort)
	applySort = func(sort params.TransactionSort) {
		if p.ListParams.Query != "" {
			return
		}
		switch sort {
		case params.TransactionSortTimestampAsc:
			builder.OrderAsc("avm_transactions.chain_id")
			builder.OrderAsc("avm_transactions.created_at")
		case params.TransactionSortTimestampDesc:
			builder.OrderAsc("avm_transactions.chain_id")
			builder.OrderDesc("avm_transactions.created_at")
		default:
			applySort(params.TransactionSortDefault)
		}
	}
	applySort(p.Sort)

	if _, err := builder.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(txs)))
		if len(txs) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			selector := p.Apply(dbRunner.
				Select("COUNT(avm_transactions.id)").
				From("avm_transactions"))

			if err := selector.LoadOneContext(ctx, &count); err != nil {
				return nil, err
			}
		}
	}

	// Add all the addition information we might want
	if err := r.dressTransactions(ctx, dbRunner, txs, avaxAssetID, p.ListParams.ID, p.DisableGenesis); err != nil {
		return nil, err
	}

	return &models.TransactionList{ListMetadata: models.ListMetadata{
		Count: count,
	},
		Transactions: txs,
		StartTime:    p.ListParams.StartTime,
		EndTime:      p.ListParams.EndTime,
	}, nil
}

func (r *Reader) ListAddresses(ctx context.Context, p *params.ListAddressesParams) (*models.AddressList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_addresses", RequestTimeout)
	if err != nil {
		return nil, err
	}

	var addresses []*models.AddressInfo
	_, err = p.Apply(dbRunner.
		Select("DISTINCT(avm_output_addresses.address)", "addresses.public_key").
		From("avm_output_addresses").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address")).
		LoadContext(ctx, &addresses)
	if err != nil {
		return nil, err
	}

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(addresses)))
		if len(addresses) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			err = p.Apply(dbRunner.
				Select("COUNT(DISTINCT(avm_output_addresses.address))").
				From("avm_output_addresses")).
				LoadOneContext(ctx, &count)
			if err != nil {
				return nil, err
			}
		}
	}

	// Add all the addition information we might want
	if err = r.dressAddresses(ctx, dbRunner, addresses, p.Version, p.ChainIDs); err != nil {
		return nil, err
	}

	return &models.AddressList{ListMetadata: models.ListMetadata{Count: count}, Addresses: addresses}, nil
}

func (r *Reader) ListOutputs(ctx context.Context, p *params.ListOutputsParams) (*models.OutputList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_transaction_outputs", RequestTimeout)
	if err != nil {
		return nil, err
	}

	var outputs []*models.Output
	_, err = p.Apply(dbRunner.
		Select(outputSelectColumns...).
		From("avm_outputs").
		LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id")).
		LoadContext(ctx, &outputs)
	if err != nil {
		return nil, err
	}

	if len(outputs) < 1 {
		return &models.OutputList{Outputs: outputs}, nil
	}

	outputIDs := make([]models.StringID, len(outputs))
	outputMap := make(map[models.StringID]*models.Output, len(outputs))
	for i, output := range outputs {
		outputIDs[i] = output.ID
		outputMap[output.ID] = output
	}

	var addresses []*models.OutputAddress
	_, err = dbRunner.
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

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(outputs)))
		if len(outputs) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			err = p.Apply(dbRunner.
				Select("COUNT(avm_outputs.id)").
				From("avm_outputs").
				LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id")).
				LoadOneContext(ctx, &count)
			if err != nil {
				return nil, err
			}
		}
	}

	return &models.OutputList{ListMetadata: models.ListMetadata{Count: count}, Outputs: outputs}, err
}

func (r *Reader) GetTransaction(ctx context.Context, id ids.ID, avaxAssetID ids.ID) (*models.Transaction, error) {
	txList, err := r.ListTransactions(ctx, &params.ListTransactionsParams{
		ListParams: params.ListParams{ID: &id},
	}, avaxAssetID)
	if err != nil {
		return nil, err
	}
	if len(txList.Transactions) > 0 {
		return txList.Transactions[0], nil
	}
	return nil, nil
}

func (r *Reader) GetAddress(ctx context.Context, id ids.ShortID) (*models.AddressInfo, error) {
	addressList, err := r.ListAddresses(ctx, &params.ListAddressesParams{Address: &id})
	if err != nil {
		return nil, err
	}
	if len(addressList.Addresses) > 0 {
		return addressList.Addresses[0], nil
	}
	return nil, err
}

func (r *Reader) GetOutput(ctx context.Context, id ids.ID) (*models.Output, error) {
	outputList, err := r.ListOutputs(ctx, &params.ListOutputsParams{ListParams: params.ListParams{ID: &id}})
	if err != nil {
		return nil, err
	}
	if len(outputList.Outputs) > 0 {
		return outputList.Outputs[0], nil
	}
	return nil, err
}

func (r *Reader) AddressChains(ctx context.Context, p *params.AddressChainsParams) (*models.AddressChains, error) {
	dbRunner, err := r.conns.DB().NewSession("addressChains", RequestTimeout)
	if err != nil {
		return nil, err
	}

	var addressChains []*models.AddressChainInfo

	// if there are no addresses specified don't query.
	if len(p.Addresses) == 0 {
		return &models.AddressChains{}, nil
	}

	_, err = p.Apply(dbRunner.
		Select("address", "chain_id", "created_at").
		From("address_chain")).
		LoadContext(ctx, &addressChains)
	if err != nil {
		return nil, err
	}

	resp := models.AddressChains{}
	resp.AddressChains = make(map[string][]models.StringID)
	for _, addressChain := range addressChains {
		addr, err := addressChain.Address.MarshalString()
		if err != nil {
			return nil, err
		}
		addrAsStr := string(addr)
		if _, ok := resp.AddressChains[addrAsStr]; !ok {
			resp.AddressChains[addrAsStr] = make([]models.StringID, 0, 2)
		}
		resp.AddressChains[addrAsStr] = append(resp.AddressChains[addrAsStr], addressChain.ChainID)
	}

	return &resp, nil
}

func (r *Reader) getFirstTransactionTime(ctx context.Context, chainIDs []string) (time.Time, error) {
	dbRunner, err := r.conns.DB().NewSession("get_first_transaction_time", RequestTimeout)
	if err != nil {
		return time.Time{}, err
	}

	var ts int64
	builder := dbRunner.
		Select("COALESCE(UNIX_TIMESTAMP(MIN(created_at)), 0)").
		From("avm_transactions")

	if len(chainIDs) > 0 {
		builder.Where("avm_transactions.chain_id = ?", chainIDs)
	}

	err = builder.LoadOneContext(ctx, &ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0).UTC(), nil
}

// Load output data for all inputs and outputs into a single list
// We can't treat them separately because some my be both inputs and outputs
// for different transactions
type compositeRecord struct {
	models.Output
	models.OutputAddress
}

func (r *Reader) dressTransactions(ctx context.Context, dbRunner dbr.SessionRunner, txs []*models.Transaction, avaxAssetID ids.ID, txID *ids.ID, disableGenesis bool) error {
	if len(txs) == 0 {
		return nil
	}

	// Get the IDs returned so we can get Input/Output data
	txIDs := make([]models.StringID, len(txs))
	for i, tx := range txs {
		if txs[i].Memo == nil {
			txs[i].Memo = []byte("")
		}
		txIDs[i] = tx.ID
	}

	outputs, err := r.collectInsAndOuts(ctx, dbRunner, txIDs)
	if err != nil {
		return err
	}

	// Create a map of addresses for each output and maps of transaction ids to
	// inputs, outputs, and the total amounts of the inputs and outputs
	var (
		outputAddrs     = make(map[models.StringID]map[models.Address]struct{}, len(txs)*2)
		inputsMap       = make(map[models.StringID]map[models.StringID]*models.Input, len(txs))
		outputsMap      = make(map[models.StringID]map[models.StringID]*models.Output, len(txs))
		inputTotalsMap  = make(map[models.StringID]map[models.StringID]*big.Int, len(txs))
		outputTotalsMap = make(map[models.StringID]map[models.StringID]*big.Int, len(txs))
	)

	// Create a helper to safely add big integers
	addToBigIntMap := func(m map[models.StringID]*big.Int, assetID models.StringID, amt *big.Int) {
		prevAmt := m[assetID]
		if prevAmt == nil {
			prevAmt = big.NewInt(0)
		}
		m[assetID] = prevAmt.Add(amt, prevAmt)
	}

	// Collect outpoints into the maps
	for _, output := range outputs {
		out := &output.Output

		bigAmt := new(big.Int)
		if _, ok := bigAmt.SetString(string(out.Amount), 10); !ok {
			return errors.New("invalid amount")
		}

		if _, ok := inputsMap[out.RedeemingTransactionID]; !ok {
			inputsMap[out.RedeemingTransactionID] = map[models.StringID]*models.Input{}
		}
		if _, ok := inputTotalsMap[out.RedeemingTransactionID]; !ok {
			inputTotalsMap[out.RedeemingTransactionID] = map[models.StringID]*big.Int{}
		}
		if _, ok := outputsMap[out.TransactionID]; !ok {
			outputsMap[out.TransactionID] = map[models.StringID]*models.Output{}
		}
		if _, ok := outputTotalsMap[out.TransactionID]; !ok {
			outputTotalsMap[out.TransactionID] = map[models.StringID]*big.Int{}
		}
		if _, ok := outputAddrs[out.ID]; !ok {
			outputAddrs[out.ID] = map[models.Address]struct{}{}
		}

		outputAddrs[out.ID][output.OutputAddress.Address] = struct{}{}
		outputsMap[out.TransactionID][out.ID] = out
		inputsMap[out.RedeemingTransactionID][out.ID] = &models.Input{Output: out}
		addToBigIntMap(outputTotalsMap[out.TransactionID], out.AssetID, bigAmt)
		addToBigIntMap(inputTotalsMap[out.RedeemingTransactionID], out.AssetID, bigAmt)
	}

	// Collect the addresses into a list on each outpoint
	var input *models.Input
	for _, out := range outputs {
		out.Addresses = make([]models.Address, 0, len(outputAddrs[out.ID]))
		for addr := range outputAddrs[out.ID] {
			// mock in records have a blank address.  Drop them.
			if len(addr) == 0 {
				continue
			}
			out.Addresses = append(out.Addresses, addr)
		}

		// If this Address didn't sign any txs then we're done
		if len(out.Signature) == 0 {
			continue
		}

		// Get the Input and add the credentials for this Address
		for _, input = range inputsMap[out.RedeemingTransactionID] {
			if input.Output.ID.Equals(out.OutputID) {
				input.Creds = append(input.Creds, models.InputCredentials{
					Address:   out.Address,
					PublicKey: out.PublicKey,
					Signature: out.Signature,
				})
				break
			}
		}
	}

	// Add the data we've built up for each transaction
	for _, tx := range txs {
		if disableGenesis && (txID == nil && string(tx.ID) == avaxAssetID.String()) {
			continue
		}
		if inputs, ok := inputsMap[tx.ID]; ok {
			for _, input := range inputs {
				tx.Inputs = append(tx.Inputs, input)
			}
		}

		if outputs, ok := outputsMap[tx.ID]; ok {
			for _, output := range outputs {
				tx.Outputs = append(tx.Outputs, output)
			}
		}

		tx.InputTotals = make(models.AssetTokenCounts, len(inputTotalsMap[tx.ID]))
		for k, v := range inputTotalsMap[tx.ID] {
			tx.InputTotals[k] = models.TokenAmount(v.String())
		}

		tx.OutputTotals = make(models.AssetTokenCounts, len(outputTotalsMap[tx.ID]))
		for k, v := range outputTotalsMap[tx.ID] {
			tx.OutputTotals[k] = models.TokenAmount(v.String())
		}
	}
	return nil
}

func (r *Reader) collectInsAndOuts(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) ([]*compositeRecord, error) {
	var outputs []*compositeRecord
	_, err := selectOutputs(dbRunner).
		Where("avm_outputs.transaction_id IN ?", txIDs).
		LoadContext(ctx, &outputs)
	if err != nil {
		return nil, err
	}

	var inputs []*compositeRecord
	_, err = selectOutputs(dbRunner).
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ?", txIDs).
		LoadContext(ctx, &inputs)
	if err != nil {
		return nil, err
	}

	inputIDs := make([]models.StringID, 0, len(inputs))
	for _, input := range inputs {
		inputIDs = append(inputIDs, input.ID)
	}

	// find any Ins without a matching Out and make mock out records for display..
	// when the avm_outputs.id is null we don't have a matching out. b/c of avm_outputs_redeeming left join avm_outputs in selectOutputsRedeeming
	// we're looking for any with my redeeming_transaction_id
	// and the avm_outputs.id is null meaning no matching out (because of the left join in selectOutputsRedeeming)
	// and not in the known inputIDs list.
	var inputsRedeeming []*compositeRecord
	q := selectOutputsRedeeming(dbRunner)
	if len(inputIDs) != 0 {
		q = q.Where("avm_outputs_redeeming.redeeming_transaction_id IN ? "+
			"and avm_outputs.id is null "+
			"and avm_outputs_redeeming.id not in ?",
			txIDs, inputIDs)
	} else {
		q = q.Where("avm_outputs_redeeming.redeeming_transaction_id IN ? "+
			"and avm_outputs.id is null ",
			txIDs)
	}
	_, err = q.
		LoadContext(ctx, &inputsRedeeming)
	if err != nil {
		return nil, err
	}

	outputs = append(outputs, inputs...)
	outputs = append(outputs, inputsRedeeming...)
	return outputs, nil
}

func (r *Reader) searchByID(ctx context.Context, id ids.ID, avaxAssetID ids.ID) (*models.SearchResults, error) {
	if txs, err := r.ListTransactions(ctx, &params.ListTransactionsParams{
		ListParams: params.ListParams{
			DisableCounting: true,
			ID:              &id,
		},
	}, avaxAssetID); err != nil {
		return nil, err
	} else if len(txs.Transactions) > 0 {
		return collateSearchResults(nil, txs)
	}

	return &models.SearchResults{}, nil
}

func (r *Reader) searchByShortID(ctx context.Context, id ids.ShortID) (*models.SearchResults, error) {
	listParams := params.ListParams{DisableCounting: true}

	if addrs, err := r.ListAddresses(ctx, &params.ListAddressesParams{ListParams: listParams, Address: &id}); err != nil {
		return nil, err
	} else if len(addrs.Addresses) > 0 {
		return collateSearchResults(addrs, nil)
	}

	return &models.SearchResults{}, nil
}

func (r *Reader) dressAddresses(ctx context.Context, dbRunner dbr.SessionRunner, addrs []*models.AddressInfo, version int, chainIDs []string) error {
	if len(addrs) == 0 {
		return nil
	}

	// Create a list of ids for querying, and a map for accumulating results later
	addrIDs := make([]models.Address, len(addrs))
	addrsByID := make(map[models.Address]*models.AddressInfo, len(addrs))
	for i, addr := range addrs {
		addrIDs[i] = addr.Address
		addrsByID[addr.Address] = addr

		addr.Assets = make(map[models.StringID]models.AssetInfo, 1)
	}

	// Load each Transaction Output for the tx, both inputs and outputs
	var rows []*struct {
		Address models.Address `json:"address"`
		models.AssetInfo
	}

	switch version {
	case 2:
		fallthrough
	case 1:
		_, err := dbRunner.
			Select(
				"avm_asset_address_counts.address",
				"avm_asset_address_counts.asset_id",
				"avm_asset_address_counts.transaction_count",
				"avm_asset_address_counts.total_received",
				"avm_asset_address_counts.total_sent",
				"avm_asset_address_counts.balance",
				"avm_asset_address_counts.utxo_count",
			).
			From("avm_asset_address_counts").
			Where("avm_asset_address_counts.address IN ?", addrIDs).
			GroupBy("avm_output_addresses.address", "avm_outputs.asset_id").
			LoadContext(ctx, &rows)
		if err != nil {
			return err
		}
	default:
		builder := dbRunner.
			Select(
				"avm_output_addresses.address",
				"avm_outputs.asset_id",
				"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
				"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN 1 ELSE 0 END), 0) AS utxo_count",
			).
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
			LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id").
			Where("avm_output_addresses.address IN ?", addrIDs).
			GroupBy("avm_output_addresses.address", "avm_outputs.asset_id")

		if len(chainIDs) > 0 {
			builder.Where("avm_outputs.chain_id IN ?", chainIDs)
		}

		if _, err := builder.LoadContext(ctx, &rows); err != nil {
			return err
		}
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

func collateSearchResults(addressResults *models.AddressList, transactionResults *models.TransactionList) (*models.SearchResults, error) {
	var (
		addresses    []*models.AddressInfo
		transactions []*models.Transaction
	)

	if addressResults != nil {
		addresses = addressResults.Addresses
	}

	if transactionResults != nil {
		transactions = transactionResults.Transactions
	}

	// Build overall SearchResults object from our pieces
	returnedResultCount := len(addresses) + len(transactions)
	if returnedResultCount > params.PaginationMaxLimit {
		returnedResultCount = params.PaginationMaxLimit
	}

	collatedResults := &models.SearchResults{
		Count: uint64(returnedResultCount),

		// Create a container for our combined results
		Results: make([]models.SearchResult, 0, returnedResultCount),
	}

	// Add each result to the list
	for _, result := range addresses {
		collatedResults.Results = append(collatedResults.Results, models.SearchResult{
			SearchResultType: models.ResultTypeAddress,
			Data:             result,
		})
	}
	for _, result := range transactions {
		collatedResults.Results = append(collatedResults.Results, models.SearchResult{
			SearchResultType: models.ResultTypeTransaction,
			Data:             result,
		})
	}

	return collatedResults, nil
}

func selectOutputs(dbRunner dbr.SessionRunner) *dbr.SelectBuilder {
	return dbRunner.Select("avm_outputs.id",
		"avm_outputs.transaction_id",
		"avm_outputs.output_index",
		"avm_outputs.asset_id",
		"avm_outputs.output_type",
		"avm_outputs.amount",
		"avm_outputs.locktime",
		"avm_outputs.threshold",
		"avm_outputs.created_at",
		"case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id",
		"avm_outputs.group_id",
		"avm_output_addresses.output_id AS output_id",
		"avm_output_addresses.address AS address",
		"avm_output_addresses.redeeming_signature AS signature",
		"addresses.public_key AS public_key",
	).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
		LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address")
}

// match selectOutputs but based from avm_outputs_redeeming
func selectOutputsRedeeming(dbRunner dbr.SessionRunner) *dbr.SelectBuilder {
	return dbRunner.Select("avm_outputs_redeeming.id",
		"avm_outputs_redeeming.intx as transaction_id",
		"avm_outputs_redeeming.output_index",
		"avm_outputs_redeeming.asset_id",
		"case when avm_outputs.output_type is null then 0 else avm_outputs.output_type end as output_type",
		"avm_outputs_redeeming.amount",
		"case when avm_outputs.locktime is null then 0 else avm_outputs.locktime end as locktime",
		"case when avm_outputs.threshold is null then 0 else avm_outputs.threshold end as threshold",
		"avm_outputs_redeeming.created_at",
		"case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id",
		"case when avm_outputs.group_id is null then 0 else avm_outputs.group_id end as group_id",
		"case when avm_output_addresses.output_id is null then '' else avm_output_addresses.output_id end AS output_id",
		"case when avm_output_addresses.address is null then '' else avm_output_addresses.address end AS address",
		"avm_output_addresses.redeeming_signature AS signature",
		"addresses.public_key AS public_key",
	).
		From("avm_outputs_redeeming").
		LeftJoin("avm_outputs", "avm_outputs_redeeming.id = avm_outputs.id").
		LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address")
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
