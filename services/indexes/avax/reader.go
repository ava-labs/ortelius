// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
	corethType "github.com/ava-labs/coreth/core/types"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/modelsc"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
	"github.com/gocraft/dbr/v2"
)

const (
	MaxAggregateIntervalCount = 20000

	MinSearchQueryLength = 1
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
		"avm_outputs.frozen",
	}
)

type Reader struct {
	conns           *utils.Connections
	sc              *servicesctrl.Control
	avmLock         sync.RWMutex
	networkID       uint32
	chainConsumers  map[string]services.Consumer
	cChainCconsumer services.ConsumerCChain

	readerAggregate ReaderAggregate

	doneCh chan struct{}
}

func NewReader(networkID uint32, conns *utils.Connections, chainConsumers map[string]services.Consumer, cChainCconsumer services.ConsumerCChain, sc *servicesctrl.Control) (*Reader, error) {
	reader := &Reader{
		conns:           conns,
		sc:              sc,
		networkID:       networkID,
		chainConsumers:  chainConsumers,
		cChainCconsumer: cChainCconsumer,
		doneCh:          make(chan struct{}),
	}

	err := reader.aggregateProcessor()
	if err != nil {
		return nil, err
	}

	return reader, nil
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

	var assets []*models.Asset
	var txs []*models.Transaction
	var addresses []*models.AddressInfo

	lenSearchResults := func() int {
		return len(assets) + len(txs) + len(addresses)
	}

	assetsResp, err := r.ListAssets(ctx, &params.ListAssetsParams{ListParams: p.ListParams}, nil)
	if err != nil {
		return nil, err
	}
	assets = assetsResp.Assets
	if lenSearchResults() >= p.ListParams.Limit {
		return collateSearchResults(assets, addresses, txs)
	}

	if false {
		// The query string was not an id/shortid so perform an ID prefix search
		// against transactions and addresses.
		transactionsRes, err := r.ListTransactions(ctx, &params.ListTransactionsParams{ListParams: p.ListParams}, avaxAssetID)
		if err != nil {
			return nil, err
		}
		txs = transactionsRes.Transactions
		if lenSearchResults() >= p.ListParams.Limit {
			return collateSearchResults(assets, addresses, txs)
		}
	}

	dbRunner, err := r.conns.DB().NewSession("search", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	builder1 := transactionQuery(dbRunner).
		Where(dbr.Like("avm_transactions.id", p.ListParams.Query+"%")).
		OrderDesc("avm_transactions.created_at").
		Limit(uint64(p.ListParams.Limit))
	if _, err := builder1.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}
	if lenSearchResults() >= p.ListParams.Limit {
		return collateSearchResults(assets, addresses, txs)
	}

	/*
		A regex search on address can't work..
		And on output_id makes no sense...

		Addresses are stored in the db in 20byte hex, and the query is by address 'fuji.....'
		There is no way to convert a part of an address into a "few" bytes...

		Future: store the address in the db in the address format 'fuji.....'
		then a part query could work.
	*/
	if false {
		addressesRes, err := r.ListAddresses(ctx, &params.ListAddressesParams{ListParams: p.ListParams})
		if err != nil {
			return nil, err
		}
		addresses = addressesRes.Addresses
		if lenSearchResults() >= p.ListParams.Limit {
			return collateSearchResults(assets, addresses, txs)
		}
	}

	return collateSearchResults(assets, addresses, txs)
}

func (r *Reader) TxfeeAggregate(ctx context.Context, params *params.TxfeeAggregateParams) (*models.TxfeeAggregatesHistogram, error) {
	// Validate params and set defaults if necessary
	if params.ListParams.StartTime.IsZero() {
		var err error
		params.ListParams.StartTime, err = r.getFirstTransactionTime(ctx, params.ChainIDs)
		if err != nil {
			return nil, err
		}
	}

	var intervals []models.TxfeeAggregates

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
	dbRunner, err := r.conns.DB().NewSession("get_txfee_aggregates_histogram", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var builder *dbr.SelectStmt

	columns := []string{
		"CAST(COALESCE(SUM(avm_transactions.txfee), 0) AS CHAR) AS txfee",
	}

	if requestedIntervalCount > 0 {
		columns = append(columns, fmt.Sprintf(
			"FLOOR((UNIX_TIMESTAMP(avm_transactions.created_at)-%d) / %d) AS idx",
			params.ListParams.StartTime.Unix(),
			intervalSeconds))
	}

	builder = dbRunner.
		Select(columns...).
		From("avm_transactions").
		Where("avm_transactions.created_at >= ?", params.ListParams.StartTime).
		Where("avm_transactions.created_at < ?", params.ListParams.EndTime)

	if requestedIntervalCount > 0 {
		builder.
			GroupBy("idx").
			OrderAsc("idx").
			Limit(uint64(requestedIntervalCount))
	}

	if len(params.ChainIDs) != 0 {
		builder.Where("avm_transactions.chain_id IN ?", params.ChainIDs)
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
			return &models.TxfeeAggregatesHistogram{
				TxfeeAggregates: intervals[0],
				StartTime:       params.ListParams.StartTime,
				EndTime:         params.ListParams.EndTime,
			}, nil
		}
		return &models.TxfeeAggregatesHistogram{
			StartTime: params.ListParams.StartTime,
			EndTime:   params.ListParams.EndTime,
		}, nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &models.TxfeeAggregatesHistogram{IntervalSize: params.IntervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		startTS = params.ListParams.StartTime.Unix() + (int64(intervalIdx) * intervalSeconds)
		return time.Unix(startTS, 0).UTC(),
			time.Unix(startTS+intervalSeconds-1, 0).UTC()
	}

	padTo := func(slice []models.TxfeeAggregates, to int) []models.TxfeeAggregates {
		for i := len(slice); i < to; i = len(slice) {
			slice = append(slice, models.TxfeeAggregates{Idx: i})
			slice[i].StartTime, slice[i].EndTime = timesForInterval(i)
		}
		return slice
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.TxfeeAggregates = models.TxfeeAggregates{StartTime: params.ListParams.StartTime, EndTime: params.ListParams.EndTime}
	var (
		bigIntFromStringOK bool
		totalVolume        = big.NewInt(0)
		intervalVolume     = big.NewInt(0)
	)

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]models.TxfeeAggregates, 0, requestedIntervalCount)
	for _, interval := range intervals {
		// Pad up to this interval's position
		aggs.Intervals = padTo(aggs.Intervals, interval.Idx)

		// Format this interval
		interval.StartTime, interval.EndTime = timesForInterval(interval.Idx)

		// Parse volume into a big.Int
		_, bigIntFromStringOK = intervalVolume.SetString(string(interval.Txfee), 10)
		if !bigIntFromStringOK {
			return nil, ErrFailedToParseStringAsBigInt
		}

		// Add to the overall aggregates counts
		totalVolume.Add(totalVolume, intervalVolume)

		// Add to the list of intervals
		aggs.Intervals = append(aggs.Intervals, interval)
	}
	// Add total aggregated token amounts
	aggs.TxfeeAggregates.Txfee = models.TokenAmount(totalVolume.String())

	// Add any missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	aggs.StartTime = params.ListParams.StartTime
	aggs.EndTime = params.ListParams.EndTime

	return aggs, nil
}

func (r *Reader) Aggregate(ctx context.Context, params *params.AggregateParams, conns *utils.Connections) (*models.AggregatesHistogram, error) {
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

	var dbRunner *dbr.Session
	var err error

	if conns != nil {
		dbRunner = conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("get_transaction_aggregates_histogram"))
	} else {
		dbRunner, err = r.conns.DB().NewSession("get_transaction_aggregates_histogram", cfg.RequestTimeout)
		if err != nil {
			return nil, err
		}
	}

	var builder *dbr.SelectStmt

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

	if len(params.ChainIDs) != 0 {
		builder.Where("avm_outputs.chain_id IN ?", params.ChainIDs)
	}

	if params.AssetID != nil {
		builder.Where("avm_outputs.asset_id = ?", params.AssetID.String())
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

func (r *Reader) ListAddresses(ctx context.Context, p *params.ListAddressesParams) (*models.AddressList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_addresses", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var rows []*struct {
		ChainID models.StringID `json:"chainID"`
		Address models.Address  `json:"address"`
		models.AssetInfo
		PublicKey []byte `json:"publicKey"`
	}

	var ua *dbr.SelectStmt
	var baseq *dbr.SelectStmt

	if r.sc.IsAccumulateBalanceReader {
		ua = dbRunner.Select("avm_outputs.chain_id", "avm_output_addresses.address").
			Distinct().
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			OrderAsc("avm_outputs.chain_id").
			OrderAsc("avm_output_addresses.address")

		baseq = p.Apply(dbRunner.
			Select(
				"accumulate_balances_received.chain_id",
				"accumulate_balances_received.address",
				"accumulate_balances_received.asset_id",
				"accumulate_balances_transactions.transaction_count",
				"accumulate_balances_received.total_amount as total_received",
				"case when accumulate_balances_sent.total_amount is null then 0 else accumulate_balances_sent.total_amount end as total_sent",
				"accumulate_balances_received.total_amount - case when accumulate_balances_sent.total_amount is null then 0 else accumulate_balances_sent.total_amount end as balance",
				"accumulate_balances_received.utxo_count - case when accumulate_balances_sent.utxo_count is null then 0 else accumulate_balances_sent.utxo_count end as utxo_count",
			).
			From("accumulate_balances_received").
			LeftJoin("accumulate_balances_sent", "accumulate_balances_received.id = accumulate_balances_sent.id").
			LeftJoin("accumulate_balances_transactions", "accumulate_balances_received.id = accumulate_balances_transactions.id").
			OrderAsc("accumulate_balances_received.chain_id").
			OrderAsc("accumulate_balances_received.address").
			OrderAsc("accumulate_balances_received.asset_id"), true)
	} else {
		ua = dbRunner.Select("avm_outputs.chain_id", "avm_output_addresses.address").
			Distinct().
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			OrderAsc("avm_outputs.chain_id").
			OrderAsc("avm_output_addresses.address")

		baseq = dbRunner.
			Select(
				"avm_outputs.chain_id",
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
			Where("avm_output_addresses.address in ?", dbRunner.Select(
				"avm_outputs_ua.address",
			).From(p.Apply(ua, false).As("avm_outputs_ua"))).
			GroupBy("avm_outputs.chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
			OrderAsc("avm_outputs.chain_id").
			OrderAsc("avm_output_addresses.address").
			OrderAsc("avm_outputs.asset_id")

		if len(p.ChainIDs) != 0 {
			baseq.Where("avm_outputs.chain_id IN ?", p.ChainIDs)
		}
	}

	builder := dbRunner.Select(
		"avm_outputs_j.chain_id",
		"avm_outputs_j.address",
		"avm_outputs_j.asset_id",
		"avm_outputs_j.transaction_count",
		"avm_outputs_j.total_received",
		"avm_outputs_j.total_sent",
		"avm_outputs_j.balance",
		"avm_outputs_j.utxo_count",
		"addresses.public_key",
	).From(baseq.As("avm_outputs_j")).
		LeftJoin("addresses", "addresses.address = avm_outputs_j.address")

	_, err = builder.
		LoadContext(ctx, &rows)
	if err != nil {
		return nil, err
	}

	addresses := make([]*models.AddressInfo, 0, len(rows))

	addrsByID := make(map[string]*models.AddressInfo)

	for _, row := range rows {
		k := fmt.Sprintf("%s:%s", row.ChainID, row.Address)
		addr, ok := addrsByID[k]
		if !ok {
			addr = &models.AddressInfo{
				ChainID:   row.ChainID,
				Address:   row.Address,
				PublicKey: row.PublicKey,
				Assets:    make(map[models.StringID]models.AssetInfo),
			}
			addrsByID[k] = addr
		}
		addr.Assets[row.AssetID] = row.AssetInfo
	}
	for _, addr := range addrsByID {
		addresses = append(addresses, addr)
	}

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(addresses)))
		if len(addresses) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			sqc := p.Apply(ua, true)
			buildercnt := dbRunner.Select(
				"count(*)",
			).From(sqc.As("avm_outputs_j"))
			err = buildercnt.
				LoadOneContext(ctx, &count)
			if err != nil {
				return nil, err
			}
		}
	}

	return &models.AddressList{ListMetadata: models.ListMetadata{Count: count}, Addresses: addresses}, nil
}

func (r *Reader) ListOutputs(ctx context.Context, p *params.ListOutputsParams) (*models.OutputList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_transaction_outputs", cfg.RequestTimeout)
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
		ListParams: params.ListParams{ID: &id, DisableCounting: true},
	}, avaxAssetID)
	if err != nil {
		return nil, err
	}
	if len(txList.Transactions) > 0 {
		return txList.Transactions[0], nil
	}
	return nil, nil
}

func (r *Reader) GetAddress(ctx context.Context, p *params.ListAddressesParams) (*models.AddressInfo, error) {
	addressList, err := r.ListAddresses(ctx, p)
	if err != nil {
		return nil, err
	}
	if len(addressList.Addresses) > 1 {
		collated := make(map[string]*models.AddressInfo)
		for _, a := range addressList.Addresses {
			key := string(a.Address)
			if addressInfo, ok := collated[key]; ok {
				if addressInfo.Assets == nil {
					addressInfo.Assets = make(map[models.StringID]models.AssetInfo)
				}
				collated[key].ChainID = ""
				collated[key].Assets = addAssetInfoMap(addressInfo.Assets, a.Assets)
			} else {
				collated[key] = a
			}
		}
		addressList.Addresses = []*models.AddressInfo{}
		for _, v := range collated {
			addressList.Addresses = append(addressList.Addresses, v)
		}
	}
	if len(addressList.Addresses) > 0 {
		return addressList.Addresses[0], nil
	}
	return nil, err
}

func (r *Reader) GetOutput(ctx context.Context, id ids.ID) (*models.Output, error) {
	outputList, err := r.ListOutputs(ctx,
		&params.ListOutputsParams{
			ListParams: params.ListParams{ID: &id, DisableCounting: true},
		})
	if err != nil {
		return nil, err
	}
	if len(outputList.Outputs) > 0 {
		return outputList.Outputs[0], nil
	}
	return nil, err
}

func (r *Reader) AddressChains(ctx context.Context, p *params.AddressChainsParams) (*models.AddressChains, error) {
	dbRunner, err := r.conns.DB().NewSession("addressChains", cfg.RequestTimeout)
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
	dbRunner, err := r.conns.DB().NewSession("get_first_transaction_time", cfg.RequestTimeout)
	if err != nil {
		return time.Time{}, err
	}

	var ts float64
	builder := dbRunner.
		Select("COALESCE(UNIX_TIMESTAMP(MIN(created_at)), 0)").
		From("avm_transactions")

	if len(chainIDs) > 0 {
		builder.Where("avm_transactions.chain_id IN ?", chainIDs)
	}

	err = builder.LoadOneContext(ctx, &ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(math.Floor(ts)), 0).UTC(), nil
}

func (r *Reader) searchByID(ctx context.Context, id ids.ID, avaxAssetID ids.ID) (*models.SearchResults, error) {
	dbRunner, err := r.conns.DB().NewSession("search_by_id", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var txs []*models.Transaction
	builder := transactionQuery(dbRunner).
		Where("avm_transactions.id = ?", id.String()).Limit(1)
	if _, err := builder.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}

	if len(txs) > 0 {
		return collateSearchResults(nil, nil, txs)
	}

	if false {
		if txs, err := r.ListTransactions(ctx, &params.ListTransactionsParams{
			ListParams: params.ListParams{
				DisableCounting: true,
				ID:              &id,
			},
		}, avaxAssetID); err != nil {
			return nil, err
		} else if len(txs.Transactions) > 0 {
			return collateSearchResults(nil, nil, txs.Transactions)
		}
	}

	return &models.SearchResults{}, nil
}

func (r *Reader) searchByShortID(ctx context.Context, id ids.ShortID) (*models.SearchResults, error) {
	listParams := params.ListParams{DisableCounting: true}

	if addrs, err := r.ListAddresses(ctx, &params.ListAddressesParams{ListParams: listParams, Address: &id}); err != nil {
		return nil, err
	} else if len(addrs.Addresses) > 0 {
		return collateSearchResults(nil, addrs.Addresses, nil)
	}

	return &models.SearchResults{}, nil
}

func collateSearchResults(assets []*models.Asset, addresses []*models.AddressInfo, transactions []*models.Transaction) (*models.SearchResults, error) {
	// Build overall SearchResults object from our pieces
	returnedResultCount := len(assets) + len(addresses) + len(transactions)
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
	for _, result := range assets {
		collatedResults.Results = append(collatedResults.Results, models.SearchResult{
			SearchResultType: models.ResultTypeAsset,
			Data:             result,
		})
	}

	return collatedResults, nil
}

func transactionQuery(dbRunner *dbr.Session) *dbr.SelectStmt {
	return dbRunner.
		Select(
			"avm_transactions.id",
			"avm_transactions.chain_id",
			"avm_transactions.type",
			"avm_transactions.memo",
			"avm_transactions.created_at",
			"avm_transactions.txfee",
			"avm_transactions.genesis",
			"case when transactions_epoch.epoch is null then 0 else transactions_epoch.epoch end as epoch",
			"case when transactions_epoch.vertex_id is null then '' else transactions_epoch.vertex_id end as vertex_id",
			"case when transactions_validator.node_id is null then '' else transactions_validator.node_id end as validator_node_id",
			"case when transactions_validator.start is null then 0 else transactions_validator.start end as validator_start",
			"case when transactions_validator.end is null then 0 else transactions_validator.end end as validator_end",
			"case when transactions_block.tx_block_id is null then '' else transactions_block.tx_block_id end as tx_block_id",
		).
		From("avm_transactions").
		LeftJoin("transactions_epoch", "avm_transactions.id = transactions_epoch.id").
		LeftJoin("transactions_validator", "avm_transactions.id = transactions_validator.id").
		LeftJoin("transactions_block", "avm_transactions.id = transactions_block.id")
}

func (r *Reader) chainWriter(chainID string) (services.Consumer, error) {
	r.avmLock.RLock()
	w, ok := r.chainConsumers[chainID]
	r.avmLock.RUnlock()
	if ok {
		return w, nil
	}
	return nil, fmt.Errorf("unimplemented")
}

func (r *Reader) ATxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("atx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		Serialization []byte
		ChainID       string
	}
	rows := []Row{}

	_, err = dbRunner.
		Select("canonical_serialization as serialization", "chain_id").
		From("avm_transactions").
		Where("id=?", p.ID).
		LoadContext(ctx, &rows)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	var c services.Consumer
	c, err = r.chainWriter(row.ChainID)
	if err != nil {
		return nil, err
	}
	j, err := c.ParseJSON(row.Serialization)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (r *Reader) PTxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("ptx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		ID            string
		Serialization []byte
		ChainID       string
	}
	rows := []Row{}

	idInt, ok := big.NewInt(0).SetString(p.ID, 10)
	if idInt != nil && ok {
		_, err = dbRunner.
			Select("id", "serialization", "chain_id").
			From(db.TablePvmBlocks).
			Where("height="+idInt.String()).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	} else {
		_, err = dbRunner.
			Select("id", "serialization", "chain_id").
			From(db.TablePvmBlocks).
			Where("id=?", p.ID).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	}
	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	// check if the proposer exists, and pull the serialization from the tx_pool.
	proposerrows := []Row{}

	_, err = dbRunner.
		Select(db.TableTxPool+".serialization").
		From(db.TablePvmProposer).
		Join(db.TableTxPool, db.TablePvmProposer+".proposer_blk_id = "+db.TableTxPool+".msg_key").
		Where(db.TablePvmProposer+".blk_id=?", row.ID).
		LoadContext(ctx, &proposerrows)
	if err != nil {
		return nil, err
	}

	if len(proposerrows) > 0 {
		proposerrow := proposerrows[0]
		row.Serialization = proposerrow.Serialization
	}

	var c services.Consumer
	c, err = r.chainWriter(row.ChainID)
	if err != nil {
		return nil, err
	}
	j, err := c.ParseJSON(row.Serialization)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (r *Reader) CTxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("ctx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		Serialization []byte
	}

	rows := []Row{}

	idInt, ok := big.NewInt(0).SetString(p.ID, 10)
	if idInt != nil && ok {
		_, err = dbRunner.
			Select("serialization").
			From("cvm_transactions").
			Where("block="+idInt.String()).
			Limit(1).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	} else {
		h := p.ID
		if !strings.HasPrefix(p.ID, "0x") {
			h = "0x" + h
		}

		sq := dbRunner.
			Select("block").
			From("cvm_transactions_txdata").
			Where("hash=? or rcpt=?", h, h)

		_, err = dbRunner.
			Select("serialization").
			From("cvm_transactions").
			Where("hash=? or block in ?",
				h,
				dbRunner.Select("block").From(sq.As("sq")),
			).
			OrderDesc("block").
			Limit(1).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	}

	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	// copy of the Block object for export to json
	type BlockExport struct {
		BlockNumber    string                   `json:"blockNumber"`
		BlockHash      string                   `json:"blockHash"`
		BlockID        string                   `json:"blockID"`
		Header         corethType.Header        `json:"header"`
		Uncles         []corethType.Header      `json:"uncles"`
		TxsBytes       [][]byte                 `json:"txs"`
		Version        uint32                   `json:"version"`
		BlockExtraData []byte                   `json:"blockExtraData"`
		BlockExtraID   string                   `json:"blockExtraID"`
		Txs            []corethType.Transaction `json:"transactions,omitempty"`
		Logs           []corethType.Log         `json:"logs,omitempty"`
	}

	unserializedBlock, err := modelsc.Unmarshal(row.Serialization)
	if err != nil {
		return nil, err
	}

	txIDs := ""
	if len(unserializedBlock.BlockExtraData) != 0 {
		txID, err := ids.ToID(hashing.ComputeHash256(unserializedBlock.BlockExtraData))
		if err != nil {
			return nil, err
		}
		txIDs = txID.String()
	}

	blockHash := unserializedBlock.Header.Hash()
	hID, err := ids.ToID(blockHash[:])
	if err != nil {
		return nil, err
	}

	block := &BlockExport{
		BlockNumber:    unserializedBlock.Header.Number.String(),
		BlockHash:      blockHash.String(),
		BlockID:        hID.String(),
		Header:         unserializedBlock.Header,
		Uncles:         unserializedBlock.Uncles,
		Version:        unserializedBlock.Version,
		BlockExtraData: unserializedBlock.BlockExtraData,
		BlockExtraID:   txIDs,
		Txs:            unserializedBlock.Txs,
	}

	type RowData struct {
		Idx           uint64
		Serialization []byte
	}
	rowsData := []RowData{}

	_, err = dbRunner.
		Select(
			"idx",
			"serialization",
		).
		From("cvm_transactions_txdata").
		Where("block="+block.Header.Number.String()).
		OrderAsc("idx").
		LoadContext(ctx, &rowsData)
	if err != nil {
		return nil, err
	}

	block.Txs = make([]corethType.Transaction, 0, len(rowsData))
	for _, rowData := range rowsData {
		var tr corethType.Transaction
		err := tr.UnmarshalJSON(rowData.Serialization)
		if err != nil {
			return nil, err
		}
		block.Txs = append(block.Txs, tr)
	}

	type RowLog struct {
		Serialization []byte
	}
	rowsLog := []RowLog{}

	_, err = dbRunner.
		Select(
			"serialization",
		).
		From(db.TableCvmLogs).
		Where("block="+block.Header.Number.String()).
		LoadContext(ctx, &rowsLog)
	if err != nil {
		return nil, err
	}

	block.Logs = make([]corethType.Log, 0, len(rowsData))
	for _, rowData := range rowsLog {
		var clog corethType.Log
		err := json.Unmarshal(rowData.Serialization, &clog)
		if err != nil {
			return nil, err
		}
		block.Logs = append(block.Logs, clog)
	}

	sort.Slice(block.Logs, func(i, j int) bool {
		return block.Logs[i].Index < block.Logs[j].Index
	})

	return json.Marshal(block)
}

func (r *Reader) ETxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("etx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		Serialization []byte
	}
	rows := []Row{}

	idInt, ok := big.NewInt(0).SetString(p.ID, 10)
	if idInt != nil && ok {
		_, err = dbRunner.
			Select("serialization").
			From(db.TableCvmTransactions).
			Where("block="+idInt.String()).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	}

	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	return r.cChainCconsumer.ParseJSON(row.Serialization)
}

func (r *Reader) RawTransaction(ctx context.Context, id ids.ID) (*models.RawTx, error) {
	dbRunner, err := r.conns.DB().NewSession("raw-transaction", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type SerialData struct {
		Serialization []byte
	}

	serialData := SerialData{}

	err = dbRunner.
		Select("serialization").
		From(db.TableTxPool).
		Where("msg_key=?", id.String()).
		LoadOneContext(ctx, &serialData)
	if err != nil {
		return nil, err
	}

	rawTx := models.RawTx{Tx: "0x" + hex.EncodeToString(serialData.Serialization)}

	return &rawTx, nil
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
