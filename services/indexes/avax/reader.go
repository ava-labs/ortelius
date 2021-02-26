// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"

	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"

	corethType "github.com/ava-labs/coreth/core/types"
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
	conns           *services.Connections
	sc              *services.Control
	avmLock         sync.RWMutex
	networkID       uint32
	chainConsumers  map[string]services.Consumer
	cChainCconsumer services.ConsumerCChain
}

func NewReader(networkID uint32, conns *services.Connections, chainConsumers map[string]services.Consumer, cChainCconsumer services.ConsumerCChain, sc *services.Control) *Reader {
	return &Reader{
		conns:           conns,
		sc:              sc,
		networkID:       networkID,
		chainConsumers:  chainConsumers,
		cChainCconsumer: cChainCconsumer,
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

	var assets []*models.Asset
	var txs []*models.Transaction
	var addresses []*models.AddressInfo

	lenSearchResults := func() int {
		return len(assets) + len(txs) + len(addresses)
	}

	assetsResp, err := r.ListAssets(ctx, &params.ListAssetsParams{ListParams: p.ListParams})
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

	builder1 := r.transactionQuery(dbRunner).
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
	dbRunner, err := r.conns.DB().NewSession("get_transaction_aggregates_histogram", cfg.RequestTimeout)
	if err != nil {
		return nil, err
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

func (r *Reader) ListTransactions(ctx context.Context, p *params.ListTransactionsParams, avaxAssetID ids.ID) (*models.TransactionList, error) {
	dbRunner, err := r.conns.DB().NewSession("get_transactions", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	p.ListParams.ObserveTimeProvided = true
	subquery := p.Apply(dbRunner.Select("id").From("avm_transactions"))

	var applySort2 func(sort params.TransactionSort)
	applySort2 = func(sort params.TransactionSort) {
		if p.ListParams.Query != "" {
			return
		}
		switch sort {
		case params.TransactionSortTimestampAsc:
			if len(p.ChainIDs) > 0 {
				subquery.OrderAsc("avm_transactions.chain_id")
			}
			subquery.OrderAsc("avm_transactions.created_at")
		case params.TransactionSortTimestampDesc:
			if len(p.ChainIDs) > 0 {
				subquery.OrderAsc("avm_transactions.chain_id")
			}
			subquery.OrderDesc("avm_transactions.created_at")
		default:
			applySort2(params.TransactionSortDefault)
		}
	}
	applySort2(p.Sort)

	var txs []*models.Transaction
	builder := r.transactionQuery(dbRunner)

	builder = builder.
		Join(subquery.As("avm_transactions_id"), "avm_transactions.id = avm_transactions_id.id")

	var applySort func(sort params.TransactionSort)
	applySort = func(sort params.TransactionSort) {
		if p.ListParams.Query != "" {
			return
		}
		switch sort {
		case params.TransactionSortTimestampAsc:
			if len(p.ChainIDs) > 0 {
				builder.OrderAsc("avm_transactions.chain_id")
			}
			builder.OrderAsc("avm_transactions.created_at")
		case params.TransactionSortTimestampDesc:
			if len(p.ChainIDs) > 0 {
				builder.OrderAsc("avm_transactions.chain_id")
			}
			builder.OrderDesc("avm_transactions.created_at")
		default:
			applySort(params.TransactionSortDefault)
		}
	}
	applySort(p.Sort)

	if _, err := builder.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}

	// Add all the addition information we might want
	if err := r.dressTransactions(ctx, dbRunner, txs, avaxAssetID, p.ListParams.ID, p.DisableGenesis); err != nil {
		return nil, err
	}

	listParamsOriginal := p.ListParams

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(txs)))
		if false {
			if len(txs) >= p.ListParams.Limit {
				p.ListParams = params.ListParams{}
				selector := p.Apply(dbRunner.
					Select("COUNT(avm_transactions.id)").
					From("avm_transactions"))

				if err := selector.LoadOneContext(ctx, &count); err != nil {
					return nil, err
				}
			}
		} else {
			if len(txs) >= p.ListParams.Limit {
				count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(txs)) + 1)
			}
		}
	}

	next := r.transactionProcessNext(txs, listParamsOriginal, p)

	return &models.TransactionList{ListMetadata: models.ListMetadata{
		Count: count,
	},
		Transactions: txs,
		StartTime:    listParamsOriginal.StartTime,
		EndTime:      listParamsOriginal.EndTime,
		Next:         next,
	}, nil
}

func (r *Reader) transactionProcessNext(txs []*models.Transaction, listParams params.ListParams, transactionsParams *params.ListTransactionsParams) *string {
	if len(txs) == 0 || len(txs) < listParams.Limit {
		return nil
	}

	lasttxCreated := txs[len(txs)-1].CreatedAt

	next := ""
	switch transactionsParams.Sort {
	case params.TransactionSortTimestampAsc:
		next = fmt.Sprintf("%s=%d", params.KeyStartTime, lasttxCreated.Add(time.Second).Unix())
	case params.TransactionSortTimestampDesc:
		next = fmt.Sprintf("%s=%d", params.KeyEndTime, lasttxCreated.Unix())
	}

	for k, vs := range listParams.Values {
		switch k {
		case params.KeyLimit:
			next = fmt.Sprintf("%s&%s=%d", next, params.KeyLimit, transactionsParams.ListParams.Limit)
		case params.KeyStartTime:
			if transactionsParams.Sort == params.TransactionSortTimestampDesc {
				for _, v := range vs {
					next = fmt.Sprintf("%s&%s=%s", next, params.KeyStartTime, v)
				}
			}
		case params.KeyEndTime:
			if transactionsParams.Sort == params.TransactionSortTimestampAsc {
				for _, v := range vs {
					next = fmt.Sprintf("%s&%s=%s", next, params.KeyEndTime, v)
				}
			}
		case params.KeyOffset:
		case params.KeySortBy:
		default:
			for _, v := range vs {
				next = fmt.Sprintf("%s&%s=%s", next, k, v)
			}
		}
	}
	next = fmt.Sprintf("%s&%s=%s", next, params.KeySortBy, transactionsParams.Sort)
	return &next
}

func (r *Reader) ListCTransactions(ctx context.Context, p *params.ListCTransactionsParams) (*models.CTransactionList, error) {
	toCTransactionData := func(t *types.Transaction) *models.CTransactionData {
		res := &models.CTransactionData{}
		res.Hash = t.Hash().Hex()
		if !strings.HasPrefix(res.Hash, "0x") {
			res.Hash = "0x" + res.Hash
		}
		res.Nonce = t.Nonce()
		if t.GasPrice() != nil {
			str := t.GasPrice().String()
			res.GasPrice = &str
		}
		res.GasLimit = t.Gas()
		if t.To() != nil {
			str := t.To().Hex()
			if !strings.HasPrefix(str, "0x") {
				str = "0x" + str
			}
			res.Recipient = &str
		}
		if t.Value() != nil {
			str := t.Value().String()
			res.Amount = &str
		}
		res.Payload = t.Data()
		v, s, r := t.RawSignatureValues()
		if v != nil {
			str := v.String()
			res.V = &str
		}
		if s != nil {
			str := s.String()
			res.S = &str
		}
		if r != nil {
			str := r.String()
			res.R = &str
		}
		return res
	}

	dbRunner, err := r.conns.DB().NewSession("list_ctransactions", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var dataList []*services.CvmTransactionsTxdata

	sq := dbRunner.Select(
		"hash",
		"block",
		"idx",
		"rcpt",
		"nonce",
		"serialization",
		"created_at",
	).From(services.TableCvmTransactionsTxdata)

	if len(p.CAddresses) > 0 {
		sq.
			Where("rcpt in ?", p.CAddresses)
	}
	if len(p.Hashes) > 0 {
		sq.
			Where("hash in ?", p.Hashes)
	}

	_, err = p.Apply(sq).
		OrderDesc("created_at").
		LoadContext(ctx, &dataList)
	if err != nil {
		return nil, err
	}

	trItemsByHash := make(map[string]*models.CTransactionData)

	trItems := make([]*models.CTransactionData, 0, len(dataList))
	hashes := make([]string, 0, len(dataList))

	for _, txdata := range dataList {
		var tr types.Transaction
		err := tr.UnmarshalJSON(txdata.Serialization)
		if err != nil {
			return nil, err
		}
		ctr := toCTransactionData(&tr)
		ctr.Block = txdata.Block
		ctr.CreatedAt = txdata.CreatedAt
		trItems = append(trItems, ctr)

		trItemsByHash[ctr.Hash] = ctr
		hashes = append(hashes, ctr.Hash)
	}

	var txTransactionTraceServices []*services.CvmTransactionsTxdataTrace
	_, err = dbRunner.Select(
		"hash",
		"idx",
		"to_addr",
		"from_addr",
		"call_type",
		"type",
		"serialization",
		"created_at",
	).From(services.TableCvmTransactionsTxdataTrace).
		Where("hash in ?", hashes).
		LoadContext(ctx, &txTransactionTraceServices)
	if err != nil {
		return nil, err
	}

	for _, txTransactionTraceService := range txTransactionTraceServices {
		txTransactionTraceModel := &models.CvmTransactionsTxDataTrace{}
		err = json.Unmarshal(txTransactionTraceService.Serialization, txTransactionTraceModel)
		if err != nil {
			return nil, err
		}
		if txTransactionTraceService.Idx == 0 {
			trItemsByHash[txTransactionTraceService.Hash].ToAddr = txTransactionTraceModel.ToAddr
			trItemsByHash[txTransactionTraceService.Hash].FromAddr = txTransactionTraceModel.FromAddr
		}

		toDecimal := func(v *string) {
			vh := strings.TrimPrefix(*v, "0x")
			vInt, okVInt := big.NewInt(0).SetString(vh, 16)
			if okVInt && vInt != nil {
				*v = vInt.String()
			}
		}
		toDecimal(&txTransactionTraceModel.Value)
		toDecimal(&txTransactionTraceModel.Gas)
		toDecimal(&txTransactionTraceModel.GasUsed)

		nilEmpty := func(v *string, def string) *string {
			if v != nil && *v == def {
				return nil
			}
			return v
		}
		txTransactionTraceModel.CreatedContractAddressHash = nilEmpty(txTransactionTraceModel.CreatedContractAddressHash, "")
		txTransactionTraceModel.Init = nilEmpty(txTransactionTraceModel.Init, "")
		txTransactionTraceModel.CreatedContractCode = nilEmpty(txTransactionTraceModel.CreatedContractCode, "")
		txTransactionTraceModel.Error = nilEmpty(txTransactionTraceModel.Error, "")
		txTransactionTraceModel.Input = nilEmpty(txTransactionTraceModel.Input, "0x")
		txTransactionTraceModel.Output = nilEmpty(txTransactionTraceModel.Output, "0x")

		if trItemsByHash[txTransactionTraceService.Hash].TracesMap == nil {
			trItemsByHash[txTransactionTraceService.Hash].TracesMap = make(map[uint32]*models.CvmTransactionsTxDataTrace)
		}
		if txTransactionTraceService.Idx+1 > trItemsByHash[txTransactionTraceService.Hash].TracesMax {
			trItemsByHash[txTransactionTraceService.Hash].TracesMax = txTransactionTraceService.Idx + 1
		}
		trItemsByHash[txTransactionTraceService.Hash].TracesMap[txTransactionTraceService.Idx] = txTransactionTraceModel
	}

	for _, trItem := range trItemsByHash {
		if trItem.TracesMax == 0 {
			continue
		}
		trItem.Traces = make([]*models.CvmTransactionsTxDataTrace, trItem.TracesMax)
		for k, v := range trItem.TracesMap {
			v.Idx = nil
			trItem.Traces[k] = v
		}
	}

	listParamsOriginal := p.ListParams

	return &models.CTransactionList{
		Transactions: trItems,
		StartTime:    listParamsOriginal.StartTime,
		EndTime:      listParamsOriginal.EndTime,
	}, nil
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

	var ts int64
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
	return time.Unix(ts, 0).UTC(), nil
}

// Load output data for all inputs and outputs into a single list
// We can't treat them separately because some my be both inputs and outputs
// for different transactions
type compositeRecord struct {
	models.Output
	models.OutputAddress
}

type rewardsTypeModel struct {
	Txid      models.StringID  `json:"txid"`
	Type      models.BlockType `json:"type"`
	CreatedAt time.Time        `json:"created_at"`
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

	rewardsTypesMap, err := r.resovleRewarded(ctx, dbRunner, txIDs)
	if err != nil {
		return err
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

	cvmin, cvmout, err := r.collectCvmTransactions(ctx, dbRunner, txIDs)
	if err != nil {
		return err
	}

	r.dressTransactionsTx(txs, disableGenesis, txID, avaxAssetID, inputsMap, outputsMap, inputTotalsMap, outputTotalsMap, rewardsTypesMap, cvmin, cvmout)
	return nil
}

func (r *Reader) dressTransactionsTx(txs []*models.Transaction, disableGenesis bool, txID *ids.ID, avaxAssetID ids.ID, inputsMap map[models.StringID]map[models.StringID]*models.Input, outputsMap map[models.StringID]map[models.StringID]*models.Output, inputTotalsMap map[models.StringID]map[models.StringID]*big.Int, outputTotalsMap map[models.StringID]map[models.StringID]*big.Int, rewardsTypesMap map[models.StringID]rewardsTypeModel, cvmins map[models.StringID][]models.Output, cvmouts map[models.StringID][]models.Output) {
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
		if inputs, ok := cvmins[tx.ID]; ok {
			for _, input := range inputs {
				var i models.Input
				var o models.Output = input
				i.Output = &o
				tx.Inputs = append(tx.Inputs, &i)
			}
		}

		if outputs, ok := outputsMap[tx.ID]; ok {
			for _, output := range outputs {
				tx.Outputs = append(tx.Outputs, output)
			}
		}
		if outputs, ok := cvmouts[tx.ID]; ok {
			for _, output := range outputs {
				var o models.Output = output
				tx.Outputs = append(tx.Outputs, &o)
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

		if rewardsType, ok := rewardsTypesMap[tx.ID]; ok {
			tx.Rewarded = rewardsType.Type == models.BlockTypeCommit
			tx.RewardedTime = &rewardsType.CreatedAt
		}
	}
}

func (r *Reader) resovleRewarded(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) (map[models.StringID]rewardsTypeModel, error) {
	rewardsTypes := []rewardsTypeModel{}
	blocktypes := []models.BlockType{models.BlockTypeAbort, models.BlockTypeCommit}
	_, err := dbRunner.Select("rewards.txid",
		"pvm_blocks.type",
		"pvm_blocks.created_at",
	).
		From("rewards").
		LeftJoin("pvm_blocks", "rewards.block_id = pvm_blocks.parent_id").
		Where("rewards.txid IN ? and pvm_blocks.type IN ?", txIDs, blocktypes).
		LoadContext(ctx, &rewardsTypes)
	if err != nil {
		return nil, err
	}

	rewardsTypesMap := make(map[models.StringID]rewardsTypeModel)
	for _, rewardsType := range rewardsTypes {
		rewardsTypesMap[rewardsType.Txid] = rewardsType
	}
	return rewardsTypesMap, nil
}

func (r *Reader) collectInsAndOuts(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) ([]*compositeRecord, error) {
	s1_0 := dbRunner.Select("avm_outputs.id").
		From("avm_outputs").
		Where("avm_outputs.transaction_id IN ?", txIDs)

	s1_1 := dbRunner.Select("avm_outputs_redeeming.id").
		From("avm_outputs_redeeming").
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ?", txIDs)

	var outputs []*compositeRecord
	_, err := selectOutputs(dbRunner).
		Join(dbr.Union(s1_0, s1_1).As("union_q_x"), "union_q_x.id = avm_outputs.id").LoadContext(ctx, &outputs)
	if err != nil {
		return nil, err
	}

	s2 := dbRunner.Select("avm_outputs.id").
		From("avm_outputs_redeeming").
		Join("avm_outputs", "avm_outputs.id = avm_outputs_redeeming.id").
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ?", txIDs)

	// if we get an input but have not yet seen the output.
	var outputs2 []*compositeRecord
	_, err = selectOutputsRedeeming(dbRunner).
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ? and avm_outputs_redeeming.id not in ?",
			txIDs, dbr.Select("sq_s2.id").From(s2.As("sq_s2"))).LoadContext(ctx, &outputs2)
	if err != nil {
		return nil, err
	}

	return append(outputs, outputs2...), nil
}

func (r *Reader) collectCvmTransactions(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) (map[models.StringID][]models.Output, map[models.StringID][]models.Output, error) {
	var cvmAddress []models.CvmOutput
	_, err := dbRunner.Select(
		"cvm_addresses.type",
		"cvm_transactions.type as transaction_type",
		"cvm_addresses.idx",
		"cast(cvm_addresses.amount as char) as amount",
		"cvm_addresses.nonce",
		"cvm_addresses.id",
		"cvm_addresses.transaction_id",
		"cvm_addresses.address",
		"cvm_addresses.asset_id",
		"cvm_addresses.created_at",
		"cvm_transactions.blockchain_id as chain_id",
		"cvm_transactions.block",
	).
		From("cvm_addresses").
		Join("cvm_transactions", "cvm_addresses.transaction_id=cvm_transactions.transaction_id").
		Where("cvm_addresses.transaction_id IN ?", txIDs).
		LoadContext(ctx, &cvmAddress)
	if err != nil {
		return nil, nil, err
	}

	ins := make(map[models.StringID][]models.Output)
	outs := make(map[models.StringID][]models.Output)

	for _, a := range cvmAddress {
		if _, ok := ins[a.TransactionID]; !ok {
			ins[a.TransactionID] = make([]models.Output, 0)
		}
		if _, ok := outs[a.TransactionID]; !ok {
			outs[a.TransactionID] = make([]models.Output, 0)
		}
		switch a.Type {
		case models.CChainIn:
			ins[a.TransactionID] = append(ins[a.TransactionID], r.mapOutput(a))
		case models.CchainOut:
			outs[a.TransactionID] = append(outs[a.TransactionID], r.mapOutput(a))
		}
	}

	return ins, outs, nil
}

func (r *Reader) mapOutput(a models.CvmOutput) models.Output {
	var o models.Output
	o.TransactionID = a.TransactionID
	o.ID = a.ID
	o.OutputIndex = a.Idx
	o.AssetID = a.AssetID
	o.Amount = a.Amount
	o.ChainID = a.ChainID
	o.CreatedAt = a.CreatedAt
	switch a.TransactionType {
	case models.CChainExport:
		o.OutputType = models.OutputTypesAtomicExportTx
	case models.CChainImport:
		o.OutputType = models.OutputTypesAtomicImportTx
	}
	o.CAddresses = []string{a.Address}
	o.Nonce = a.Nonce
	o.Block = a.Block
	return o
}

func (r *Reader) searchByID(ctx context.Context, id ids.ID, avaxAssetID ids.ID) (*models.SearchResults, error) {
	dbRunner, err := r.conns.DB().NewSession("search_by_id", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var txs []*models.Transaction
	builder := r.transactionQuery(dbRunner).
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

func selectOutputs(dbRunner dbr.SessionRunner) *dbr.SelectBuilder {
	return dbRunner.Select("avm_outputs.id",
		"avm_outputs.transaction_id",
		"avm_outputs.output_index",
		"avm_outputs.asset_id",
		"avm_outputs.output_type",
		"avm_outputs.amount",
		"avm_outputs.locktime",
		"avm_outputs.stake_locktime",
		"avm_outputs.threshold",
		"avm_outputs.created_at",
		"case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id",
		"avm_outputs.group_id",
		"avm_output_addresses.output_id AS output_id",
		"avm_output_addresses.address AS address",
		"avm_output_addresses.redeeming_signature AS signature",
		"addresses.public_key AS public_key",
		"avm_outputs.chain_id",
		"case when avm_outputs.payload is null then '' else avm_outputs.payload end as payload",
		"case when avm_outputs.stake is null then 0 else avm_outputs.stake end as stake",
		"case when avm_outputs.stakeableout is null then 0 else avm_outputs.stakeableout end as stakeableout",
		"case when avm_outputs.genesisutxo is null then 0 else avm_outputs.genesisutxo end as genesisutxo",
		"case when avm_outputs.frozen is null then 0 else avm_outputs.frozen end as frozen",
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
		"case when avm_outputs.stake_locktime is null then 0 else avm_outputs.stake_locktime end as stake_locktime",
		"case when avm_outputs.threshold is null then 0 else avm_outputs.threshold end as threshold",
		"avm_outputs_redeeming.created_at",
		"case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id",
		"case when avm_outputs.group_id is null then 0 else avm_outputs.group_id end as group_id",
		"case when avm_output_addresses.output_id is null then '' else avm_output_addresses.output_id end AS output_id",
		"case when avm_output_addresses.address is null then '' else avm_output_addresses.address end AS address",
		"avm_output_addresses.redeeming_signature AS signature",
		"addresses.public_key AS public_key",
		"avm_outputs_redeeming.chain_id",
		"case when avm_outputs.payload is null then '' else avm_outputs.payload end as payload",
		"case when avm_outputs.stake is null then 0 else avm_outputs.stake end as stake",
		"case when avm_outputs.stakeableout is null then 0 else avm_outputs.stakeableout end as stakeableout",
		"case when avm_outputs.genesisutxo is null then 0 else avm_outputs.genesisutxo end as genesisutxo",
		"case when avm_outputs.frozen is null then 0 else avm_outputs.frozen end as frozen",
	).
		From("avm_outputs_redeeming").
		LeftJoin("avm_outputs", "avm_outputs_redeeming.id = avm_outputs.id").
		LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address")
}

func (r *Reader) transactionQuery(dbRunner *dbr.Session) *dbr.SelectStmt {
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
		Serialization []byte
		ChainID       string
	}
	rows := []Row{}

	_, err = dbRunner.
		Select("serialization", "chain_id").
		From("pvm_blocks").
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

func (r *Reader) CTxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("ctx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		Serialization []byte
	}

	rows := []Row{}

	iv, res := big.NewInt(0).SetString(p.ID, 10)
	if iv != nil && res {
		_, err = dbRunner.
			Select("serialization").
			From("cvm_transactions").
			Where("block="+p.ID).
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
			Where("hash=? or parent_hash=? or block in ?",
				h,
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

	block, err := cblock.Unmarshal(row.Serialization)
	if err != nil {
		return nil, err
	}
	block.TxsBytes = nil

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
		Where("block="+p.ID).
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

	_, err = dbRunner.
		Select("serialization").
		From("cvm_transactions").
		Where("block="+p.ID).
		LoadContext(ctx, &rows)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	return r.cChainCconsumer.ParseJSON(row.Serialization)
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
