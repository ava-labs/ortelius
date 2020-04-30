// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"
)

const (
	PaginationMaxLimit      = 500
	PaginationDefaultLimit  = 500
	PaginationDefaultOffset = 0

	TransactionSortDefault       TransactionSort = TransactionSortTimestampAsc
	TransactionSortTimestampAsc                  = "timestamp-asc"
	TransactionSortTimestampDesc                 = "timestamp-desc"

	queryParamKeysID           = "id"
	queryParamKeysAddress      = "address"
	queryParamKeysAssetID      = "assetID"
	queryParamKeysQuery        = "query"
	queryParamKeysSortBy       = "sort"
	queryParamKeysLimit        = "limit"
	queryParamKeysOffset       = "offset"
	queryParamKeysSpent        = "spent"
	queryParamKeysStartTime    = "startTime"
	queryParamKeysEndTime      = "endTime"
	queryParamKeysIntervalSize = "intervalSize"
)

var (
	ErrUndefinedSort = errors.New("undefined sort")
)

//
// General route params
//

type SearchParams struct {
	ListParams
	Query string
}

func SearchParamsForHTTPRequest(r *http.Request) (*SearchParams, error) {
	q := r.URL.Query()

	listParams, err := ListParamForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	params := &SearchParams{
		ListParams: listParams,
	}

	queryStrs, ok := q[queryParamKeysQuery]
	if ok || len(queryStrs) >= 1 {
		params.Query = queryStrs[0]
	} else {
		return nil, errors.New("query required")
	}

	return params, nil
}

type AggregateParams struct {
	AssetID      *ids.ID
	StartTime    time.Time
	EndTime      time.Time
	IntervalSize time.Duration
}

func GetAggregateTransactionsParamsForHTTPRequest(r *http.Request) (*AggregateParams, error) {
	q := r.URL.Query()
	params := &AggregateParams{}

	if assetIDStrs, ok := q[queryParamKeysAssetID]; ok || len(assetIDStrs) >= 1 {
		assetID, err := ids.FromString(assetIDStrs[0])
		if err != nil {
			return nil, err
		}
		params.AssetID = &assetID
	}

	var err error
	params.StartTime, err = getQueryTime(q, queryParamKeysStartTime)
	if err != nil {
		return nil, err
	}

	params.EndTime, err = getQueryTime(q, queryParamKeysEndTime)
	if err != nil {
		return nil, err
	}

	params.IntervalSize, err = getQueryInterval(q, queryParamKeysIntervalSize)
	if err != nil {
		return nil, err
	}

	return params, nil
}

//
// List route params
//

type ListParams struct {
	Limit  int
	Offset int
}

func ListParamForHTTPRequest(r *http.Request) (params ListParams, err error) {
	q := r.URL.Query()
	params.Limit, err = getQueryInt(q, queryParamKeysLimit, PaginationDefaultLimit)
	if err != nil {
		return params, err
	}
	params.Offset, err = getQueryInt(q, queryParamKeysOffset, PaginationDefaultOffset)
	if err != nil {
		return params, err
	}
	return params, nil
}

func (p ListParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	if p.Limit > PaginationMaxLimit {
		p.Limit = PaginationMaxLimit
	}
	if p.Limit != 0 {
		b.Limit(uint64(p.Limit))
	}
	if p.Offset != 0 {
		b.Offset(uint64(p.Offset))
	}
	return b
}

type ListTransactionsParams struct {
	ListParams

	ID *ids.ID

	Query string

	Addresses []ids.ShortID
	AssetID   *ids.ID

	StartTime time.Time
	EndTime   time.Time

	Sort TransactionSort
}

func ListTransactionsParamsForHTTPRequest(r *http.Request) (*ListTransactionsParams, error) {
	q := r.URL.Query()

	listParams, err := ListParamForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	params := &ListTransactionsParams{
		Sort:       TransactionSortDefault,
		ListParams: listParams,
	}

	sortBys, ok := q[queryParamKeysSortBy]
	if ok && len(sortBys) >= 1 {
		params.Sort, _ = toTransactionSort(sortBys[0])
	}

	idStr := getQueryString(q, queryParamKeysID, "")
	if idStr != "" {
		id, err := ids.FromString(idStr)
		if err != nil {
			return nil, err
		}
		params.ID = &id
	}

	assetIDStr := getQueryString(q, queryParamKeysAssetID, "")
	if assetIDStr != "" {
		id, err := ids.FromString(assetIDStr)
		if err != nil {
			return nil, err
		}
		params.AssetID = &id
	}

	addressStrs := q[queryParamKeysAddress]
	for _, addressStr := range addressStrs {
		addr, err := ids.ShortFromString(addressStr)
		if err != nil {
			return nil, err
		}
		params.Addresses = append(params.Addresses, addr)
	}

	params.StartTime, err = getQueryTime(q, queryParamKeysStartTime)
	if err != nil {
		return nil, err
	}

	params.EndTime, err = getQueryTime(q, queryParamKeysEndTime)
	if err != nil {
		return nil, err
	}

	return params, nil
}

func (p *ListTransactionsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply(b)

	if p.ID != nil {
		b = b.
			Where("id = ?", p.ID.String()).
			Limit(1)
	}

	needOutputsJoin := len(p.Addresses) > 0 || p.AssetID != nil
	if needOutputsJoin {
		b = b.LeftJoin("avm_outputs", "avm_outputs.transaction_id = avm_transactions.id OR avm_outputs.redeeming_transaction_id = avm_transactions.id")
	}

	if len(p.Addresses) > 0 {
		addrs := make([]string, len(p.Addresses))
		for i, id := range p.Addresses {
			addrs[i] = id.String()
		}
		b = b.LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_output_addresses.address IN ?", addrs)
	}

	if p.AssetID != nil {
		b = b.Where("avm_outputs.asset_id = ?", p.AssetID.String())
	}

	if !p.StartTime.IsZero() {
		b = b.Where("created_at >= ?", p.StartTime)
	}
	if !p.EndTime.IsZero() {
		b = b.Where("created_at <= ?", p.EndTime)
	}

	var applySort func(b *dbr.SelectBuilder, sort TransactionSort) *dbr.SelectBuilder
	applySort = func(b *dbr.SelectBuilder, sort TransactionSort) *dbr.SelectBuilder {
		if p.Query != "" {
			return b
		}
		switch sort {
		case TransactionSortTimestampAsc:
			return b.OrderAsc("avm_transactions.created_at")
		case TransactionSortTimestampDesc:
			return b.OrderDesc("avm_transactions.created_at")
		}
		return applySort(b, TransactionSortDefault)
	}
	b = applySort(b, p.Sort)

	return b
}

type ListAssetsParams struct {
	ListParams
	ID    *ids.ID
	Query string
	Alias string
}

func ListAssetsParamsForHTTPRequest(r *http.Request) (*ListAssetsParams, error) {
	q := r.URL.Query()

	listParams, err := ListParamForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	params := &ListAssetsParams{
		ListParams: listParams,
	}

	params.ID, err = getQueryID(q, queryParamKeysID)
	if err != nil {
		return nil, err
	}

	return params, nil
}

func (p *ListAssetsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply(b)

	if p.ID != nil {
		b = b.
			Where("id = ?", p.ID.String()).
			Limit(1)
	}

	if p.Alias != "" {
		b = b.
			Where("alias = ?", p.Alias)
	}

	return b
}

type ListAddressesParams struct {
	ListParams
	Address *ids.ShortID
	Query   string
}

func ListAddressesParamsForHTTPRequest(r *http.Request) (*ListAddressesParams, error) {
	q := r.URL.Query()

	listParams, err := ListParamForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	params := &ListAddressesParams{
		ListParams: listParams,
	}

	params.Address, err = getQueryShortID(q, queryParamKeysAddress)
	if err != nil {
		return nil, err
	}

	return params, nil
}

func (p *ListAddressesParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply(b)

	if p.Address != nil {
		b = b.
			Where("avm_output_addresses.address = ?", p.Address.String()).
			Limit(1)
	}

	return b
}

type ListOutputsParams struct {
	ListParams
	ID    *ids.ID
	Query string

	Addresses []ids.ShortID
	Spent     *bool
}

func ListOutputsParamsForHTTPRequest(r *http.Request) (*ListOutputsParams, error) {
	q := r.URL.Query()

	listParams, err := ListParamForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	var b *bool
	params := &ListOutputsParams{
		Spent:      b,
		ListParams: listParams,
	}

	spentStrs, ok := q[queryParamKeysSpent]
	if ok || len(spentStrs) >= 1 {
		b, err := strconv.ParseBool(spentStrs[0])
		if err != nil {
			return nil, err
		}
		params.Spent = &b
	}

	return params, nil
}

func (p *ListOutputsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply(b)

	if p.Spent != nil {
		if *p.Spent {
			b = b.Where("avm_outputs.redeeming_transaction_id IS NOT NULL")
		} else {
			b = b.Where("avm_outputs.redeeming_transaction_id IS NULL")
		}
	}

	if p.Addresses != nil {
		addrStrs := make([]string, len(p.Addresses))
		for i, addr := range p.Addresses {
			addrStrs[i] = addr.String()
		}
		b = b.LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_output_addresses.address IN ?", addrStrs)
	}

	if p.ID != nil {
		b = b.
			Where("id = ?", p.ID.String()).
			Limit(1)
	}

	return b
}

//
// Sorting
//
type TransactionSort string

func toTransactionSort(s string) (TransactionSort, error) {
	switch s {
	case TransactionSortTimestampAsc:
		return TransactionSortTimestampAsc, nil
	case TransactionSortTimestampDesc:
		return TransactionSortTimestampDesc, nil
	}
	return TransactionSortDefault, ErrUndefinedSort
}

//
// Query string helpers
//
func getQueryInt(q url.Values, key string, defaultVal int) (val int, err error) {
	strs := q[key]
	if len(strs) >= 1 {
		return strconv.Atoi(strs[0])
	}
	return defaultVal, err
}

func getQueryString(q url.Values, key string, defaultVal string) string {
	strs := q[key]
	if len(strs) >= 1 {
		return strs[0]
	}
	return defaultVal
}

func getQueryTime(q url.Values, key string) (time.Time, error) {
	strs, ok := q[key]
	if !ok || len(strs) < 1 {
		return time.Time{}, nil
	}

	timestamp, err := strconv.Atoi(strs[0])
	if err == nil {
		return time.Unix(int64(timestamp), 0).UTC(), nil
	}

	t, err := time.Parse(time.RFC3339, strs[0])
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func getQueryID(q url.Values, key string) (*ids.ID, error) {
	idStr := getQueryString(q, key, "")
	if idStr == "" {
		return nil, nil
	}

	id, err := ids.FromString(idStr)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func getQueryShortID(q url.Values, key string) (*ids.ShortID, error) {
	idStr := getQueryString(q, key, "")
	if idStr == "" {
		return nil, nil
	}

	id, err := ids.ShortFromString(idStr)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func getQueryInterval(q url.Values, key string) (time.Duration, error) {
	intervalStrs, ok := q[key]
	if !ok || len(intervalStrs) < 1 {
		return 0, nil
	}

	interval, ok := IntervalNames[intervalStrs[0]]
	if !ok {
		var err error
		interval, err = time.ParseDuration(intervalStrs[0])
		if err != nil {
			return 0, err
		}
	}
	return interval, nil
}
