// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"

	"github.com/ava-labs/ortelius/services/params"
)

const (
	TransactionSortDefault       TransactionSort = TransactionSortTimestampAsc
	TransactionSortTimestampAsc                  = "timestamp-asc"
	TransactionSortTimestampDesc                 = "timestamp-desc"
)

//
// General route params
//

type SearchParams struct {
	params.ListParams
	Query string
}

func SearchParamsForHTTPRequest(r *http.Request) (*SearchParams, error) {
	q := r.URL.Query()

	listParams, err := params.ListParamsForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	p := &SearchParams{ListParams: listParams}

	queryStrs, ok := q[params.KeySearchQuery]
	if ok || len(queryStrs) >= 1 {
		p.Query = queryStrs[0]
	} else {
		return nil, errors.New("query required")
	}

	return p, nil
}

type AggregateParams struct {
	AssetID      *ids.ID
	StartTime    time.Time
	EndTime      time.Time
	IntervalSize time.Duration
}

func GetAggregateTransactionsParamsForHTTPRequest(r *http.Request) (*AggregateParams, error) {
	q := r.URL.Query()
	p := &AggregateParams{}

	if assetIDStrs, ok := q[params.KeyAssetID]; ok || len(assetIDStrs) >= 1 {
		assetID, err := ids.FromString(assetIDStrs[0])
		if err != nil {
			return nil, err
		}
		p.AssetID = &assetID
	}

	var err error
	p.StartTime, err = params.GetQueryTime(q, params.KeyStartTime)
	if err != nil {
		return nil, err
	}

	p.EndTime, err = params.GetQueryTime(q, params.KeyEndTime)
	if err != nil {
		return nil, err
	}

	p.IntervalSize, err = params.GetQueryInterval(q, params.KeyIntervalSize)
	if err != nil {
		return nil, err
	}

	return p, nil
}

//
// List route params
//

type ListTransactionsParams struct {
	params.ListParams

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

	listParams, err := params.ListParamsForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	p := &ListTransactionsParams{
		Sort:       TransactionSortDefault,
		ListParams: listParams,
	}

	sortBys, ok := q[params.KeySortBy]
	if ok && len(sortBys) >= 1 {
		p.Sort, _ = toTransactionSort(sortBys[0])
	}

	idStr := params.GetQueryString(q, params.KeyID, "")
	if idStr != "" {
		id, err := ids.FromString(idStr)
		if err != nil {
			return nil, err
		}
		p.ID = &id
	}

	assetIDStr := params.GetQueryString(q, params.KeyAssetID, "")
	if assetIDStr != "" {
		id, err := ids.FromString(assetIDStr)
		if err != nil {
			return nil, err
		}
		p.AssetID = &id
	}

	addressStrs := q[params.KeyAddress]
	for _, addressStr := range addressStrs {
		addr, err := ids.ShortFromString(addressStr)
		if err != nil {
			return nil, err
		}
		p.Addresses = append(p.Addresses, addr)
	}

	p.StartTime, err = params.GetQueryTime(q, params.KeyStartTime)
	if err != nil {
		return nil, err
	}

	p.EndTime, err = params.GetQueryTime(q, params.KeyEndTime)
	if err != nil {
		return nil, err
	}

	return p, nil
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
	params.ListParams
	ID    *ids.ID
	Query string
	Alias string
}

func ListAssetsParamsForHTTPRequest(r *http.Request) (*ListAssetsParams, error) {
	q := r.URL.Query()

	listParams, err := params.ListParamsForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	p := &ListAssetsParams{
		ListParams: listParams,
	}

	p.ID, err = params.GetQueryID(q, params.KeyID)
	if err != nil {
		return nil, err
	}

	return p, nil
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
	params.ListParams
	Address *ids.ShortID
	Query   string
}

func ListAddressesParamsForHTTPRequest(r *http.Request) (*ListAddressesParams, error) {
	q := r.URL.Query()

	listParams, err := params.ListParamsForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	p := &ListAddressesParams{
		ListParams: listParams,
	}

	p.Address, err = params.GetQueryShortID(q, params.KeyAddress)
	if err != nil {
		return nil, err
	}

	return p, nil
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
	params.ListParams
	ID    *ids.ID
	Query string

	Addresses []ids.ShortID
	Spent     *bool
}

func ListOutputsParamsForHTTPRequest(r *http.Request) (*ListOutputsParams, error) {
	q := r.URL.Query()

	listParams, err := params.ListParamsForHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	var b *bool
	p := &ListOutputsParams{
		Spent:      b,
		ListParams: listParams,
	}

	spentStrs, ok := q[params.KeySpent]
	if ok || len(spentStrs) >= 1 {
		b, err := strconv.ParseBool(spentStrs[0])
		if err != nil {
			return nil, err
		}
		p.Spent = &b
	}

	return p, nil
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
	return TransactionSortDefault, params.ErrUndefinedSort
}
