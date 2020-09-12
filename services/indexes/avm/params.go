// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/ortelius/services/indexes/params"
)

const (
	TransactionSortDefault       TransactionSort = TransactionSortTimestampAsc
	TransactionSortTimestampAsc                  = "timestamp-asc"
	TransactionSortTimestampDesc                 = "timestamp-desc"
)

var (
	_ params.Param = &SearchParams{}
	_ params.Param = &AggregateParams{}
	_ params.Param = &ListTransactionsParams{}
	_ params.Param = &ListAssetsParams{}
	_ params.Param = &ListAddressesParams{}
	_ params.Param = &ListOutputsParams{}
)

type SearchParams struct {
	params.ListParams
	Query string
}

func (p *SearchParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	queryStrs, ok := q[params.KeySearchQuery]
	if ok || len(queryStrs) >= 1 {
		p.Query = queryStrs[0]
	} else {
		return errors.New("query required")
	}

	return nil
}

func (p *SearchParams) CacheKey() []string {
	return append(p.ListParams.CacheKey(), params.CacheKey(params.KeySearchQuery, p.Query))
}

type AggregateParams struct {
	AssetID      *ids.ID
	StartTime    time.Time
	EndTime      time.Time
	IntervalSize time.Duration
}

func (p *AggregateParams) ForValues(q url.Values) (err error) {
	p.AssetID, err = params.GetQueryID(q, params.KeyAssetID)
	if err != nil {
		return err
	}

	p.StartTime, err = params.GetQueryTime(q, params.KeyStartTime)
	if err != nil {
		return err
	}

	p.EndTime, err = params.GetQueryTime(q, params.KeyEndTime)
	if err != nil {
		return err
	}

	p.IntervalSize, err = params.GetQueryInterval(q, params.KeyIntervalSize)
	if err != nil {
		return err
	}

	return nil
}

func (p *AggregateParams) CacheKey() []string {
	k := make([]string, 0, 4)

	if p.AssetID != nil {
		k = append(k, params.CacheKey(params.KeyAssetID, p.AssetID.String()))
	}

	k = append(k,
		params.CacheKey(params.KeyStartTime, params.RoundTime(p.StartTime, time.Hour).Unix()),
		params.CacheKey(params.KeyEndTime, params.RoundTime(p.EndTime, time.Hour).Unix()),
		params.CacheKey(params.KeyIntervalSize, int64(p.IntervalSize.Seconds())),
	)

	return k
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

func (p *ListTransactionsParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.Sort = TransactionSortDefault
	sortBys, ok := q[params.KeySortBy]
	if ok && len(sortBys) >= 1 {
		p.Sort, _ = toTransactionSort(sortBys[0])
	}

	p.ID, err = params.GetQueryID(q, params.KeyID)
	if err != nil {
		return err
	}

	p.AssetID, err = params.GetQueryID(q, params.KeyAssetID)
	if err != nil {
		return err
	}

	addressStrs := q[params.KeyAddress]
	for _, addressStr := range addressStrs {
		addr, err := params.AddressFromString(addressStr)
		if err != nil {
			return err
		}
		p.Addresses = append(p.Addresses, addr)
	}

	p.StartTime, err = params.GetQueryTime(q, params.KeyStartTime)
	if err != nil {
		return err
	}

	p.EndTime, err = params.GetQueryTime(q, params.KeyEndTime)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListTransactionsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()
	k = append(k, params.CacheKey(params.KeySortBy, p.Sort))

	if p.ID != nil {
		k = append(k, params.CacheKey(params.KeyID, p.ID.String()))
	}

	if p.AssetID != nil {
		k = append(k, params.CacheKey(params.KeyAssetID, p.AssetID.String()))
	}

	for _, address := range p.Addresses {
		k = append(k, params.CacheKey(params.KeyAddress, address.String()))
	}

	k = append(k,
		params.CacheKey(params.KeyStartTime, params.RoundTime(p.StartTime, time.Hour).Unix()),
		params.CacheKey(params.KeyEndTime, params.RoundTime(p.EndTime, time.Hour).Unix()),
	)

	return k
}

func (p *ListTransactionsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply(b)

	if p.ID != nil {
		b = b.
			Where("avm_transactions.id = ?", p.ID.String()).
			Limit(1)
	}

	needsDistinct := false

	needOutputsJoin := len(p.Addresses) > 0 || p.AssetID != nil
	if needOutputsJoin {
		needsDistinct = true
		b = b.LeftJoin("avm_outputs", "avm_outputs.transaction_id = avm_transactions.id")
	}

	if len(p.Addresses) > 0 {
		addrs := make([]string, len(p.Addresses))
		for i, id := range p.Addresses {
			addrs[i] = id.String()
		}
		needsDistinct = true
		b = b.LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_output_addresses.address IN ?", addrs)
	}
	if needsDistinct {
		b = b.Distinct()
	}

	if p.AssetID != nil {
		b = b.Where("avm_outputs.asset_id = ?", p.AssetID.String())
	}

	if !p.StartTime.IsZero() {
		b = b.Where("avm_transactions.created_at >= ?", p.StartTime)
	}
	if !p.EndTime.IsZero() {
		b = b.Where("avm_transactions.created_at <= ?", p.EndTime)
	}

	if p.Query != "" {
		b.Where(dbr.Like("avm_transactions.id", p.Query+"%"))
	}

	return b
}

type ListAssetsParams struct {
	params.ListParams
	ID    *ids.ID
	Query string
	Alias string
}

func (p *ListAssetsParams) ForValue(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.ID, err = params.GetQueryID(q, params.KeyID)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListAssetsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.ID != nil {
		k = append(k, params.CacheKey(params.KeyID, p.ID.String()))
	}

	return k
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

	if p.Query != "" {
		b.Where(dbr.Or(
			dbr.Like("avm_assets.id", p.Query+"%"),
			dbr.Like("avm_assets.name", p.Query+"%"),
			dbr.Like("avm_assets.symbol", p.Query+"%"),
		))
	}

	return b
}

type ListAddressesParams struct {
	params.ListParams
	Address *ids.ShortID
	Query   string
}

func (p *ListAddressesParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.Address, err = params.GetQueryAddress(q, params.KeyAddress)
	if err != nil {
		return err
	}

	if p.Address == nil && p.Query != "" {
		addr, err := params.AddressFromString(p.Query)
		if err != nil {
			return err
		}
		p.Address = &addr
	}

	return nil
}

func (p *ListAddressesParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.Address != nil {
		k = append(k, params.CacheKey(params.KeyAddress, p.Address.String()))
	}

	return k
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
	ID        *ids.ID
	Addresses []ids.ShortID
	Spent     *bool
	Query     string
}

func (p *ListOutputsParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.ID, err = params.GetQueryID(q, params.KeyID)
	if err != nil {
		return err
	}

	addressStrs := q[params.KeyAddress]
	for _, addressStr := range addressStrs {
		addr, err := params.AddressFromString(addressStr)
		if err != nil {
			return err
		}
		p.Addresses = append(p.Addresses, addr)
	}

	spentStrs, ok := q[params.KeySpent]
	if ok || len(spentStrs) >= 1 {
		b, err := strconv.ParseBool(spentStrs[0])
		if err != nil {
			return err
		}
		p.Spent = &b
	}

	return nil
}

func (p *ListOutputsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.ID != nil {
		k = append(k, params.CacheKey(params.KeyID, p.ID.String()))
	}

	for _, address := range p.Addresses {
		k = append(k, params.CacheKey(params.KeyAddress, address.String()))
	}

	if p.Spent != nil {
		k = append(k, params.CacheKey(params.KeySpent, *p.Spent))
	}

	if p.Query != "" {
		k = append(k, params.CacheKey(params.KeySearchQuery, p.Query))
	}

	return k
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
			Where("avm_outputs.id = ?", p.ID.String()).
			Limit(1)
	}

	if p.Query != "" {
		b.Where(dbr.Like("avm_outputs.id", p.Query+"%"))
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
