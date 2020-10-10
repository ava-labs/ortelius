// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/ortelius/services/indexes/models"
)

const (
	TransactionSortDefault       TransactionSort = TransactionSortTimestampAsc
	TransactionSortTimestampAsc                  = "timestamp-asc"
	TransactionSortTimestampDesc                 = "timestamp-desc"
)

var (
	_ Param = &SearchParams{}
	_ Param = &AggregateParams{}
	_ Param = &ListTransactionsParams{}
	_ Param = &ListAssetsParams{}
	_ Param = &ListAddressesParams{}
	_ Param = &ListOutputsParams{}
)

type SearchParams struct {
	ListParams
	Query string
}

func (p *SearchParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	queryStrs, ok := q[KeySearchQuery]
	if ok || len(queryStrs) >= 1 {
		p.Query = queryStrs[0]
	} else {
		return errors.New("query required")
	}

	return nil
}

func (p *SearchParams) CacheKey() []string {
	return append(p.ListParams.CacheKey(), CacheKey(KeySearchQuery, p.Query))
}

type AggregateParams struct {
	ChainIDs     []string
	AssetID      *ids.ID
	StartTime    time.Time
	EndTime      time.Time
	IntervalSize time.Duration
	Version      int
}

func (p *AggregateParams) ForValues(q url.Values) (err error) {
	p.ChainIDs = q[KeyChainID]

	p.AssetID, err = GetQueryID(q, KeyAssetID)
	if err != nil {
		return err
	}

	p.StartTime, err = GetQueryTime(q, KeyStartTime)
	if err != nil {
		return err
	}

	p.EndTime, err = GetQueryTime(q, KeyEndTime)
	if err != nil {
		return err
	}

	if p.EndTime.IsZero() {
		p.EndTime = time.Now().UTC()
	}

	p.IntervalSize, err = GetQueryInterval(q, KeyIntervalSize)
	if err != nil {
		return err
	}

	p.Version = GetQueryVersion(q, KeyVersion)
	if err != nil {
		return err
	}

	return nil
}

func (p *AggregateParams) CacheKey() []string {
	k := make([]string, 0, 4)

	if p.AssetID != nil {
		k = append(k, CacheKey(KeyAssetID, p.AssetID.String()))
	}

	k = append(k,
		CacheKey(KeyStartTime, RoundTime(p.StartTime, time.Hour).Unix()),
		CacheKey(KeyEndTime, RoundTime(p.EndTime, time.Hour).Unix()),
		CacheKey(KeyIntervalSize, int64(p.IntervalSize.Seconds())),
		CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")),
		CacheKey(KeyVersion, int64(p.Version)),
	)

	return k
}

func (p *AggregateParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	b.Where("avm_outputs.created_at >= ?", p.StartTime)
	b.Where("avm_outputs.created_at < ?", p.EndTime)

	if p.AssetID != nil {
		b.Where("avm_outputs.asset_id = ?", p.AssetID.String())
	}

	if len(p.ChainIDs) > 0 {
		b.Where("avm_outputs.chain_id = ?", p.ChainIDs)
	}

	return b
}

//
// List route params
//

type ListTransactionsParams struct {
	ListParams

	ID       *ids.ID
	ChainIDs []string

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
	sortBys, ok := q[KeySortBy]
	if ok && len(sortBys) >= 1 {
		p.Sort, _ = toTransactionSort(sortBys[0])
	}

	p.ID, err = GetQueryID(q, KeyID)
	if err != nil {
		return err
	}

	p.ChainIDs = q[KeyChainID]

	p.AssetID, err = GetQueryID(q, KeyAssetID)
	if err != nil {
		return err
	}

	addressStrs := q[KeyAddress]
	for _, addressStr := range addressStrs {
		addr, err := AddressFromString(addressStr)
		if err != nil {
			return err
		}
		p.Addresses = append(p.Addresses, addr)
	}

	p.StartTime, err = GetQueryTime(q, KeyStartTime)
	if err != nil {
		return err
	}

	p.EndTime, err = GetQueryTime(q, KeyEndTime)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListTransactionsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()
	k = append(k, CacheKey(KeySortBy, p.Sort))

	if p.ID != nil {
		k = append(k, CacheKey(KeyID, p.ID.String()))
	}

	if p.AssetID != nil {
		k = append(k, CacheKey(KeyAssetID, p.AssetID.String()))
	}

	for _, address := range p.Addresses {
		k = append(k, CacheKey(KeyAddress, address.String()))
	}

	k = append(k,
		CacheKey(KeyStartTime, RoundTime(p.StartTime, time.Hour).Unix()),
		CacheKey(KeyEndTime, RoundTime(p.EndTime, time.Hour).Unix()),
		CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")),
	)

	return k
}

// true if we will need to left join
func (p *ListTransactionsParams) NeedsDistinct() bool {
	return len(p.Addresses) > 0 || p.AssetID != nil
}

func (p *ListTransactionsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply(b)

	if p.ID != nil {
		b = b.
			Where("avm_transactions.id = ?", p.ID.String()).
			Limit(1)
	}

	needOutputsJoin := len(p.Addresses) > 0 || p.AssetID != nil
	if needOutputsJoin {
		b = b.LeftJoin("avm_outputs", "(avm_outputs.transaction_id = avm_transactions.id OR avm_outputs.redeeming_transaction_id = avm_transactions.id)")
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
		b = b.Where("avm_transactions.created_at >= ?", p.StartTime)
	}
	if !p.EndTime.IsZero() {
		b = b.Where("avm_transactions.created_at <= ?", p.EndTime)
	}

	if p.Query != "" {
		b.Where(dbr.Like("avm_transactions.id", p.Query+"%"))
	}

	if len(p.ChainIDs) > 0 {
		b.Where("avm_transactions.chain_id = ?", p.ChainIDs)
	}

	return b
}

type ListAssetsParams struct {
	ListParams
	ID    *ids.ID
	Query string
	Alias string
}

func (p *ListAssetsParams) ForValue(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.ID, err = GetQueryID(q, KeyID)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListAssetsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.ID != nil {
		k = append(k, CacheKey(KeyID, p.ID.String()))
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
	ListParams
	Address *ids.ShortID
	Query   string
	Version int
}

func (p *ListAddressesParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.Address, err = GetQueryAddress(q, KeyAddress)
	if err != nil {
		return err
	}

	if p.Address == nil && p.Query != "" {
		addr, err := AddressFromString(p.Query)
		if err != nil {
			return err
		}
		p.Address = &addr
	}

	p.Version = GetQueryVersion(q, KeyVersion)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListAddressesParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.Address != nil {
		k = append(k, CacheKey(KeyAddress, p.Address.String()))
	}

	k = append(k, CacheKey(KeyVersion, int64(p.Version)))

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
	ListParams
	ID        *ids.ID
	ChainIDs  []string
	Addresses []ids.ShortID
	Spent     *bool
	Query     string
}

func (p *ListOutputsParams) ForValues(q url.Values) error {
	err := p.ListParams.ForValues(q)
	if err != nil {
		return err
	}

	p.ID, err = GetQueryID(q, KeyID)
	if err != nil {
		return err
	}

	p.ChainIDs = q[KeyChainID]

	addressStrs := q[KeyAddress]
	for _, addressStr := range addressStrs {
		addr, err := AddressFromString(addressStr)
		if err != nil {
			return err
		}
		p.Addresses = append(p.Addresses, addr)
	}

	spentStrs, ok := q[KeySpent]
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

	k = append(k, CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")))

	if p.ID != nil {
		k = append(k, CacheKey(KeyID, p.ID.String()))
	}

	for _, address := range p.Addresses {
		k = append(k, CacheKey(KeyAddress, address.String()))
	}

	if p.Spent != nil {
		k = append(k, CacheKey(KeySpent, *p.Spent))
	}

	if p.Query != "" {
		k = append(k, CacheKey(KeySearchQuery, p.Query))
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

	if len(p.ChainIDs) > 0 {
		b.Where("avm_outputs.chain_id = ?", p.ChainIDs)
	}

	return b
}

type ListBlocksParams struct {
	ListParams

	ID        *ids.ID
	Types     []models.BlockType
	StartTime time.Time
	EndTime   time.Time
	Sort      BlockSort
}

type ListValidatorsParams struct {
	ListParams

	ID           *ids.ID
	Subnets      []ids.ID
	Destinations []ids.ShortID
	StartTime    time.Time
	EndTime      time.Time
}

type ListChainsParams struct {
	ListParams

	ID      *ids.ID
	Subnets []ids.ID
	VMID    *ids.ID
}

type ListSubnetsParams struct {
	ListParams

	ID *ids.ID
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

type BlockSort string
