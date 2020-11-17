// See the file LICENSE for licensing terms.

package params

import (
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
	ListParams ListParams
}

func (p *SearchParams) ForValues(v uint8, q url.Values) error {
	return p.ListParams.ForValues(v, q)
}

func (p *SearchParams) CacheKey() []string {
	return p.ListParams.CacheKey()
}

type AggregateParams struct {
	ListParams ListParams

	ChainIDs     []string
	AssetID      *ids.ID
	IntervalSize time.Duration
	Version      int
}

func (p *AggregateParams) ForValues(version uint8, q url.Values) (err error) {
	err = p.ListParams.ForValues(version, q)
	if err != nil {
		return err
	}

	p.ChainIDs = q[KeyChainID]

	p.AssetID, err = GetQueryID(q, KeyAssetID)
	if err != nil {
		return err
	}

	p.IntervalSize, err = GetQueryInterval(q, KeyIntervalSize)
	if err != nil {
		return err
	}

	p.Version, err = GetQueryInt(q, KeyVersion, VersionDefault)
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
		CacheKey(KeyIntervalSize, int64(p.IntervalSize.Seconds())),
		CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")),
		CacheKey(KeyVersion, int64(p.Version)),
	)

	return append(p.ListParams.CacheKey(), k...)
}

func (p *AggregateParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	b.Where("avm_outputs.created_at >= ?", p.ListParams.StartTime)
	b.Where("avm_outputs.created_at < ?", p.ListParams.EndTime)

	if p.AssetID != nil {
		b.Where("avm_outputs.asset_id = ?", p.AssetID.String())
	}

	if len(p.ChainIDs) > 0 {
		b.Where("avm_outputs.chain_id = ?", p.ChainIDs)
	}

	return b
}

type ListTransactionsParams struct {
	ListParams     ListParams
	ChainIDs       []string
	Addresses      []ids.ShortID
	AssetID        *ids.ID
	DisableGenesis bool
	Sort           TransactionSort
}

func (p *ListTransactionsParams) ForValues(v uint8, q url.Values) error {
	err := p.ListParams.ForValues(v, q)
	if err != nil {
		return err
	}

	p.Sort = TransactionSortDefault
	sortBys, ok := q[KeySortBy]
	if ok && len(sortBys) >= 1 {
		p.Sort, _ = toTransactionSort(sortBys[0])
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

	p.DisableGenesis, err = GetQueryBool(q, KeyDisableGenesis, false)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListTransactionsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()
	k = append(k, CacheKey(KeySortBy, p.Sort))

	if p.AssetID != nil {
		k = append(k, CacheKey(KeyAssetID, p.AssetID.String()))
	}

	for _, address := range p.Addresses {
		k = append(k, CacheKey(KeyAddress, address.String()))
	}

	k = append(k,
		CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")),
		CacheKey(KeyDisableGenesis, p.DisableGenesis),
	)

	return k
}

func (p *ListTransactionsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply("avm_transactions", b)

	if len(p.ChainIDs) > 0 {
		b.Where("avm_transactions.chain_id = ?", p.ChainIDs)
	}

	var dosq bool
	var dosqRedeem bool
	subquery := dbr.Select("avm_outputs.transaction_id").
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id")
	subqueryRedeem := dbr.Select("avm_outputs_redeeming.redeeming_transaction_id").
		From("avm_outputs_redeeming").
		LeftJoin("avm_output_addresses", "avm_outputs_redeeming.id = avm_output_addresses.output_id").
		Where("avm_outputs_redeeming.redeeming_transaction_id is not null")

	if len(p.Addresses) > 0 {
		dosq = true
		dosqRedeem = true
		addrs := make([]string, len(p.Addresses))
		for i, id := range p.Addresses {
			addrs[i] = id.String()
		}
		subquery = subquery.Where("avm_output_addresses.address IN ?", addrs)
		subqueryRedeem = subqueryRedeem.Where("avm_output_addresses.address IN ?", addrs)
	}

	if p.AssetID != nil {
		dosq = true
		subquery = subquery.Where("avm_outputs.asset_id = ?", p.AssetID.String())
	}

	if dosq && dosqRedeem {
		b.Where("avm_transactions.id in ? or avm_transactions.id in ?", subquery, subqueryRedeem)
	} else if dosq {
		b.Where("avm_transactions.id in ?", subquery)
	}

	return b
}

type ListAssetsParams struct {
	ListParams      ListParams
	Alias           string
	EnableAggregate bool
	PathParamID     string
}

func (p *ListAssetsParams) ForValues(v uint8, q url.Values) error {
	if err := p.ListParams.ForValues(v, q); err != nil {
		return err
	}

	p.Alias = GetQueryString(q, KeyAlias, "")

	var err error
	p.EnableAggregate, err = GetQueryBool(q, KeyEnableAggregate, false)
	if err != nil {
		return err
	}

	return nil
}

func (p *ListAssetsParams) CacheKey() []string {
	return append(p.ListParams.CacheKey(),
		CacheKey(KeyEnableAggregate, p.EnableAggregate),
		CacheKey(KeyAlias, p.Alias),
		CacheKey("PathParamID", p.PathParamID))
}

func (p *ListAssetsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply("avm_assets", b)

	if p.Alias != "" {
		b.Where("avm_assets.alias = ?", p.Alias)
	}

	return b
}

type ListAddressesParams struct {
	ListParams ListParams
	ChainIDs   []string
	Address    *ids.ShortID
	Version    int
}

func (p *ListAddressesParams) ForValues(v uint8, q url.Values) error {
	if err := p.ListParams.ForValues(v, q); err != nil {
		return err
	}

	p.ChainIDs = q[KeyChainID]

	var err error
	if p.Address, err = GetQueryAddress(q, KeyAddress); err != nil {
		return err
	}

	if p.Address == nil && p.ListParams.Query != "" {
		addr, err := AddressFromString(p.ListParams.Query)
		if err != nil {
			return err
		}
		p.Address = &addr
	}

	if p.Version, err = GetQueryInt(q, KeyVersion, VersionDefault); err != nil {
		return err
	}

	return nil
}

func (p *ListAddressesParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.Address != nil {
		k = append(k, CacheKey(KeyAddress, p.Address.String()))
	}

	k = append(k, CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")))

	k = append(k, CacheKey(KeyVersion, int64(p.Version)))

	return k
}

func (p *ListAddressesParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	b = p.ListParams.Apply("avm_output_addresses", b)

	if p.Address != nil {
		b = b.
			Where("avm_output_addresses.address = ?", p.Address.String()).
			Limit(1)
	}

	return b
}

type AddressChainsParams struct {
	ListParams ListParams
	Addresses  []ids.ShortID
}

func (p *AddressChainsParams) ForValues(v uint8, q url.Values) error {
	if err := p.ListParams.ForValues(v, q); err != nil {
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

	return nil
}

func (p *AddressChainsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	for _, address := range p.Addresses {
		k = append(k, CacheKey(KeyAddress, address.String()))
	}

	return k
}

func (p *AddressChainsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply("address_chain", b)

	if len(p.Addresses) == 0 {
		return b
	}

	addrs := make([]string, len(p.Addresses))
	for i, id := range p.Addresses {
		addrs[i] = id.String()
	}
	return b.Where("address_chain.address IN ?", addrs)
}

type ListOutputsParams struct {
	ListParams ListParams
	ChainIDs   []string
	Addresses  []ids.ShortID
	Spent      *bool
}

func (p *ListOutputsParams) ForValues(v uint8, q url.Values) error {
	if err := p.ListParams.ForValues(v, q); err != nil {
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
	k := append(p.ListParams.CacheKey(),
		CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")))

	for _, address := range p.Addresses {
		k = append(k, CacheKey(KeyAddress, address.String()))
	}

	if p.Spent != nil {
		k = append(k, CacheKey(KeySpent, *p.Spent))
	}

	return k
}

func (p *ListOutputsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.Apply("avm_outputs", b)

	if p.Spent != nil {
		if *p.Spent {
			b.Where("avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL")
		} else {
			b.Where("avm_outputs_redeeming.redeeming_transaction_id IS NULL")
		}
	}

	if p.Addresses != nil {
		addrStrs := make([]string, len(p.Addresses))
		for i, addr := range p.Addresses {
			addrStrs[i] = addr.String()
		}
		b.LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_output_addresses.address IN ?", addrStrs)
	}

	if len(p.ChainIDs) > 0 {
		b.Where("avm_outputs.chain_id = ?", p.ChainIDs)
	}

	return b
}

type ListBlocksParams struct {
	ListParams ListParams
	Types      []models.BlockType
	Sort       BlockSort
}

func (p *ListBlocksParams) ForValues(v uint8, q url.Values) error {
	return p.ListParams.ForValues(v, q)
}

func (p *ListBlocksParams) CacheKey() []string {
	return p.ListParams.CacheKey()
}

func (p *ListBlocksParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	return p.ListParams.Apply("pvm_blocks", b)
}

func ForValueChainID(chainID *ids.ID, chainIDs []string) []string {
	if chainID == nil {
		return chainIDs
	}

	if chainIDs == nil {
		chainIDs = make([]string, 0, 1)
	}

	cnew := chainID.String()

	var found bool
	for _, cval := range chainIDs {
		if cval == cnew {
			found = true
			break
		}
	}
	if found {
		return chainIDs
	}
	chainIDs = append(chainIDs, cnew)
	return chainIDs
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
