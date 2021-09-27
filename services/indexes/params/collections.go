// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/gocraft/dbr/v2"
)

const (
	TransactionSortTimestampAsc TransactionSort = iota
	TransactionSortTimestampDesc

	TransactionSortDefault = TransactionSortTimestampAsc

	TransactionSortTimestampAscStr  = "timestamp-asc"
	TransactionSortTimestampDescStr = "timestamp-desc"
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

type TxfeeAggregateParams struct {
	ListParams ListParams

	IntervalSize time.Duration

	ChainIDs []string
}

func (p *TxfeeAggregateParams) ForValues(version uint8, q url.Values) (err error) {
	err = p.ListParams.ForValues(version, q)
	if err != nil {
		return err
	}

	p.ChainIDs = q[KeyChainID]

	p.IntervalSize, err = GetQueryInterval(q, KeyIntervalSize)
	if err != nil {
		return err
	}

	return nil
}

func (p *TxfeeAggregateParams) CacheKey() []string {
	k := make([]string, 0, 4)

	k = append(k,
		CacheKey(KeyIntervalSize, int64(p.IntervalSize.Seconds())),
		CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")),
	)

	return append(p.ListParams.CacheKey(), k...)
}

func (p *TxfeeAggregateParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	return b
}

type AggregateParams struct {
	ListParams ListParams

	ChainIDs     []string
	AssetID      *ids.ID
	IntervalSize time.Duration
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
	ListParams ListParams
	ChainIDs   []string
	Addresses  []ids.ShortID
	AssetID    *ids.ID

	OutputOutputTypes []uint64
	OutputGroupIDs    []uint64

	DisableGenesis bool
	Sort           TransactionSort
}

func (p *ListTransactionsParams) ForValues(v uint8, q url.Values) error {
	err := p.ListParams.ForValuesAllowOffset(v, q)
	if err != nil {
		return err
	}

	p.Sort = TransactionSortDefault
	sortBys, ok := q[KeySortBy]
	if ok && len(sortBys) >= 1 {
		p.Sort = toTransactionSort(sortBys[0])
	}

	p.ChainIDs = q[KeyChainID]

	p.AssetID, err = GetQueryID(q, KeyAssetID)
	if err != nil {
		return err
	}

	outputOutputTypes := q[KeyOutputOutputType]
	for _, outputOutputType := range outputOutputTypes {
		outpuOutputTypeValue, err := strconv.ParseUint(outputOutputType, 10, 32)
		if err != nil {
			return err
		}
		p.OutputOutputTypes = append(p.OutputOutputTypes, outpuOutputTypeValue)
	}

	outputGroupIDs := q[KeyOutputGroupID]
	for _, outputGroupID := range outputGroupIDs {
		outputGroupIDValue, err := strconv.ParseUint(outputGroupID, 10, 64)
		if err != nil {
			return err
		}
		p.OutputGroupIDs = append(p.OutputGroupIDs, outputGroupIDValue)
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
	return nil
}

type ListCTransactionsParams struct {
	ListParams     ListParams
	CAddresses     []string
	CAddressesTo   []string
	CAddressesFrom []string
	Hashes         []string
	Sort           TransactionSort
	BlockStart     *big.Int
	BlockEnd       *big.Int
}

func (p *ListCTransactionsParams) ForValues(v uint8, q url.Values) error {
	err := p.ListParams.ForValues(v, q)
	if err != nil {
		return err
	}

	p.Sort = TransactionSortDefault
	sortBys, ok := q[KeySortBy]
	if ok && len(sortBys) >= 1 {
		p.Sort = toTransactionSort(sortBys[0])
	}

	addressStrs := q[KeyAddress]
	for _, addressStr := range addressStrs {
		if !strings.HasPrefix(addressStr, "0x") {
			addressStr = "0x" + addressStr
		}
		p.CAddresses = append(p.CAddresses, strings.ToLower(addressStr))
	}

	addressStrs = q[KeyToAddress]
	for _, addressStr := range addressStrs {
		if !strings.HasPrefix(addressStr, "0x") {
			addressStr = "0x" + addressStr
		}
		p.CAddressesTo = append(p.CAddressesTo, strings.ToLower(addressStr))
	}
	addressStrs = q[KeyFromAddress]
	for _, addressStr := range addressStrs {
		if !strings.HasPrefix(addressStr, "0x") {
			addressStr = "0x" + addressStr
		}
		p.CAddressesFrom = append(p.CAddressesFrom, strings.ToLower(addressStr))
	}

	blockStartStrs := q[KeyBlockStart]
	for _, blockStartStr := range blockStartStrs {
		nint := big.NewInt(0)
		if _, ok := nint.SetString(blockStartStr, 10); ok {
			p.BlockStart = nint
		}
	}
	blockEndStrs := q[KeyBlockEnd]
	for _, blockEndStr := range blockEndStrs {
		nint := big.NewInt(0)
		if _, ok := nint.SetString(blockEndStr, 10); ok {
			p.BlockEnd = nint
		}
	}

	hashStrs := q[KeyHash]
	for _, hashStr := range hashStrs {
		if !strings.HasPrefix(hashStr, "0x") {
			hashStr = "0x" + hashStr
		}
		p.Hashes = append(p.Hashes, hashStr)
	}

	return nil
}

func (p *ListCTransactionsParams) CacheKey() []string {
	k := p.ListParams.CacheKey()
	k = append(k, CacheKey(KeySortBy, p.Sort))

	for _, address := range p.CAddresses {
		k = append(k, CacheKey(KeyAddress, address))
	}

	return k
}

func (p *ListCTransactionsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	p.ListParams.ApplyPk(db.TableCvmTransactionsTxdata, b, "hash", false)

	return b
}

type ListAssetsParams struct {
	ListParams  ListParams
	Alias       string
	PathParamID string
}

func (p *ListAssetsParams) ForValues(v uint8, q url.Values) error {
	if err := p.ListParams.ForValues(v, q); err != nil {
		return err
	}

	p.Alias = GetQueryString(q, KeyAlias, "")

	return nil
}

func (p *ListAssetsParams) CacheKey() []string {
	return append(p.ListParams.CacheKey(),
		CacheKey(KeyAlias, p.Alias),
		CacheKey("PathParamID", p.PathParamID))
}

func (p *ListAssetsParams) Apply(b *dbr.SelectBuilder) *dbr.SelectBuilder {
	// we don't want to apply the query on the primary key.
	// clear out the Query for the Apply call.
	querySave := p.ListParams.Query
	p.ListParams.Query = ""
	p.ListParams.Apply("avm_assets", b)
	p.ListParams.Query = querySave

	if p.ListParams.Query != "" {
		b.Where(dbr.Or(
			dbr.Like("avm_assets.alias", p.ListParams.Query+"%"),
			dbr.Like("avm_assets.name", p.ListParams.Query+"%"),
			dbr.Like("avm_assets.symbol", p.ListParams.Query+"%"),
		))
	}

	if p.Alias != "" {
		b.Where("avm_assets.alias = ?", p.Alias)
	}

	return b
}

type ListAddressesParams struct {
	ListParams ListParams
	ChainIDs   []string
	Address    *ids.ShortID
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

	return nil
}

func (p *ListAddressesParams) CacheKey() []string {
	k := p.ListParams.CacheKey()

	if p.Address != nil {
		k = append(k, CacheKey(KeyAddress, p.Address.String()))
	}

	k = append(k, CacheKey(KeyChainID, strings.Join(p.ChainIDs, "|")))

	return k
}

func (p *ListAddressesParams) Apply(b *dbr.SelectBuilder, accumulateReader bool) *dbr.SelectBuilder {
	if !accumulateReader {
		b = p.ListParams.ApplyPk("avm_output_addresses", b, "output_id", false)
		if len(p.ChainIDs) != 0 {
			b.Where("avm_outputs.chain_id IN ?", p.ChainIDs)
		}

		if p.Address != nil {
			b.Where("avm_output_addresses.address = ?", p.Address.String())
		}
	} else {
		b = p.ListParams.ApplyPk("avm_output_addresses", b, "output_id", true)
		if len(p.ChainIDs) != 0 {
			b.Where("accumulate_balances_received.chain_id IN ?", p.ChainIDs)
		}

		if p.Address != nil {
			b.Where("accumulate_balances_received.address = ?", p.Address.String())
		}
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

	if p.ListParams.StartTimeProvided && !p.ListParams.StartTime.IsZero() {
		b.Where("avm_outputs.created_at >= ?", p.ListParams.StartTime)
	}
	if p.ListParams.EndTimeProvided && !p.ListParams.EndTime.IsZero() {
		b.Where("avm_outputs.created_at < ?", p.ListParams.EndTime)
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
	chainIDs = append([]string{cnew}, chainIDs...)
	return chainIDs
}

//
// Sorting
//
type TransactionSort uint8

func toTransactionSort(s string) TransactionSort {
	switch s {
	case TransactionSortTimestampAscStr:
		return TransactionSortTimestampAsc
	case TransactionSortTimestampDescStr:
		return TransactionSortTimestampDesc
	}
	return TransactionSortDefault
}

func (t TransactionSort) String() string {
	if t == TransactionSortTimestampDesc {
		return TransactionSortTimestampDescStr
	}
	return TransactionSortTimestampAscStr
}

type BlockSort string

type TxDataParam struct {
	ListParams ListParams
	ID         string
}

func (p *TxDataParam) ForValues(v uint8, q url.Values) error {
	if err := p.ListParams.ForValues(v, q); err != nil {
		return err
	}

	return nil
}

func (p *TxDataParam) CacheKey() []string {
	return p.ListParams.CacheKey()
}
