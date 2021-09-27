// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/gocraft/dbr/v2"
)

const (
	KeyID               = "id"
	KeyChainID          = "chainID"
	KeyAddress          = "address"
	KeyToAddress        = "toAddress"
	KeyFromAddress      = "fromAddress"
	KeyBlockStart       = "blockStart"
	KeyBlockEnd         = "blockEnd"
	KeyHash             = "hash"
	KeyAlias            = "alias"
	KeyAssetID          = "assetID"
	KeySearchQuery      = "query"
	KeySortBy           = "sort"
	KeyLimit            = "limit"
	KeyOffset           = "offset"
	KeySpent            = "spent"
	KeyStartTime        = "startTime"
	KeyEndTime          = "endTime"
	KeyIntervalSize     = "intervalSize"
	KeyDisableCount     = "disableCount"
	KeyDisableGenesis   = "disableGenesis"
	KeyOutputOutputType = "outputOutputType"
	KeyOutputGroupID    = "outputGroupId"

	PaginationMaxLimit      = 5000
	PaginationDefaultOffset = 0

	VersionDefault = 0
)

var (
	IntervalMinute = 1 * time.Minute
	IntervalHour   = 60 * time.Minute
	IntervalDay    = 1440 * time.Minute
	IntervalWeek   = 7 * IntervalDay
	IntervalMonth  = 30 * IntervalDay
	IntervalYear   = 365 * IntervalDay
	IntervalAll    = time.Duration(0)
	IntervalNames  = map[string]time.Duration{
		"minute": IntervalMinute,
		"hour":   IntervalHour,
		"day":    IntervalDay,
		"week":   IntervalWeek,
		"month":  IntervalMonth,
		"year":   IntervalYear,
		"all":    IntervalAll,
	}

	ErrUndefinedSort = errors.New("undefined sort")

	// Ensure params types satisfy the interface
	_ Param = &ListParams{}

	TransactionRoundDuration = time.Second
)

type Param interface {
	ForValues(uint8, url.Values) error
	CacheKey() []string
}

func CacheKey(name string, val interface{}) string {
	return fmt.Sprintf("%s=%v", name, val)
}

//
// Global params
//
type ListParams struct {
	Values url.Values
	ID     *ids.ID
	Query  string

	Limit           int
	Offset          int
	DisableCounting bool

	StartTimeProvided bool
	EndTimeProvided   bool

	StartTime time.Time
	EndTime   time.Time
}

func (p *ListParams) ForValues(version uint8, q url.Values) (err error) {
	return p.forValues(version, q, false)
}
func (p *ListParams) ForValuesAllowOffset(version uint8, q url.Values) (err error) {
	return p.forValues(version, q, true)
}

func (p *ListParams) forValues(version uint8, q url.Values, allowOffset bool) (err error) {
	p.Values = q
	p.Limit, err = GetQueryInt(q, KeyLimit, PaginationMaxLimit)
	if err != nil {
		return err
	}
	if p.Limit < 0 {
		p.Limit = 0
	}
	if p.Limit > PaginationMaxLimit {
		p.Limit = PaginationMaxLimit
	}
	p.Offset, err = GetQueryInt(q, KeyOffset, PaginationDefaultOffset)
	if err != nil {
		return err
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
	if !allowOffset && p.Offset > 0 {
		return errors.New("offset deprecated")
	}

	p.ID, err = GetQueryID(q, KeyID)
	if err != nil {
		return err
	}

	p.StartTimeProvided, p.StartTime, err = GetQueryTime(q, KeyStartTime)
	if err != nil {
		return err
	}
	p.StartTime = p.StartTime.Round(TransactionRoundDuration)

	p.EndTimeProvided, p.EndTime, err = GetQueryTime(q, KeyEndTime)
	if err != nil {
		return err
	}
	if p.EndTime.IsZero() {
		p.EndTime = time.Now().UTC()
	}
	p.EndTime = p.EndTime.Round(TransactionRoundDuration)

	p.Query = GetQueryString(q, KeySearchQuery, "")

	if p.DisableCounting, err = GetQueryBool(q, KeyDisableCount, false); err != nil {
		return err
	}

	// Always disable counting for v2+
	if version >= 2 {
		p.DisableCounting = true
	}

	return nil
}

func (p *ListParams) CacheKey() []string {
	var keys []string
	if p.ID != nil {
		keys = append(keys, CacheKey(KeyID, p.ID.String()))
	}

	return append(keys,
		CacheKey(KeyLimit, p.Limit),
		CacheKey(KeyOffset, p.Offset),

		CacheKey(KeyStartTime, p.StartTime.Unix()),
		CacheKey(KeyEndTime, p.EndTime.Unix()),
		CacheKey(KeySearchQuery, p.Query),
		CacheKey(KeyDisableCount, p.DisableCounting))
}

func (p ListParams) Apply(listTable string, b *dbr.SelectBuilder) *dbr.SelectBuilder {
	return p.ApplyPk(listTable, b, "id", true)
}

func (p ListParams) ApplyPk(listTable string, b *dbr.SelectBuilder, primaryKey string, pk bool) *dbr.SelectBuilder {
	if p.Limit != 0 {
		b.Limit(uint64(p.Limit))
	}
	if p.Offset != 0 {
		b.Offset(uint64(p.Offset))
	}

	if pk {
		if p.ID != nil {
			b.Where(listTable+"."+primaryKey+" = ?", p.ID.String()).Limit(1)
		}

		if p.Query != "" {
			b.Where(dbr.Like(listTable+"."+primaryKey, p.Query+"%"))
		}
	}

	return b
}
