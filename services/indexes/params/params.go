// (c) 2020, Ava Labs, Inc. All rights reserved.
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
	KeyID              = "id"
	KeyChainID         = "chainID"
	KeyAddress         = "address"
	KeyAlias           = "alias"
	KeyAssetID         = "assetID"
	KeySearchQuery     = "query"
	KeySortBy          = "sort"
	KeyLimit           = "limit"
	KeyOffset          = "offset"
	KeySpent           = "spent"
	KeyStartTime       = "startTime"
	KeyEndTime         = "endTime"
	KeyIntervalSize    = "intervalSize"
	KeyDisableCount    = "disableCount"
	KeyDisableGenesis  = "disableGenesis"
	KeyVersion         = "version"
	KeyEnableAggregate = "enableAggregate"

	PaginationMaxLimit      = 500
	PaginationDefaultLimit  = 500
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
	Limit  int
	Offset int

	ID        *ids.ID
	Query     string
	StartTime time.Time
	EndTime   time.Time

	DisableCounting bool
}

func (p *ListParams) ForValues(version uint8, q url.Values) (err error) {
	p.Limit, err = GetQueryInt(q, KeyLimit, PaginationDefaultLimit)
	if err != nil {
		return err
	}
	p.Offset, err = GetQueryInt(q, KeyOffset, PaginationDefaultOffset)
	if err != nil {
		return err
	}

	p.ID, err = GetQueryID(q, KeyID)
	if err != nil {
		return err
	}

	p.StartTime, err = GetQueryTime(q, KeyStartTime)
	if err != nil {
		return err
	}
	p.StartTime = p.StartTime.Round(TransactionRoundDuration)

	p.EndTime, err = GetQueryTime(q, KeyEndTime)
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
	if p.Limit > PaginationMaxLimit {
		p.Limit = PaginationMaxLimit
	}
	if p.Limit != 0 {
		b.Limit(uint64(p.Limit))
	}
	if p.Offset != 0 {
		b.Offset(uint64(p.Offset))
	}

	if p.ID != nil {
		b.Where(listTable+".id = ?", p.ID.String()).Limit(1)
	}

	if p.Query != "" {
		b.Where(dbr.Like(listTable+".id", p.Query+"%"))
	}

	return b
}
