// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/gocraft/dbr/v2"
)

const (
	KeyID              = "id"
	KeyChainID         = "chainID"
	KeyAddress         = "address"
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
	VersionDefault          = 0
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
	ForValues(url.Values) error
	CacheKey() []string
}

func CacheKey(name string, val interface{}) string {
	return fmt.Sprintf("%s=%v", name, val)
}

//
// Global params
//
type ListParams struct {
	Limit           int
	Offset          int
	DisableCounting bool
}

func (p *ListParams) ForValues(q url.Values) (err error) {
	p.Limit, err = GetQueryInt(q, KeyLimit, PaginationDefaultLimit)
	if err != nil {
		return err
	}
	p.Offset, err = GetQueryInt(q, KeyOffset, PaginationDefaultOffset)
	if err != nil {
		return err
	}
	p.DisableCounting, err = GetQueryBool(q, KeyDisableCount, false)
	if err != nil {
		return err
	}
	return nil
}

func (p *ListParams) CacheKey() []string {
	return []string{
		CacheKey(KeyLimit, p.Limit),
		CacheKey(KeyOffset, p.Offset),

		// inject the DisableCount to the key..  Makes sure cache hits will return answer matching request
		CacheKey(KeyDisableCount, p.DisableCounting),
	}
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
