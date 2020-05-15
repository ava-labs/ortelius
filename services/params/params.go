// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"errors"
	"net/http"
	"time"

	"github.com/gocraft/dbr"
)

const (
	KeyID           = "id"
	KeyAddress      = "address"
	KeyAssetID      = "assetID"
	KeySearchQuery  = "query"
	KeySortBy       = "sort"
	KeyLimit        = "limit"
	KeyOffset       = "offset"
	KeySpent        = "spent"
	KeyStartTime    = "startTime"
	KeyEndTime      = "endTime"
	KeyIntervalSize = "intervalSize"

	PaginationMaxLimit      = 500
	PaginationDefaultLimit  = 500
	PaginationDefaultOffset = 0
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
)

type ListParams struct {
	Limit  int
	Offset int
}

func ListParamsForHTTPRequest(r *http.Request) (params ListParams, err error) {
	q := r.URL.Query()
	params.Limit, err = GetQueryInt(q, KeyLimit, PaginationDefaultLimit)
	if err != nil {
		return params, err
	}
	params.Offset, err = GetQueryInt(q, KeyOffset, PaginationDefaultOffset)
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

var (
	ErrUndefinedSort = errors.New("undefined sort")
)
