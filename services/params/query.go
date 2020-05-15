// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"net/url"
	"strconv"
	"time"

	"github.com/ava-labs/gecko/ids"
)

func GetQueryInt(q url.Values, key string, defaultVal int) (val int, err error) {
	strs := q[key]
	if len(strs) >= 1 {
		return strconv.Atoi(strs[0])
	}
	return defaultVal, err
}

func GetQueryString(q url.Values, key string, defaultVal string) string {
	strs := q[key]
	if len(strs) >= 1 {
		return strs[0]
	}
	return defaultVal
}

func GetQueryTime(q url.Values, key string) (time.Time, error) {
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

func GetQueryID(q url.Values, key string) (*ids.ID, error) {
	idStr := GetQueryString(q, key, "")
	if idStr == "" {
		return nil, nil
	}

	id, err := ids.FromString(idStr)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func GetQueryShortID(q url.Values, key string) (*ids.ShortID, error) {
	idStr := GetQueryString(q, key, "")
	if idStr == "" {
		return nil, nil
	}

	id, err := ids.ShortFromString(idStr)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func GetQueryInterval(q url.Values, key string) (time.Duration, error) {
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
