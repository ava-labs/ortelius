// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package params

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/formatting"
)

func GetQueryInt(q url.Values, key string, defaultVal int) (val int, err error) {
	strs := q[key]
	if len(strs) >= 1 {
		return strconv.Atoi(strs[0])
	}
	return defaultVal, err
}

func GetQueryBool(q url.Values, key string, defaultVal bool) (val bool, err error) {
	strs := q[key]
	if len(strs) >= 1 {
		return strconv.ParseBool(strs[0])
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

func GetQueryTime(q url.Values, key string) (bool, time.Time, error) {
	strs, ok := q[key]
	if !ok || len(strs) < 1 {
		return false, time.Time{}, nil
	}

	timestamp, err := strconv.Atoi(strs[0])
	if err == nil {
		return true, time.Unix(int64(timestamp), 0).UTC(), nil
	}

	t, err := time.Parse(time.RFC3339, strs[0])
	if err != nil {
		return false, time.Time{}, err
	}
	return true, t, nil
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

func GetQueryAddress(q url.Values, key string) (*ids.ShortID, error) {
	addrStr := GetQueryString(q, key, "")
	if addrStr == "" {
		return nil, nil
	}

	addr, err := AddressFromString(addrStr)
	if err != nil {
		return nil, err
	}
	return &addr, nil
}

var addressPrefixes = []string{"X", "P", "C"}

func AddressFromString(addrStr string) (ids.ShortID, error) {
	for _, prefix := range addressPrefixes {
		addrStr = strings.TrimPrefix(addrStr, prefix+"-")
		addrStr = strings.TrimPrefix(addrStr, strings.ToLower(prefix)+"-")
	}

	_, addrBytes, err := formatting.ParseBech32(addrStr)
	if err != nil {
		addrFromShortIDStr, err := ids.ShortFromString(addrStr)
		if err == nil {
			return addrFromShortIDStr, nil
		}
		return ids.ShortEmpty, err
	}

	return ids.ToShortID(addrBytes)
}
