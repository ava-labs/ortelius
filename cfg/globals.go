// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import "time"

const (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout   = 15 * time.Second
	HTTPWriteTimeout = 30 * time.Second
	CacheTimeout     = 3 * time.Second

	DefaultConsumeProcessWriteTimeout = 5 * time.Minute

	RequestGetMaxSize = int64(10 * 1024 * 1024)

	MaxSizedList  = 20000
	MaxTxPoolSize = 10000
)

// PerformUpdates controls for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
