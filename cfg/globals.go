package cfg

import "time"

const (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout   = 10 * time.Second
	HTTPWriteTimeout = 20 * time.Second
	CacheTimeout     = 3 * time.Second

	// DefaultConsumeProcessWriteTimeout consume context
	DefaultConsumeProcessWriteTimeout = 10 * time.Second

	DatabaseRetries = 60

	RequestGetMaxSize = int64(10 * 1024 * 1024)
)

// PerformUpdates controls for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
