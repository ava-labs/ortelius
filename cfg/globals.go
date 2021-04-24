package cfg

import "time"

const (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout   = 10 * time.Second
	HTTPWriteTimeout = 30 * time.Second
	CacheTimeout     = 3 * time.Second

	DefaultConsumeProcessWriteTimeout = 5 * time.Minute

	RequestGetMaxSize = int64(10 * 1024 * 1024)

	ConsumerMaxBytesDefault = 10e8

	MaxSizedList = 5000
)

// PerformUpdates controls for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
