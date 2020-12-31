package cfg

import "time"

const (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout = 20 * time.Second
	// AggregateRequestTimeout is the maximum duration to allow an Aggregate API request to execute
	AggregateRequestTimeout = 20 * time.Second
	HTTPWriteTimeout        = 30 * time.Second
	CacheTimeout            = 3 * time.Second

	// DefaultConsumeProcessWriteTimeout consume context
	DefaultConsumeProcessWriteTimeout = 10 * time.Second
)

// PerformUpdates controls for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
