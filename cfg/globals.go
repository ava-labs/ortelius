package cfg

import "time"

const (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout   = 5 * time.Second
	HTTPWriteTimeout = 30 * time.Second
	CacheTimeout     = 10 * time.Second
)

// PerformUpdates controls for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
