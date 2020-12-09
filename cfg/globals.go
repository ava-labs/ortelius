package cfg

import "time"

const (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout   = 10 * time.Second
	HTTPWriteTimeout = 20 * time.Second
	CacheTimeout     = 3 * time.Second
)

// PerformUpdates controls for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
