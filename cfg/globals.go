package cfg

import "time"

// RequestTimeout is the maximum duration to allow an API request to execute
const (
	RequestTimeout = 2 * time.Minute
)

// Control for performing sql update operations.  Disabled by normal operation.
var PerformUpdates = false
