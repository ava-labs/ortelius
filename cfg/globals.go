package cfg

import "time"

// RequestTimeout is the maximum duration to allow an API request to execute
const (
	RequestTimeout = 2 * time.Minute

	IndexerTaskEnabled = false
)
