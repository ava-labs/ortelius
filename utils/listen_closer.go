// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

// ListenCloser listens for messages until it's asked to close
type ListenCloser interface {
	Listen() error
	Close() error
}
