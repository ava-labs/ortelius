// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"errors"
)

var (
	ErrNotIndexable = errors.New("object is not indexable")
)

// Indexable is the basic interface for objects opting in to being indexed
type Indexable interface {
	// IndexableBytes returns the data to be indexed for the object serialized to
	// binary
	IndexableBytes() ([]byte, error)
}
