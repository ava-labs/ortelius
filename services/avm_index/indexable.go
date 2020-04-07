// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"errors"

	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
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

// toIndexable converts a snowtorm.Tx into an Indexable or returns an error if
// the Tx is not an Indexable
func toIndexable(tx snowstorm.Tx) (Indexable, error) {
	indexable, ok := tx.(Indexable)
	if !ok {
		return nil, ErrNotIndexable
	}
	return indexable, nil
}

// toIndexableBytes gets the indexable bytes for the given snowstorm.Tx
func toIndexableBytes(tx snowstorm.Tx) ([]byte, error) {
	indexable, err := toIndexable(tx)
	if err != nil {
		return nil, err
	}
	return indexable.IndexableBytes()
}
