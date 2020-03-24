// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"errors"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/record"
)

var (
	// defaultKafkaReadTimeout is the amount of time to wait for a Kafka message
	// before hanging up
	defaultKafkaReadTimeout = 1 * time.Minute

	// ErrNotIndexable is returned when trying to get turn a snowstorm tx into an
	// Indexable object
	ErrNotIndexable = errors.New("object is not indexable")
)

// Consumer is a basic interface for consumers
type Consumer interface {
	Initialize(logging.Logger, *cfg.ClientConfig) error
	ProcessNextMessage() error
	Close() error
}

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

// readNextTxBytes gets the next tx from the Kafka consumer and returns its id
// and bytes, or an error
func readNextTxBytes(c *kafka.Consumer) (ids.ID, []byte, error) {
	msg, err := c.ReadMessage(defaultKafkaReadTimeout)
	if err != nil {
		return ids.Empty, nil, err
	}

	msgID, err := ids.ToID(msg.Key)
	if err != nil {
		return ids.Empty, nil, err
	}

	bytes, err := record.Unmarshal(msg.Value)
	if err != nil {
		return ids.Empty, nil, err
	}

	return msgID, bytes, nil
}
