// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"errors"
	"time"

	"github.com/ava-labs/gecko/ids"
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
	ErrTopicPtrNil = errors.New("topic pointer is nil")
)

// Consumer is a basic interface for consumers
type Consumer interface {
	Initialize(logging.Logger, *cfg.ClientConfig) error
	ProcessNextMessage() error
	Close() error
}

type message struct {
	id      ids.ID
	chainID ids.ID
	body    []byte
}

func (m *message) ID() ids.ID      { return m.id }
func (m *message) ChainID() ids.ID { return m.chainID }
func (m *message) Body() []byte    { return m.body }

// getNextMessage gets the next message from the Kafka consumer
func getNextMessage(c *kafka.Consumer) (*message, error) {
	// Get raw message from Kafka
	msg, err := c.ReadMessage(defaultKafkaReadTimeout)
	if err != nil {
		return nil, err
	}

	// Extract chainID from topic
	if msg.TopicPartition.Topic == nil {
		return nil, err
	}
	chainID, err := ids.FromString(*msg.TopicPartition.Topic)
	if err != nil {
		return nil, err
	}

	// Extract message ID from key
	id, err := ids.ToID(msg.Key)
	if err != nil {
		return nil, err
	}

	// Extract tx body from value
	body, err := record.Unmarshal(msg.Value)
	if err != nil {
		return nil, err
	}

	return &message{
		id:      id,
		chainID: chainID,
		body:    body,
	}, nil
}
