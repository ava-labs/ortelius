// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package client

import (
	"errors"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client/record"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/avm_index"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// defaultKafkaReadTimeout is the amount of time to wait for a Kafka message
	// before hanging up
	defaultKafkaReadTimeout = 1 * time.Minute

	// ErrTopicPtrNil is returned when Kafka gives us a nil topic pointer
	ErrTopicPtrNil = errors.New("topic pointer is nil")

	// ErrTimeTooFar is returned when encountering a timestamp too far in the
	// future for us to handle internally.
	ErrTimeTooFar = errors.New("timestamp too far in the future")
)

// consumer takes events from Kafka and sends them to a service
type consumer struct {
	consumer *kafka.Consumer
	service  services.FanOutService
}

// newConsumer creates an consumer for the given config
func newConsumer(conf *cfg.ClientConfig) (*consumer, error) {
	var (
		err error
		c   = &consumer{}
	)

	// Create service backend
	c.service, err = createServices(conf.ServiceConfig, conf.ChainID)
	if err != nil {
		return nil, err
	}

	// Create consumer for the topic
	if c.consumer, err = kafka.NewConsumer(conf.Kafka); err != nil {
		return nil, err
	}
	if err = c.consumer.Subscribe(conf.ChainID.String(), nil); err != nil {
		return nil, err
	}
	return c, nil
}

// Close closes the consumer
func (c *consumer) Close() error {
	return c.consumer.Close()
}

// ProcessNextMessage waits for a new message and adds it to the services
func (c *consumer) ProcessNextMessage() (*message, error) {
	msg, err := getNextMessage(c.consumer)
	if err != nil {
		return nil, err
	}
	return msg, c.service.Add(msg)
}

type message struct {
	id        ids.ID
	chainID   ids.ID
	body      []byte
	timestamp uint64
}

func (m *message) ID() ids.ID        { return m.id }
func (m *message) ChainID() ids.ID   { return m.chainID }
func (m *message) Body() []byte      { return m.body }
func (m *message) Timestamp() uint64 { return m.timestamp }

// getNextMessage gets the next message from the Kafka consumer
func getNextMessage(c *kafka.Consumer) (*message, error) {
	// Get raw message from Kafka
	msg, err := c.ReadMessage(defaultKafkaReadTimeout)
	if err != nil {
		return nil, err
	}

	// Extract chainID from topic
	if msg.TopicPartition.Topic == nil {
		return nil, ErrTopicPtrNil
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
		id:        id,
		chainID:   chainID,
		body:      body,
		timestamp: uint64(msg.Timestamp.UTC().Unix()),
	}, nil
}

func createServices(conf cfg.ServiceConfig, chainID ids.ID) (services.FanOutService, error) {
	// Create and bootstrap an AVMIndex
	avmIndex, err := avm_index.New(conf, chainID)
	if err != nil {
		return nil, err
	}

	err = avmIndex.Bootstrap()
	if err != nil {
		return nil, err
	}

	return services.FanOutService{avmIndex}, nil
}
