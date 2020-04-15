// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package client

import (
	"context"
	"errors"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client/record"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/avm_index"
	"github.com/segmentio/kafka-go"
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
	reader  *kafka.Reader
	service services.FanOutService
}

// newConsumer creates an consumer for the given config
func newConsumer(conf *cfg.ClientConfig, chainID ids.ID) (backend, error) {
	var (
		err error
		c   = &consumer{}
	)

	// Create service backend
	c.service, err = createServices(conf.ServiceConfig, chainID)
	if err != nil {
		return nil, err
	}

	// Create reader for the topic
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Topic:    chainID.String(),
		Brokers:  conf.KafkaConfig.Brokers,
		GroupID:  conf.KafkaConfig.GroupName,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return c, nil
}

// Close closes the consumer
func (c *consumer) Close() error {
	return c.reader.Close()
}

// ProcessNextMessage waits for a new message and adds it to the services
func (c *consumer) ProcessNextMessage() (*message, error) {
	msg, err := getNextMessage(c.reader)
	if err != nil {
		return nil, err
	}
	return msg, c.service.Add(msg)
}

// getNextMessage gets the next message from the Kafka consumer
func getNextMessage(r *kafka.Reader) (*message, error) {
	// Get raw message from Kafka
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultKafkaReadTimeout)
	defer cancelFn()

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	// Extract chainID from topic
	chainID, err := ids.FromString(msg.Topic)
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
		timestamp: uint64(msg.Time.UTC().Unix()),
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
