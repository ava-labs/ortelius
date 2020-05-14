// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"

	"github.com/ava-labs/gecko/ids"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/avm_index"
	"github.com/ava-labs/ortelius/stream/record"
)

// consumer takes events from Kafka and sends them to a service
type consumer struct {
	reader  *kafka.Reader
	service services.FanOutService
}

// NewConsumer creates a consumer for the given config
func NewConsumer(conf cfg.ClientConfig, networkID uint32, chainID ids.ID) (Processor, error) {
	var (
		err error
		c   = &consumer{}
	)

	// Create service backend
	c.service, err = createServices(conf.ServiceConfig, networkID, chainID)
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

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumer) ProcessNextMessage(ctx context.Context) (*Message, error) {
	msg, err := getNextMessage(ctx, c.reader)
	if err != nil {
		return nil, err
	}
	return msg, c.service.Add(msg)
}

// getNextMessage gets the next Message from the Kafka consumer
func getNextMessage(ctx context.Context, r *kafka.Reader) (*Message, error) {
	// Get raw Message from Kafka
	msg, err := r.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	// Extract chainID from topic
	chainID, err := ids.FromString(msg.Topic)
	if err != nil {
		return nil, err
	}

	// Extract Message ID from key
	id, err := ids.ToID(msg.Key)
	if err != nil {
		return nil, err
	}

	// Extract tx body from value
	body, err := record.Unmarshal(msg.Value)
	if err != nil {
		return nil, err
	}

	return &Message{
		id:        id,
		chainID:   chainID,
		body:      body,
		timestamp: uint64(msg.Time.UTC().Unix()),
	}, nil
}

func createServices(conf cfg.ServiceConfig, networkID uint32, chainID ids.ID) (services.FanOutService, error) {
	// Create and bootstrap an AVMIndex
	avmIndex, err := avm_index.New(conf, networkID, chainID)
	if err != nil {
		return nil, err
	}

	err = avmIndex.Bootstrap()
	if err != nil {
		return nil, err
	}

	return services.FanOutService{avmIndex}, nil
}
