// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/stream/record"
)

type serviceConsumerFactory func(cfg.Config, uint32, string, string) (services.Consumer, error)

// consumer takes events from Kafka and sends them to a service consumer
type consumer struct {
	reader   *kafka.Reader
	consumer services.Consumer
}

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerFactory(factory serviceConsumerFactory) ProcessorFactory {
	return func(conf cfg.Config, networkID uint32, chainVM string, chainID string) (Processor, error) {
		var (
			err error
			c   = &consumer{}
		)

		// Create consumer backend
		c.consumer, err = factory(conf, networkID, chainVM, chainID)
		if err != nil {
			return nil, err
		}

		// Bootstrap our service
		if err = c.consumer.Bootstrap(); err != nil {
			return nil, err
		}

		// Setup config
		groupName := conf.Consumer.GroupName
		if groupName == "" {
			groupName = c.consumer.Name()
		}
		if !conf.Consumer.StartTime.IsZero() {
			groupName = ""
		}

		// Create reader for the topic
		c.reader = kafka.NewReader(kafka.ReaderConfig{
			Topic:    chainID,
			Brokers:  conf.Kafka.Brokers,
			GroupID:  groupName,
			MaxBytes: 10e6,
		})

		// If the start time is set then seek to the correct offset
		if !conf.Consumer.StartTime.IsZero() {
			ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(readTimeout))
			defer cancelFn()

			if err = c.reader.SetOffsetAt(ctx, conf.Consumer.StartTime); err != nil {
				return nil, err
			}
		}

		return c, nil
	}
}

// Close closes the consumer
func (c *consumer) Close() error {
	return c.reader.Close()
}

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumer) ProcessNextMessage(ctx context.Context) error {
	msg, err := getNextMessage(ctx, c.reader)
	if err != nil {
		return err
	}
	return c.consumer.Consume(msg)
}

// getNextMessage gets the next Message from the Kafka Indexer
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
		id:        id.String(),
		chainID:   chainID.String(),
		body:      body,
		timestamp: msg.Time.UTC().Unix(),
	}, nil
}
