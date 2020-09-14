// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/stream/record"
)

type serviceConsumerFactory func(cfg.Config, uint32, string, string) (services.Consumer, error)

// consumer takes events from Kafka and sends them to a service consumer
type consumer struct {
	networkID uint32
	eventType EventType
	reader    *kafka.Reader
	consumer  services.Consumer
	msgchan     chan <- *Message
	waitgroup   sync.WaitGroup
	cancel      context.CancelFunc
	successes   uint64
	log         logging.Logger
}

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerFactory(factory serviceConsumerFactory, eventType EventType) ProcessorFactory {
	return func(conf cfg.Config, networkID uint32, chainVM string, chainID string, log logging.Logger) (Processor, error) {
		msgchan := make(chan *Message)
		ctx2, cancel := context.WithCancel(context.Background())

		var (
			err error
			c   = &consumer{
				networkID: networkID,
				eventType: eventType,
				msgchan:   msgchan,
				waitgroup: sync.WaitGroup{},
				cancel:    cancel,
				log:       log,
			}
		)

		// Create consumer backend
		c.consumer, err = factory(conf, networkID, chainVM, chainID)
		if err != nil {
			return nil, err
		}

		// Bootstrap our service
		ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelFn()
		if err = c.consumer.Bootstrap(ctx); err != nil {
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
			Topic:       GetTopicName(networkID, chainID, eventType),
			Brokers:     conf.Kafka.Brokers,
			GroupID:     groupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    10e6,
		})

		// If the start time is set then seek to the correct offset
		if !conf.Consumer.StartTime.IsZero() {
			ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(readTimeout))
			defer cancelFn()

			if err = c.reader.SetOffsetAt(ctx, conf.Consumer.StartTime); err != nil {
				return nil, err
			}
		}

		for i := 0; i < int(conf.QueueSizeConsumer); i++ {
			c.waitgroup.Add(1)
			go c.ProcessWorker(ctx2, msgchan)
		}

		return c, nil
	}
}

// Close closes the consumer
func (c *consumer) Close() error {
	c.cancel()
	c.waitgroup.Wait()

	ctx, cancelFn := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancelFn()

	errs := wrappers.Errs{}
	errs.Add(c.reader.Close(), c.consumer.Close(ctx))
	return errs.Err
}

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumer) ProcessNextMessage(ctx context.Context) error {
	msg, err := c.getNextMessage(ctx)
	if err != nil {
		c.log.Error("consumer.getNextMessage: %s", err.Error())
		return err
	}

	c.msgchan <- msg

	return nil
}

func (c* consumer) Successes() uint64 {
	return c.successes
}

func (c *consumer) ProcessWorker(ctx context.Context, msgchan <- chan *Message) {
	defer c.waitgroup.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <- msgchan:
			if err := c.consumer.Consume(ctx, msg); err != nil {
				c.log.Error("consumer.Consume: %s", err.Error())
			} else {
				atomic.AddUint64(&c.successes, 1)
			}
		}
	}
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *consumer) getNextMessage(ctx context.Context) (*Message, error) {
	// Get raw Message from Kafka
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	// Extract chainID from topic
	chainID, err := parseTopicNameToChainID(msg.Topic, c.networkID, c.eventType)
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
