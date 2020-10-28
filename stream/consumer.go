// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

const (
	consumerEventType = EventTypeDecisions
)

var (
	consumerInitializeTimeout = 3 * time.Minute
)

type serviceConsumerFactory func(*services.Connections, uint32, string, string) (services.Consumer, error)

// consumer takes events from Kafka and sends them to a service consumer
type consumer struct {
	chainID                     string
	reader                      *kafka.Reader
	consumer                    services.Consumer
	conns                       *services.Connections
	metricProcessedCountKey     string
	metricProcessMillisCountKey string
	metricSuccessCountKey       string
	metricFailureCountKey       string
}

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerFactory(factory serviceConsumerFactory) ProcessorFactory {
	return func(conf cfg.Config, chainVM string, chainID string) (Processor, error) {
		conns, err := services.NewConnectionsFromConfig(conf.Services)
		if err != nil {
			return nil, err
		}

		c := &consumer{
			chainID:                     chainID,
			conns:                       conns,
			metricProcessedCountKey:     fmt.Sprintf("records_processed_%s", chainID),
			metricProcessMillisCountKey: fmt.Sprintf("records_process_millis_%s", chainID),
			metricSuccessCountKey:       fmt.Sprintf("records_success_%s", chainID),
			metricFailureCountKey:       fmt.Sprintf("records_failure_%s", chainID),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCountKey, "records process millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")

		// Create consumer backend
		c.consumer, err = factory(conns, conf.NetworkID, chainVM, chainID)
		if err != nil {
			return nil, err
		}

		// Bootstrap our service
		ctx, cancelFn := context.WithTimeout(context.Background(), consumerInitializeTimeout)
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
			Topic:       GetTopicName(conf.NetworkID, chainID, consumerEventType),
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

		return c, nil
	}
}

// Close closes the consumer
func (c *consumer) Close() error {
	errs := wrappers.Errs{}
	errs.Add(c.conns.Close(), c.reader.Close())
	return errs.Err
}

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumer) ProcessNextMessage(ctx context.Context, log logging.Logger) error {
	msg, err := c.getNextMessage(ctx)
	if err != nil {
		log.Error("consumer.getNextMessage: %s", err.Error())
		return err
	}

	metrics.Prometheus.CounterInc(c.metricProcessedCountKey)

	timeNow := time.Now()
	defer metrics.Prometheus.CounterAdd(c.metricProcessMillisCountKey, float64(time.Now().Sub(timeNow).Milliseconds()))
	if err = c.consumer.Consume(ctx, msg); err != nil {
		log.Error("consumer.Consume: %s", err.Error())
		return err
	}
	return nil
}

func (c *consumer) Failure() {
	metrics.Prometheus.CounterInc(c.metricFailureCountKey)
}

func (c *consumer) Success() {
	metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *consumer) getNextMessage(ctx context.Context) (*Message, error) {
	// Get raw Message from Kafka
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	// Extract Message ID from key
	id, err := ids.ToID(msg.Key)
	if err != nil {
		return nil, err
	}

	return &Message{
		chainID:   c.chainID,
		body:      msg.Value,
		id:        id.String(),
		timestamp: msg.Time.UTC().Unix(),
	}, nil
}
