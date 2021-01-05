// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

const (
	kafkaReadTimeout = 10 * time.Second

	ConsumerEventTypeDefault = EventTypeDecisions
	ConsumerMaxBytesDefault  = 10e8
)

var (
	consumerInitializeTimeout = 5 * time.Minute
)

type serviceConsumerFactory func(*services.Connections, uint32, string, string) (services.Consumer, error)

// consumer takes events from Kafka and sends them to a service consumer
type consumer struct {
	id       string
	chainID  string
	reader   *kafka.Reader
	consumer services.Consumer
	conns    *services.Connections

	// metrics
	metricProcessedCountKey       string
	metricFailureCountKey         string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string
}

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerFactory(factory serviceConsumerFactory) ProcessorFactory {
	return func(conf cfg.Config, chainVM string, chainID string) (Processor, error) {
		conns, err := services.NewConnectionsFromConfig(conf.Services, false)
		if err != nil {
			return nil, err
		}

		conns.DB().SetMaxIdleConns(32)
		conns.DB().SetConnMaxIdleTime(5 * time.Minute)
		conns.DB().SetConnMaxLifetime(5 * time.Minute)

		c := &consumer{
			chainID:                       chainID,
			conns:                         conns,
			metricProcessedCountKey:       fmt.Sprintf("consume_records_processed_%s", chainID),
			metricProcessMillisCounterKey: fmt.Sprintf("consume_records_process_millis_%s", chainID),
			metricSuccessCountKey:         fmt.Sprintf("consume_records_success_%s", chainID),
			metricFailureCountKey:         fmt.Sprintf("consume_records_failure_%s", chainID),
			id:                            fmt.Sprintf("consumer %d %s %s", conf.NetworkID, chainVM, chainID),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")

		initializeConsumerTasker(conns)

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
			Topic:       GetTopicName(conf.NetworkID, chainID, EventTypeDecisions),
			Brokers:     conf.Kafka.Brokers,
			GroupID:     groupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    ConsumerMaxBytesDefault,
		})

		// If the start time is set then seek to the correct offset
		if !conf.Consumer.StartTime.IsZero() {
			ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
			defer cancelFn()

			if err = c.reader.SetOffsetAt(ctx, conf.Consumer.StartTime); err != nil {
				return nil, err
			}
		}

		return c, nil
	}
}

func (c *consumer) ID() string {
	return c.id
}

// Close closes the consumer
func (c *consumer) Close() error {
	errs := wrappers.Errs{}
	errs.Add(c.conns.Close(), c.reader.Close())
	return errs.Err
}

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumer) ProcessNextMessage() error {
	msg, err := c.nextMessage()
	if err != nil {
		if err != context.DeadlineExceeded {
			c.conns.Logger().Error("consumer.getNextMessage: %s", err.Error())
		}
		return err
	}

	collectors := metrics.NewCollectors(
		metrics.NewCounterIncCollect(c.metricProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(c.metricProcessMillisCounterKey),
	)
	defer func() {
		err := collectors.Collect()
		if err != nil {
			c.conns.Logger().Error("collectors.Collect: %s", err)
		}
	}()

	id, err := ids.FromString(msg.ID())
	if err != nil {
		collectors.Error()
		c.conns.Logger().Error("consumer.Consume: %s %v", id.String(), err)
		return err
	}

	icnt := 0
	for ; icnt <= cfg.DatabaseRetries; icnt++ {
		err = c.persistConsume(msg)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), db.DeadlockDBErrorMessage) {
			c.conns.Logger().Warn("consumer.Consume: %s %v", id.String(), err)
		} else {
			icnt = 0
		}

		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		collectors.Error()
		c.conns.Logger().Error("consumer.Consume: %s %v", id.String(), err)
		return err
	}
	return nil
}

func (c *consumer) persistConsume(msg *Message) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.Consume(ctx, msg)
}

func (c *consumer) nextMessage() (*Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()

	return c.getNextMessage(ctx)
}

func (c *consumer) Failure() {
	err := metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	if err != nil {
		c.conns.Logger().Error("prmetheus.CounterInc: %s", err)
	}
}

func (c *consumer) Success() {
	err := metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	if err != nil {
		c.conns.Logger().Error("prmetheus.CounterInc: %s", err)
	}
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
