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

// consumer takes events from Kafka and sends them to a service consumer
type consumerconsensus struct {
	id       string
	chainID  string
	reader   *kafka.Reader
	consumer services.Consumer
	conns    *services.Connections
	sc       *services.Control

	// metrics
	metricProcessedCountKey       string
	metricFailureCountKey         string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string

	groupName string

	BalancerAccumulateHandler services.BalancerAccumulateHandler
}

// NewConsumerConsensusFactory returns a processorFactory for the given service consumer
func NewConsumerConsensusFactory(factory serviceConsumerFactory) ProcessorFactory {
	return func(sc *services.Control, conf cfg.Config, chainVM string, chainID string) (Processor, error) {
		conns, err := sc.Database()
		if err != nil {
			return nil, err
		}

		c := &consumerconsensus{
			chainID:                       chainID,
			conns:                         conns,
			sc:                            sc,
			metricProcessedCountKey:       fmt.Sprintf("consume_consensus_records_processed_%s", chainID),
			metricProcessMillisCounterKey: fmt.Sprintf("consume_consensus_records_process_millis_%s", chainID),
			metricSuccessCountKey:         fmt.Sprintf("consume_consensus_records_success_%s", chainID),
			metricFailureCountKey:         fmt.Sprintf("consume_consensus_records_failure_%s", chainID),
			id:                            fmt.Sprintf("consumer_consensus %d %s %s", conf.NetworkID, chainVM, chainID),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		// Create consumer backend
		c.consumer, err = factory(conf.NetworkID, chainVM, chainID)
		if err != nil {
			return nil, err
		}

		// Bootstrap our service
		ctx, cancelFn := context.WithTimeout(context.Background(), consumerInitializeTimeout)
		defer cancelFn()
		if err = c.consumer.Bootstrap(ctx, c.conns, sc.Persist); err != nil {
			return nil, err
		}

		// Setup config
		c.groupName = conf.Consumer.GroupName
		if c.groupName == "" {
			c.groupName = c.consumer.Name()
		}
		if !conf.Consumer.StartTime.IsZero() {
			c.groupName = ""
		}

		topicName := GetTopicName(conf.NetworkID, chainID, EventTypeConsensus)
		// Create reader for the topic
		c.reader = kafka.NewReader(kafka.ReaderConfig{
			Topic:       topicName,
			Brokers:     conf.Kafka.Brokers,
			GroupID:     c.groupName,
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

func (c *consumerconsensus) ID() string {
	return c.id
}

// Close closes the consumer
func (c *consumerconsensus) Close() error {
	c.sc.Log.Info("close %s", c.id)
	errs := wrappers.Errs{}
	if c.reader != nil {
		errs.Add(c.reader.Close())
	}
	if c.conns != nil {
		errs.Add(c.conns.Close())
	}
	return errs.Err
}

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumerconsensus) ProcessNextMessage() error {
	msg, err := c.nextMessage()
	if err != nil {
		if err != context.DeadlineExceeded {
			c.sc.Log.Error("consumer.getNextMessage: %s", err.Error())
		}
		return err
	}

	collectors := metrics.NewCollectors(
		metrics.NewCounterIncCollect(c.metricProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(c.metricProcessMillisCounterKey),
		metrics.NewCounterIncCollect(services.MetricConsumeProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(services.MetricConsumeProcessMillisCounterKey),
	)
	defer func() {
		err := collectors.Collect()
		if err != nil {
			c.sc.Log.Error("collectors.Collect: %s", err)
		}
	}()

	for {
		err = c.persistConsume(msg)
		if err == nil || !strings.Contains(err.Error(), db.DeadlockDBErrorMessage) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if err != nil {
		collectors.Error()
		c.sc.Log.Error("consumer.ConsumeConsensus: %s", err)
		return err
	}

	c.BalancerAccumulateHandler.Run(c.sc.Persist, c.sc)

	return c.commitMessage(msg)
}

func (c *consumerconsensus) persistConsume(msg *Message) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.ConsumeConsensus(ctx, c.conns, msg, c.sc.Persist)
}

func (c *consumerconsensus) nextMessage() (*Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()

	return c.getNextMessage(ctx)
}

func (c *consumerconsensus) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeFailureCountKey)
}

func (c *consumerconsensus) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeSuccessCountKey)
}

func (c *consumerconsensus) commitMessage(msg services.Consumable) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()
	return c.reader.CommitMessages(ctx, *msg.KafkaMessage())
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *consumerconsensus) getNextMessage(ctx context.Context) (*Message, error) {
	// Get raw Message from Kafka
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	m := &Message{
		chainID:      c.chainID,
		body:         msg.Value,
		timestamp:    msg.Time.UTC().Unix(),
		kafkaMessage: &msg,
	}

	// Extract Message ID from key
	id, err := ids.ToID(msg.Key)
	if err != nil {
		m.id = string(msg.Key)
	} else {
		m.id = id.String()
	}

	return m, nil
}
