// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
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

	pollLimit = 100
)

type serviceConsumerFactory func(uint32, string, string) (services.Consumer, error)

// consumer takes events from Kafka and sends them to a service consumer
type consumer struct {
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
	topicName string

	idx    int
	maxIdx int
}

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerFactory(factory serviceConsumerFactory) ProcessorFactory {
	return func(sc *services.Control, conf cfg.Config, chainVM string, chainID string, idx int, maxIdx int) (Processor, error) {
		conns, err := sc.DatabaseOnly()
		if err != nil {
			return nil, err
		}

		c := &consumer{
			idx:                           idx,
			maxIdx:                        maxIdx,
			chainID:                       chainID,
			conns:                         conns,
			sc:                            sc,
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
		sc.InitConsumeMetrics()

		// Create consumer backend
		c.consumer, err = factory(conf.NetworkID, chainVM, chainID)
		if err != nil {
			c.Close()
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

		c.topicName = GetTopicName(conf.NetworkID, chainID, EventTypeDecisions)
		// Create reader for the topic
		c.reader = kafka.NewReader(kafka.ReaderConfig{
			Topic:       c.topicName,
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
				c.Close()
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
func (c *consumer) ProcessNextMessage() error {
	wm := func(msg *Message) error {
		var err error
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
			if !db.ErrIsLockError(err) {
				break
			}
		}
		if err != nil {
			collectors.Error()
			c.sc.Log.Error("consumer.Consume: %s", err)
			return err
		}

		c.sc.BalanceAccumulatorManager.Run(c.sc.Persist, c.sc)
		return err
	}

	if c.sc.IsDBPoll {
		job := c.conns.Stream().NewJob("query-txpoll")
		sess := c.conns.DB().NewSessionForEventReceiver(job)

		updateStatus := func(txPoll *services.TxPool) error {
			ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
			defer cancelFn()
			return c.sc.Persist.UpdateTxPoolStatus(ctx, sess, txPoll)
		}

		var err error
		var rowdata []*services.TxPool
		rowdata, err = fetchPollForTopic(sess, c.topicName, &c.idx, c.maxIdx)

		if err != nil {
			return err
		}

		if len(rowdata) == 0 {
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		for _, row := range rowdata {
			msg := &Message{
				id:         row.MsgKey,
				chainID:    c.chainID,
				body:       row.Serialization,
				timestamp:  row.CreatedAt.UTC().Unix(),
				nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
			}
			err = wm(msg)
			if err != nil {
				return err
			}
			row.Processed = 1
			err = updateStatus(row)
			if err != nil {
				return err
			}
		}

		return nil
	}

	msg, err := c.nextMessage()
	if err != nil {
		if err != context.DeadlineExceeded {
			c.sc.Log.Error("consumer.getNextMessage: %s", err.Error())
		}
		return err
	}

	err = wm(msg)
	if err != nil {
		return err
	}

	return c.commitMessage(msg)
}

func (c *consumer) persistConsume(msg *Message) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.Consume(ctx, c.conns, msg, c.sc.Persist)
}

func (c *consumer) nextMessage() (*Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()

	return c.getNextMessage(ctx)
}

func (c *consumer) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeFailureCountKey)
}

func (c *consumer) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeSuccessCountKey)
}

func (c *consumer) commitMessage(msg services.Consumable) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()
	return c.reader.CommitMessages(ctx, *msg.KafkaMessage())
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *consumer) getNextMessage(ctx context.Context) (*Message, error) {
	// Get raw Message from Kafka
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	m := &Message{
		chainID:      c.chainID,
		body:         msg.Value,
		timestamp:    msg.Time.UTC().Unix(),
		nanosecond:   int64(msg.Time.UTC().Nanosecond()),
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
