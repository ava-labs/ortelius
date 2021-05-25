// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/ava-labs/ortelius/services/idb"
	"github.com/ava-labs/ortelius/services/metrics"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/services/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
)

// consumer takes events from db and sends them to a service consumer
type consumer struct {
	eventType EventType

	chainID  string
	consumer services.Consumer
	sc       *servicesctrl.Control

	metricProcessedCountKey       string
	metricFailureCountKey         string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string

	topicName string
}

type serviceConsumerFactory func(uint32, string, string) (services.Consumer, error)

func NewConsumer(factory serviceConsumerFactory, eventType EventType) ProcessorFactoryChain {
	return func(sc *servicesctrl.Control, conf cfg.Config, chainVM string, chainID string) (Processor, error) {
		c := &consumer{
			eventType: eventType,
			chainID:   chainID,
			sc:        sc,
		}

		switch eventType {
		case EventTypeDecisions:
			c.metricProcessedCountKey = fmt.Sprintf("consume_records_processed_%s", chainID)
			c.metricProcessMillisCounterKey = fmt.Sprintf("consume_records_process_millis_%s", chainID)
			c.metricSuccessCountKey = fmt.Sprintf("consume_records_success_%s", chainID)
			c.metricFailureCountKey = fmt.Sprintf("consume_records_failure_%s", chainID)
		case EventTypeConsensus:
			c.metricProcessedCountKey = fmt.Sprintf("consume_consensus_records_processed_%s", chainID)
			c.metricProcessMillisCounterKey = fmt.Sprintf("consume_consensus_records_process_millis_%s", chainID)
			c.metricSuccessCountKey = fmt.Sprintf("consume_consensus_records_success_%s", chainID)
			c.metricFailureCountKey = fmt.Sprintf("consume_consensus_records_failure_%s", chainID)
		}

		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		var err error
		c.consumer, err = factory(conf.NetworkID, chainVM, chainID)
		if err != nil {
			return nil, err
		}

		c.topicName = GetTopicName(conf.NetworkID, chainID, c.eventType)

		return c, nil
	}
}

func (c *consumer) Topic() []string {
	return []string{c.topicName}
}

func (c *consumer) Process(conns *servicesconn.Connections, row *idb.TxPool) error {
	msg := &Message{
		body:       row.Serialization,
		timestamp:  row.CreatedAt.UTC().Unix(),
		nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
	}
	return c.Consume(conns, msg)
}

func (c *consumer) Consume(conns *servicesconn.Connections, msg *Message) error {
	collectors := metrics.NewCollectors(
		metrics.NewCounterIncCollect(c.metricProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(c.metricProcessMillisCounterKey),
		metrics.NewCounterIncCollect(servicesctrl.MetricConsumeProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(servicesctrl.MetricConsumeProcessMillisCounterKey),
	)
	defer func() {
		err := collectors.Collect()
		if err != nil {
			c.sc.Log.Error("collectors.Collect: %s", err)
		}
	}()

	var err error
	rsleep := utils.NewRetrySleeper(1, 100*time.Millisecond, time.Second)
	for {
		err = c.persistConsume(conns, msg)
		if !db.ErrIsLockError(err) {
			break
		}
		rsleep.Inc()
	}
	if err != nil {
		c.Failure()
		collectors.Error()
		c.sc.Log.Error("consumer.Consume: %s", err)
		return err
	}
	c.Success()

	c.sc.BalanceManager.Run()
	return err
}

func (c *consumer) persistConsume(conns *servicesconn.Connections, msg *Message) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	switch c.eventType {
	case EventTypeDecisions:
		return c.consumer.Consume(ctx, conns, msg, c.sc.Persist)
	case EventTypeConsensus:
		return c.consumer.ConsumeConsensus(ctx, conns, msg, c.sc.Persist)
	default:
		return fmt.Errorf("invalid eventType %v", c.eventType)
	}
}

func (c *consumer) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(servicesctrl.MetricConsumeFailureCountKey)
}

func (c *consumer) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(servicesctrl.MetricConsumeSuccessCountKey)
}
