// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
)

// consumer takes events from db and sends them to a service consumer
type consumerDB struct {
	id        string
	eventType EventType

	chainID  string
	consumer services.Consumer
	sc       *servicesctrl.Control

	// metrics
	metricProcessedCountKey       string
	metricFailureCountKey         string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string

	topicName string
}

type serviceConsumerFactory func(uint32, string, string) (services.Consumer, error)

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerDBFactory(factory serviceConsumerFactory, eventType EventType) ProcessorFactoryChainDB {
	return func(sc *servicesctrl.Control, conf cfg.Config, chainVM string, chainID string) (ProcessorDB, error) {
		c := &consumerDB{
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
			c.id = fmt.Sprintf("consumer %d %s %s", conf.NetworkID, chainVM, chainID)
		case EventTypeConsensus:
			c.metricProcessedCountKey = fmt.Sprintf("consume_consensus_records_processed_%s", chainID)
			c.metricProcessMillisCounterKey = fmt.Sprintf("consume_consensus_records_process_millis_%s", chainID)
			c.metricSuccessCountKey = fmt.Sprintf("consume_consensus_records_success_%s", chainID)
			c.metricFailureCountKey = fmt.Sprintf("consume_consensus_records_failure_%s", chainID)
			c.id = fmt.Sprintf("consumer_consensus %d %s %s", conf.NetworkID, chainVM, chainID)
		}

		utils.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		utils.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		utils.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		utils.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		var err error
		c.consumer, err = factory(conf.NetworkID, chainVM, chainID)
		if err != nil {
			_ = c.Close()
			return nil, err
		}

		c.topicName = GetTopicName(conf.NetworkID, chainID, c.eventType)

		return c, nil
	}
}

func (c *consumerDB) ID() string {
	return c.id
}

func (c *consumerDB) Topic() []string {
	return []string{c.topicName}
}

// Close closes the consumer
func (c *consumerDB) Close() error {
	return nil
}

func (c *consumerDB) Process(conns *utils.Connections, row *db.TxPool) error {
	msg := &Message{
		id:         row.MsgKey,
		chainID:    c.chainID,
		body:       row.Serialization,
		timestamp:  row.CreatedAt.UTC().Unix(),
		nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
	}
	return c.Consume(conns, msg)
}

func (c *consumerDB) Consume(conns *utils.Connections, msg *Message) error {
	collectors := utils.NewCollectors(
		utils.NewCounterIncCollect(c.metricProcessedCountKey),
		utils.NewCounterObserveMillisCollect(c.metricProcessMillisCounterKey),
		utils.NewCounterIncCollect(servicesctrl.MetricConsumeProcessedCountKey),
		utils.NewCounterObserveMillisCollect(servicesctrl.MetricConsumeProcessMillisCounterKey),
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
		if !utils.ErrIsLockError(err) {
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

	c.sc.BalanceManager.Exec()
	return err
}

func (c *consumerDB) persistConsume(conns *utils.Connections, msg *Message) error {
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

func (c *consumerDB) Failure() {
	_ = utils.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = utils.Prometheus.CounterInc(servicesctrl.MetricConsumeFailureCountKey)
}

func (c *consumerDB) Success() {
	_ = utils.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = utils.Prometheus.CounterInc(servicesctrl.MetricConsumeSuccessCountKey)
}
