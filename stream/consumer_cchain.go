// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/coreth/core/types"
	"github.com/ava-labs/ortelius/cblock"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/metrics"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/ava-labs/ortelius/services/idb"
	"github.com/ava-labs/ortelius/services/indexes/cvm"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/services/servicesctrl"
)

type consumerCChain struct {
	sc *servicesctrl.Control

	metricProcessedCountKey       string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string
	metricFailureCountKey         string

	conf cfg.Config

	consumer *cvm.Writer

	topicName     string
	topicTrcName  string
	topicLogsName string
}

func NewConsumerCChain() ProcessorFactoryInst {
	return func(sc *servicesctrl.Control, conf cfg.Config) (Processor, error) {
		c := &consumerCChain{
			conf:                          conf,
			sc:                            sc,
			metricProcessedCountKey:       fmt.Sprintf("consume_records_processed_%s_cchain", conf.CchainID),
			metricProcessMillisCounterKey: fmt.Sprintf("consume_records_process_millis_%s_cchain", conf.CchainID),
			metricSuccessCountKey:         fmt.Sprintf("consume_records_success_%s_cchain", conf.CchainID),
			metricFailureCountKey:         fmt.Sprintf("consume_records_failure_%s_cchain", conf.CchainID),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		var err error
		c.consumer, err = cvm.NewWriter(c.conf.NetworkID, c.conf.CchainID)
		if err != nil {
			return nil, err
		}

		c.topicName = fmt.Sprintf("%d-%s-cchain", c.conf.NetworkID, c.conf.CchainID)
		c.topicTrcName = fmt.Sprintf("%d-%s-cchain-trc", c.conf.NetworkID, c.conf.CchainID)
		c.topicLogsName = fmt.Sprintf("%d-%s-cchain-logs", conf.NetworkID, conf.CchainID)

		return c, nil
	}
}

func (c *consumerCChain) Topic() []string {
	return []string{c.topicName, c.topicTrcName, c.topicLogsName}
}

func (c *consumerCChain) Process(conns *servicesconn.Connections, row *idb.TxPool) error {
	switch row.Topic {
	case c.topicName:
		msg := &Message{
			body:       row.Serialization,
			timestamp:  row.CreatedAt.UTC().Unix(),
			nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
		}
		return c.Consume(conns, msg)
	case c.topicTrcName:
		msg := &Message{
			body:       row.Serialization,
			timestamp:  row.CreatedAt.UTC().Unix(),
			nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
		}
		return c.ConsumeTrace(conns, msg)
	case c.topicLogsName:
		msg := &Message{
			body:       row.Serialization,
			timestamp:  row.CreatedAt.UTC().Unix(),
			nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
		}
		return c.ConsumeLogs(conns, msg)
	}

	return nil
}

func (c *consumerCChain) ConsumeLogs(conns *servicesconn.Connections, msg services.Consumable) error {
	txLogs := &types.Log{}
	err := json.Unmarshal(msg.Body(), txLogs)
	if err != nil {
		return err
	}
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

	rsleep := utils.NewRetrySleeper(1, 100*time.Millisecond, time.Second)
	for {
		err = c.persistConsumeLogs(conns, msg, txLogs)
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

	return nil
}

func (c *consumerCChain) ConsumeTrace(conns *servicesconn.Connections, msg services.Consumable) error {
	transactionTrace := &cblock.TransactionTrace{}
	err := json.Unmarshal(msg.Body(), transactionTrace)
	if err != nil {
		return err
	}
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

	rsleep := utils.NewRetrySleeper(1, 100*time.Millisecond, time.Second)
	for {
		err = c.persistConsumeTrace(conns, msg, transactionTrace)
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

	return nil
}

func (c *consumerCChain) Consume(conns *servicesconn.Connections, msg services.Consumable) error {
	block, err := cblock.Unmarshal(msg.Body())
	if err != nil {
		return err
	}

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

	if block.BlockExtraData == nil {
		block.BlockExtraData = []byte("")
	}

	nmsg := NewMessage(block.BlockExtraData, msg.Timestamp(), msg.Nanosecond())

	rsleep := utils.NewRetrySleeper(1, 100*time.Millisecond, time.Second)
	for {
		err = c.persistConsume(conns, nmsg, block)
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

	return nil
}

func (c *consumerCChain) persistConsumeLogs(conns *servicesconn.Connections, msg services.Consumable, txLogs *types.Log) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.ConsumeLogs(ctx, conns, msg, txLogs, c.sc.Persist)
}

func (c *consumerCChain) persistConsumeTrace(conns *servicesconn.Connections, msg services.Consumable, transactionTrace *cblock.TransactionTrace) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.ConsumeTrace(ctx, conns, msg, transactionTrace, c.sc.Persist)
}

func (c *consumerCChain) persistConsume(conns *servicesconn.Connections, msg services.Consumable, block *cblock.Block) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.Consume(ctx, conns, msg, block, c.sc.Persist)
}

func (c *consumerCChain) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(servicesctrl.MetricConsumeFailureCountKey)
}

func (c *consumerCChain) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(servicesctrl.MetricConsumeSuccessCountKey)
}
