// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ava-labs/coreth/core/types"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/avalanchego/utils/hashing"
	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/ortelius/services/indexes/cvm"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

type consumerCChainDB struct {
	id string
	sc *services.Control

	// metrics
	metricProcessedCountKey       string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string
	metricFailureCountKey         string

	conf cfg.Config

	// Concurrency control
	quitCh   chan struct{}
	consumer *cvm.Writer

	topicName     string
	topicTrcName  string
	topicLogsName string
}

func NewConsumerCChainDB() ProcessorFactoryInstDB {
	return func(sc *services.Control, conf cfg.Config) (ProcessorDB, error) {
		c := &consumerCChainDB{
			conf:                          conf,
			sc:                            sc,
			metricProcessedCountKey:       fmt.Sprintf("consume_records_processed_%s_cchain", conf.CchainID),
			metricProcessMillisCounterKey: fmt.Sprintf("consume_records_process_millis_%s_cchain", conf.CchainID),
			metricSuccessCountKey:         fmt.Sprintf("consume_records_success_%s_cchain", conf.CchainID),
			metricFailureCountKey:         fmt.Sprintf("consume_records_failure_%s_cchain", conf.CchainID),
			id:                            fmt.Sprintf("consumer %d %s cchain", conf.NetworkID, conf.CchainID),

			quitCh: make(chan struct{}),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		var err error
		c.consumer, err = cvm.NewWriter(c.conf.NetworkID, c.conf.CchainID)
		if err != nil {
			_ = c.Close()
			return nil, err
		}

		c.topicName = fmt.Sprintf("%d-%s-cchain", c.conf.NetworkID, c.conf.CchainID)
		c.topicTrcName = fmt.Sprintf("%d-%s-cchain-trc", c.conf.NetworkID, c.conf.CchainID)
		c.topicLogsName = fmt.Sprintf("%d-%s-cchain-logs", conf.NetworkID, conf.CchainID)

		return c, nil
	}
}

// Close shuts down the producer
func (c *consumerCChainDB) Close() error {
	return nil
}

func (c *consumerCChainDB) ID() string {
	return c.id
}

func (c *consumerCChainDB) Topic() []string {
	return []string{c.topicName, c.topicTrcName, c.topicLogsName}
}

func (c *consumerCChainDB) Process(conns *services.Connections, row *services.TxPool) error {
	switch row.Topic {
	case c.topicName:
		msg := &Message{
			id:         row.MsgKey,
			chainID:    c.conf.CchainID,
			body:       row.Serialization,
			timestamp:  row.CreatedAt.UTC().Unix(),
			nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
		}
		return c.Consume(conns, msg)
	case c.topicTrcName:
		msg := &Message{
			id:         row.MsgKey,
			chainID:    c.conf.CchainID,
			body:       row.Serialization,
			timestamp:  row.CreatedAt.UTC().Unix(),
			nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
		}
		return c.ConsumeTrace(conns, msg)
	case c.topicLogsName:
		msg := &Message{
			id:         row.MsgKey,
			chainID:    c.conf.CchainID,
			body:       row.Serialization,
			timestamp:  row.CreatedAt.UTC().Unix(),
			nanosecond: int64(row.CreatedAt.UTC().Nanosecond()),
		}
		return c.ConsumeLogs(conns, msg)
	}

	return nil
}

func (c *consumerCChainDB) ConsumeLogs(conns *services.Connections, msg services.Consumable) error {
	txLogs := &types.Log{}
	err := json.Unmarshal(msg.Body(), txLogs)
	if err != nil {
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

	id := hashing.ComputeHash256(msg.Body())

	nmsg := NewMessage(string(id), msg.ChainID(), msg.Body(), msg.Timestamp(), msg.Nanosecond())

	rsleep := utils.NewRetrySleeper(5, 100*time.Millisecond, time.Second)
	for {
		err = c.persistConsumeLogs(conns, nmsg, txLogs)
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

func (c *consumerCChainDB) ConsumeTrace(conns *services.Connections, msg services.Consumable) error {
	transactionTrace := &cblock.TransactionTrace{}
	err := json.Unmarshal(msg.Body(), transactionTrace)
	if err != nil {
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

	id := hashing.ComputeHash256(transactionTrace.Trace)

	nmsg := NewMessage(string(id), msg.ChainID(), transactionTrace.Trace, msg.Timestamp(), msg.Nanosecond())

	rsleep := utils.NewRetrySleeper(5, 100*time.Millisecond, time.Second)
	for {
		err = c.persistConsumeTrace(conns, nmsg, transactionTrace)
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

func (c *consumerCChainDB) Consume(conns *services.Connections, msg services.Consumable) error {
	block, err := cblock.Unmarshal(msg.Body())
	if err != nil {
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

	if block.BlockExtraData == nil {
		block.BlockExtraData = []byte("")
	}

	id := hashing.ComputeHash256(block.BlockExtraData)
	nmsg := NewMessage(string(id), msg.ChainID(), block.BlockExtraData, msg.Timestamp(), msg.Nanosecond())

	rsleep := utils.NewRetrySleeper(5, 100*time.Millisecond, time.Second)
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

	c.sc.BalanceAccumulatorManager.Run(c.sc)

	return nil
}

func (c *consumerCChainDB) persistConsumeLogs(conns *services.Connections, msg services.Consumable, txLogs *types.Log) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.ConsumeLogs(ctx, conns, msg, txLogs, c.sc.Persist)
}

func (c *consumerCChainDB) persistConsumeTrace(conns *services.Connections, msg services.Consumable, transactionTrace *cblock.TransactionTrace) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.ConsumeTrace(ctx, conns, msg, transactionTrace, c.sc.Persist)
}

func (c *consumerCChainDB) persistConsume(conns *services.Connections, msg services.Consumable, block *cblock.Block) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.Consume(ctx, conns, msg, block, c.sc.Persist)
}

func (c *consumerCChainDB) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeFailureCountKey)
}

func (c *consumerCChainDB) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeSuccessCountKey)
}
