// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/avalanchego/utils/hashing"
	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services/indexes/cvm"

	"github.com/ava-labs/avalanchego/utils/wrappers"

	"github.com/ava-labs/ortelius/utils"

	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

type ConsumerCChain struct {
	id string
	sc *services.Control

	// metrics
	metricProcessedCountKey       string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string
	metricFailureCountKey         string

	conns  *services.Connections
	reader *kafka.Reader
	conf   cfg.Config

	// Concurrency control
	quitCh   chan struct{}
	doneCh   chan struct{}
	consumer *cvm.Writer

	groupName string
}

func NewConsumerCChain() utils.ListenCloserFactory {
	return func(sc *services.Control, conf cfg.Config) utils.ListenCloser {
		c := &ConsumerCChain{
			conf:                          conf,
			sc:                            sc,
			metricProcessedCountKey:       fmt.Sprintf("consume_records_processed_%s_cchain", conf.CchainID),
			metricProcessMillisCounterKey: fmt.Sprintf("consume_records_process_millis_%s_cchain", conf.CchainID),
			metricSuccessCountKey:         fmt.Sprintf("consume_records_success_%s_cchain", conf.CchainID),
			metricFailureCountKey:         fmt.Sprintf("consume_records_failure_%s_cchain", conf.CchainID),
			id:                            fmt.Sprintf("consumer %d %s cchain", conf.NetworkID, conf.CchainID),

			quitCh: make(chan struct{}),
			doneCh: make(chan struct{}),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		// Setup config
		c.groupName = conf.Consumer.GroupName
		if c.groupName == "" {
			c.groupName = c.consumer.Name()
		}
		if !conf.Consumer.StartTime.IsZero() {
			c.groupName = ""
		}

		topicName := fmt.Sprintf("%d-%s-cchain", conf.NetworkID, conf.CchainID)
		c.reader = kafka.NewReader(kafka.ReaderConfig{
			Topic:       topicName,
			Brokers:     conf.Kafka.Brokers,
			GroupID:     c.groupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    ConsumerMaxBytesDefault,
		})

		return c
	}
}

// Close shuts down the producer
func (c *ConsumerCChain) Close() error {
	close(c.quitCh)
	<-c.doneCh
	return nil
}

func (c *ConsumerCChain) ID() string {
	return c.id
}

func (c *ConsumerCChain) ProcessNextMessage() error {
	msg, err := c.nextMessage()
	if err != nil {
		if err != context.DeadlineExceeded {
			c.sc.Log.Error("consumer.getNextMessage: %s", err.Error())
		}
		return err
	}

	return c.Consume(msg, c.sc.Persist)
}

func (c *ConsumerCChain) Consume(msg services.Consumable, persist services.Persist) error {
	block, err := cblock.Unmarshal(msg.Body())
	if err != nil {
		return err
	}

	if len(block.BlockExtraData) == 0 {
		return c.commitMessage(msg)
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

	id := hashing.ComputeHash256(block.BlockExtraData)
	nmsg := NewMessage(string(id), msg.ChainID(), block.BlockExtraData, msg.Timestamp())

	for {
		err = c.persistConsume(nmsg, block)
		if err == nil || !strings.Contains(err.Error(), db.DeadlockDBErrorMessage) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if err != nil {
		collectors.Error()
		c.sc.Log.Error("consumer.Consume: %s", err)
		return err
	}

	c.sc.BalancerAccumulateHandler.Run(c.conns, c.sc.Persist, c.sc)

	return c.commitMessage(msg)
}

func (c *ConsumerCChain) persistConsume(msg services.Consumable, block *cblock.Block) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.Consume(ctx, msg, &block.Header, c.sc.Persist)
}

func (c *ConsumerCChain) nextMessage() (*Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()

	return c.getNextMessage(ctx)
}

func (c *ConsumerCChain) commitMessage(msg services.Consumable) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()
	return c.reader.CommitMessages(ctx, *msg.KafkaMessage())
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *ConsumerCChain) getNextMessage(ctx context.Context) (*Message, error) {
	// Get raw Message from Kafka
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	m := &Message{
		chainID:      c.conf.CchainID,
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

func (c *ConsumerCChain) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeFailureCountKey)
}

func (c *ConsumerCChain) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeSuccessCountKey)
}

func (c *ConsumerCChain) Listen() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		c.sc.Log.Info("Started worker manager for cchain")
		defer c.sc.Log.Info("Exiting worker manager for cchain")
		defer wg.Done()

		// Keep running the worker until we're asked to stop
		var err error
		for !c.isStopping() {
			err = c.runProcessor()

			// If there was an error we want to log it, and iff we are not stopping
			// we want to add a retry delay.
			if err != nil {
				c.sc.Log.Error("Error running worker: %s", err.Error())
			}
			if c.isStopping() {
				return
			}
			if err != nil {
				<-time.After(processorFailureRetryInterval)
			}
		}
	}()

	// Wait for all workers to finish
	wg.Wait()
	c.sc.Log.Info("All workers stopped")
	close(c.doneCh)

	return nil
}

// isStopping returns true iff quitCh has been signaled
func (c *ConsumerCChain) isStopping() bool {
	select {
	case <-c.quitCh:
		return true
	default:
		return false
	}
}

func (c *ConsumerCChain) init() error {
	conns, err := c.sc.Database()
	if err != nil {
		return err
	}

	c.conns = conns

	consumer, err := cvm.NewWriter(c.conns, c.conf.NetworkID, c.conf.CchainID)
	if err != nil {
		return err
	}

	c.consumer = consumer

	return nil
}

func (c *ConsumerCChain) processorClose() error {
	c.sc.Log.Info("processorClose %s", c.id)
	errs := wrappers.Errs{}
	if c.reader != nil {
		errs.Add(c.reader.Close())
	}
	if c.conns != nil {
		errs.Add(c.conns.Close())
	}
	return errs.Err
}

// runProcessor starts the processing loop for the backend and closes it when
// finished
func (c *ConsumerCChain) runProcessor() error {
	if c.isStopping() {
		c.sc.Log.Info("Not starting worker for cchain because we're stopping")
		return nil
	}

	c.sc.Log.Info("Starting worker for cchain")
	defer c.sc.Log.Info("Exiting worker for cchain")

	defer func() {
		err := c.processorClose()
		if err != nil {
			c.sc.Log.Warn("Stopping worker for cchain %w", err)
		}
	}()
	err := c.init()
	if err != nil {
		return err
	}

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := c.ProcessNextMessage()

			switch err {
			case nil:
				successes++
				c.Success()
				return nil

			// This error is expected when the upstream service isn't producing
			case context.DeadlineExceeded:
				nomsg++
				c.sc.Log.Debug("context deadline exceeded")
				return nil

			case ErrNoMessage:
				nomsg++
				c.sc.Log.Debug("no message")
				return nil

			case io.EOF:
				c.sc.Log.Error("EOF")
				return io.EOF
			default:
				failures++
				c.Failure()
				c.sc.Log.Error("Unknown error: %v", err)
				return err
			}
		}
	)

	id := c.ID()

	// Log run statistics periodically until asked to stop
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			c.sc.Log.Info("IProcessor %s successes=%d failures=%d nomsg=%d", id, successes, failures, nomsg)
			if c.isStopping() {
				return
			}
		}
	}()

	// Process messages until asked to stop
	for !c.isStopping() {
		err := processNextMessage()
		if err != nil {
			return err
		}
	}

	return nil
}
