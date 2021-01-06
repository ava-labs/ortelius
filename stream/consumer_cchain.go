// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

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

		topicName := fmt.Sprintf("%d-%s-cchain", conf.NetworkID, conf.CchainID)

		// Create reader for the topic
		c.reader = kafka.NewReader(kafka.ReaderConfig{
			Topic:       topicName,
			Brokers:     conf.Kafka.Brokers,
			GroupID:     conf.Consumer.GroupName,
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

	return c.Consume(msg)
}

func (c *ConsumerCChain) Consume(msg services.Consumable) error {
	block, err := cblock.Unmarshal(msg.Body())
	if err != nil {
		return err
	}

	if len(block.BlockExtraData) == 0 {
		return nil
	}

	collectors := metrics.NewCollectors(
		metrics.NewCounterIncCollect(c.metricProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(c.metricProcessMillisCounterKey),
	)
	defer func() {
		err := collectors.Collect()
		if err != nil {
			c.sc.Log.Error("collectors.Collect: %s", err)
		}
	}()

	id := hashing.ComputeHash256(block.BlockExtraData)
	nmsg := NewMessage(string(id), msg.ChainID(), block.BlockExtraData, msg.Timestamp())

	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()

	if err = c.consumer.Consume(ctx, nmsg, &block.Header); err != nil {
		collectors.Error()
		c.sc.Log.Error("consumer.Consume: %s %v", block.Header.Number.String(), err)
		return err
	}

	return nil
}

func (c *ConsumerCChain) nextMessage() (*Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()

	return c.getNextMessage(ctx)
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *ConsumerCChain) getNextMessage(ctx context.Context) (*Message, error) {
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
		chainID:   c.conf.CchainID,
		body:      msg.Value,
		id:        id.String(),
		timestamp: msg.Time.UTC().Unix(),
	}, nil
}

func (c *ConsumerCChain) Failure() {
	err := metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	if err != nil {
		c.sc.Log.Error("prometheus.CounterInc %s", err)
	}
}

func (c *ConsumerCChain) Success() {
	err := metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	if err != nil {
		c.sc.Log.Error("prometheus.CounterInc %s", err)
	}
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
	errs := wrappers.Errs{}
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

	err := c.init()
	if err != nil {
		return err
	}

	defer func() {
		err := c.processorClose()
		if err != nil {
			c.sc.Log.Warn("Stopping worker for cchain %w", err)
		}
	}()

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := c.ProcessNextMessage()
			if err == nil {
				successes++
				c.Success()
				return nil
			}

			switch err {
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
				c.Failure()
				c.sc.Log.Error("Unknown error: %s", err.Error())
			}

			failures++
			return nil
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
		if err == io.EOF && !c.isStopping() {
			return err
		}
	}

	return nil
}
