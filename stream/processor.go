// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

var (
	readTimeout  = 10 * time.Second
	writeTimeout = 10 * time.Second

	processorFailureRetryInterval = 200 * time.Millisecond

	// ErrUnknownProcessorType is returned when encountering a client type with no
	// known implementation
	ErrUnknownProcessorType = errors.New("unknown processor type")
)

// ProcessorFactory takes in configuration and returns a stream Processor
type ProcessorFactory func(cfg.Config, string, string) (Processor, error)

// Processor handles writing and reading to/from the event stream
type Processor interface {
	ProcessNextMessage(context.Context, logging.Logger) error
	Close() error
}

// ProcessorManager supervises the Processor lifecycle; it will use the given
// configuration and ProcessorFactory to keep a Processor active
type ProcessorManager struct {
	conf    cfg.Config
	log     logging.Logger
	factory ProcessorFactory
	conns   *services.Connections

	// Concurrency control
	quitCh chan struct{}
	doneCh chan struct{}
}

// NewProcessorManager creates a new *ProcessorManager ready for listening
func NewProcessorManager(conf cfg.Config, factory ProcessorFactory) (*ProcessorManager, error) {
	conns, err := services.NewConnectionsFromConfig(conf.Services)
	if err != nil {
		return nil, err
	}

	return &ProcessorManager{
		conf:  conf,
		conns: conns,

		// copy the logger from conns.
		log: conns.Logger(),

		factory: factory,

		quitCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}, nil
}

// Listen sets a client to listen for and handle incoming messages
func (c *ProcessorManager) Listen() error {
	// Create a backend for each chain we want to watch and wait for them to exit
	wg := &sync.WaitGroup{}
	wg.Add(len(c.conf.Chains))
	for _, chainConfig := range c.conf.Chains {
		go func(chainConfig cfg.Chain) {
			c.log.Info("Started worker manager for chain %s", chainConfig.ID)
			defer c.log.Info("Exiting worker manager for chain %s", chainConfig.ID)
			defer wg.Done()

			// Keep running the worker until we're asked to stop
			var err error
			for !c.isStopping() {
				err = c.runProcessor(chainConfig)

				// If there was an error we want to log it, and iff we are not stopping
				// we want to add a retry delay.
				if err != nil {
					c.log.Error("Error running worker: %s", err.Error())
				}
				if c.isStopping() {
					return
				}
				if err != nil {
					<-time.After(processorFailureRetryInterval)
				}
			}
		}(chainConfig)
	}

	// Wait for all workers to finish
	wg.Wait()
	c.log.Info("All workers stopped")
	close(c.doneCh)

	return nil
}

// Close tells the workers to shutdown and waits for them to all stop
func (c *ProcessorManager) Close() error {
	close(c.quitCh)
	<-c.doneCh
	c.conns.Close()
	return nil
}

// isStopping returns true iff quitCh has been signaled
func (c *ProcessorManager) isStopping() bool {
	select {
	case <-c.quitCh:
		return true
	default:
		return false
	}
}

// runProcessor starts the processing loop for the backend and closes it when
// finished
func (c *ProcessorManager) runProcessor(chainConfig cfg.Chain) error {
	if c.isStopping() {
		c.log.Info("Not starting worker for chain %s because we're stopping", chainConfig.ID)
		return nil
	}

	c.log.Info("Starting worker for chain %s", chainConfig.ID)
	defer c.log.Info("Exiting worker for chain %s", chainConfig.ID)

	// Create a backend to get messages from
	backend, err := c.factory(c.conf, chainConfig.VMType, chainConfig.ID)
	if err != nil {
		return err
	}
	defer backend.Close()

	// Create a closure that processes the next message from the backend
	var (
		ctx                context.Context
		cancelFn           context.CancelFunc
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			ctx, cancelFn = context.WithTimeout(context.Background(), readTimeout)
			defer cancelFn()

			err = backend.ProcessNextMessage(ctx, c.log)
			if err == nil {
				successes++
				return nil
			}

			switch err {
			// This error is expected when the upstream service isn't producing
			case context.DeadlineExceeded:
				nomsg++
				c.log.Debug("context deadline exceeded")
				return nil

			// These are always errors
			case kafka.RequestTimedOut:
				c.log.Debug("kafka timeout")
			case io.EOF:
				c.log.Error("EOF")
				return io.EOF
			default:
				c.log.Error("Unknown error: %s", err.Error())
			}

			failures++
			return nil
		}
	)

	// Log run statistics periodically until asked to stop
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			c.log.Info("IProcessor successes=%d failures=%d nomsg=%d", successes, failures, nomsg)
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
