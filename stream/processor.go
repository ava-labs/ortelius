// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/ava-labs/ortelius/services"

	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/cfg"
)

var (
	processorFailureRetryInterval = 200 * time.Millisecond

	// ErrUnknownProcessorType is returned when encountering a client type with no
	// known implementation
	ErrUnknownProcessorType = errors.New("unknown processor type")

	// ErrNoMessage is no message
	ErrNoMessage = errors.New("no message")
)

// ProcessorFactory takes in configuration and returns a stream Processor
type ProcessorFactory func(*services.Control, cfg.Config, string, string) (Processor, error)

// Processor handles writing and reading to/from the event stream
type Processor interface {
	ProcessNextMessage() error
	Close() error
	Failure()
	Success()
	ID() string
}

// ProcessorManager supervises the Processor lifecycle; it will use the given
// configuration and ProcessorFactory to keep a Processor active
type ProcessorManager struct {
	conf    cfg.Config
	sc      *services.Control
	factory ProcessorFactory

	// Concurrency control
	quitCh chan struct{}
	doneCh chan struct{}
}

// NewProcessorManager creates a new *ProcessorManager ready for listening
func NewProcessorManager(sc *services.Control, conf cfg.Config, factory ProcessorFactory) *ProcessorManager {
	return &ProcessorManager{
		conf: conf,
		sc:   sc,

		factory: factory,

		quitCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Listen sets a client to listen for and handle incoming messages
func (c *ProcessorManager) Listen() error {
	// Create a backend for each chain we want to watch and wait for them to exit
	wg := &sync.WaitGroup{}
	wg.Add(len(c.conf.Chains))
	for _, chainConfig := range c.conf.Chains {
		go func(chainConfig cfg.Chain) {
			c.sc.Log.Info("Started worker manager for chain %s", chainConfig.ID)
			defer c.sc.Log.Info("Exiting worker manager for chain %s", chainConfig.ID)
			defer wg.Done()

			// Keep running the worker until we're asked to stop
			var err error
			for !c.isStopping() {
				err = c.runProcessor(chainConfig)

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
		}(chainConfig)
	}

	// Wait for all workers to finish
	wg.Wait()
	c.sc.Log.Info("All workers stopped")
	close(c.doneCh)

	return nil
}

// Close tells the workers to shutdown and waits for them to all stop
func (c *ProcessorManager) Close() error {
	close(c.quitCh)
	<-c.doneCh
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
		c.sc.Log.Info("Not starting worker for chain %s because we're stopping", chainConfig.ID)
		return nil
	}

	c.sc.Log.Info("Starting worker for chain %s", chainConfig.ID)
	defer c.sc.Log.Info("Exiting worker for chain %s", chainConfig.ID)

	// Create a backend to get messages from
	backend, err := c.factory(c.sc, c.conf, chainConfig.VMType, chainConfig.ID)
	if err != nil {
		return err
	}
	defer backend.Close()

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err = backend.ProcessNextMessage()
			if err == nil {
				successes++
				backend.Success()
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

			// These are always errors
			case kafka.RequestTimedOut:
				c.sc.Log.Debug("kafka timeout")
			case io.EOF:
				c.sc.Log.Error("EOF")
				return io.EOF
			default:
				backend.Failure()
				c.sc.Log.Error("Unknown error: %s", err.Error())
			}

			failures++
			return nil
		}
	)

	id := backend.ID()

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
