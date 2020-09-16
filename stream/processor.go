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
)

var (
	readTimeout  = 10 * time.Second
	writeTimeout = 10 * time.Second

	// ErrUnknownProcessorType is returned when encountering a client type with no
	// known implementation
	ErrUnknownProcessorType = errors.New("unknown processor type")
)

type ProcessorFactory func(cfg.Config, uint32, string, string, *logging.Log) (Processor, error)

// Processor handles writing and reading to/from the event stream
type Processor interface {
	ProcessNextMessage(context.Context, logging.Logger) error
	Close() error
}

// ProcessorManager reads or writes from/to the event stream backend
type ProcessorManager struct {
	conf    cfg.Config
	log     *logging.Log
	factory ProcessorFactory

	// Concurrency control
	workerWG *sync.WaitGroup
	quitCh   chan struct{}
	doneCh   chan struct{}
}

// NewProcessorManager creates a new *ProcessorManager ready for listening
func NewProcessorManager(conf cfg.Config, factory ProcessorFactory) (*ProcessorManager, error) {
	loggingConf, err := logging.DefaultConfig()
	if err != nil {
		return nil, err
	}
	loggingConf.Directory = conf.LogDirectory

	log, err := logging.New(loggingConf)
	if err != nil {
		return nil, err
	}

	return &ProcessorManager{
		conf:    conf,
		log:     log,
		factory: factory,

		workerWG: &sync.WaitGroup{},
		quitCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

// Listen sets a client to listen for and handle incoming messages
func (c *ProcessorManager) Listen() error {
	// Create a backend for each chain we want to watch and wait for them to exit
	workerManagerWG := &sync.WaitGroup{}
	for _, chainConfig := range c.conf.Chains {
		workerManagerWG.Add(1)
		go func(chainConfig cfg.Chain) {
			defer workerManagerWG.Done()
			c.log.Info("Started worker manager for chain %s", chainConfig.ID)

			// Keep running the worker until it exits without an error
			for err := c.runProcessor(chainConfig); err != nil; err = c.runProcessor(chainConfig) {
				c.log.Error("Error running worker: %s", err.Error())
				<-time.After(200 * time.Millisecond)
			}
			c.log.Info("Exiting worker manager for chain %s", chainConfig.ID)
		}(chainConfig)
	}

	// Wait for all workers to finish without an error
	workerManagerWG.Wait()
	c.log.Info("All workers stopped")
	close(c.doneCh)

	return nil
}

// Close tells the workers to shutdown and waits for them to all stop
func (c *ProcessorManager) Close() error {
	close(c.quitCh)
	<-c.doneCh
	return nil
}

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

	var err error
	for {
		err = c.runProcessorLoop(chainConfig)
		if err != io.EOF {
			break
		}
	}
	return err
}

func (c *ProcessorManager) runProcessorLoop(chainConfig cfg.Chain) error {
	// Create a backend to get messages from
	backend, err := c.factory(c.conf, c.conf.NetworkID, chainConfig.VMType, chainConfig.ID, c.log)
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
