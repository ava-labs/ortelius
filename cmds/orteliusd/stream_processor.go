package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ava-labs/gecko/utils/logging"
	"github.com/segmentio/kafka-go"
	"nanomsg.org/go/mangos/v2"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/ipc"
)

var (
	streamReadTimeout = 10 * time.Second

	// ErrUnknownStreamProcessorType is returned when encountering a client type with no
	// known implementation
	ErrUnknownStreamProcessorType = errors.New("unknown stream processor type")
)

type streamProcessorFactory func(cfg.ClientConfig, uint32, cfg.ChainConfig) (stream.Processor, error)

// StreamProcessorManager reads or writes from/to the event stream backend
type StreamProcessorManager struct {
	conf    cfg.ClientConfig
	log     *logging.Log
	factory streamProcessorFactory

	// Concurrency control
	workerWG *sync.WaitGroup
	quitCh   chan struct{}
	doneCh   chan struct{}
}

// newStreamProcessorManager creates a new *StreamProcessorManager ready for listening
func newStreamProcessorManager(conf cfg.ClientConfig, factory streamProcessorFactory) (*StreamProcessorManager, error) {
	log, err := logging.New(conf.Logging)
	if err != nil {
		return nil, err
	}

	return &StreamProcessorManager{
		conf:    conf,
		log:     log,
		factory: factory,

		workerWG: &sync.WaitGroup{},
		quitCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

// Listen sets a client to listen for and handle incoming messages
func (c *StreamProcessorManager) Listen() error {
	// Create a backend for each chain we want to watch and wait for them to exit
	workerManagerWG := &sync.WaitGroup{}
	for _, chainConfig := range c.conf.ChainsConfig {
		workerManagerWG.Add(1)
		go func(chainConfig cfg.ChainConfig) {
			defer workerManagerWG.Done()
			c.log.Info("Started worker manager for chain %s", chainConfig.ID)

			// Keep running the worker until it exits without an error
			for err := c.runWorker(chainConfig); err != nil; err = c.runWorker(chainConfig) {
				c.log.Error("Error running worker: %s", err.Error())
				<-time.After(10 * time.Second)
			}
			c.log.Info("Exiting worker manager for chain %s", chainConfig.ID)
		}(chainConfig)
	}

	// Wait for all workers to finish without an error
	workerManagerWG.Wait()
	c.log.Debug("All workers stopped")
	close(c.doneCh)

	return nil
}

// Close tells the workers to shutdown and waits for them to all stop
func (c *StreamProcessorManager) Close() error {
	close(c.quitCh)
	<-c.doneCh
	return nil
}

func (c *StreamProcessorManager) isStopping() bool {
	select {
	case <-c.quitCh:
		return true
	default:
		return false
	}
}

// runWorker starts the processing loop for the backend and closes it when
// finished
func (c *StreamProcessorManager) runWorker(chainConfig cfg.ChainConfig) error {
	if c.isStopping() {
		c.log.Info("Not starting worker for chain %s because we're stopping", chainConfig.ID)
		return nil
	}

	c.log.Info("Starting worker for chain %s", chainConfig.ID)
	defer c.log.Info("Exiting worker for chain %s", chainConfig.ID)

	// Create a backend to get messages from
	backend, err := c.factory(c.conf, c.conf.NetworkID, chainConfig)
	if err != nil {
		// panic(err)
		return err
	}

	// Create a closure that processes the next message from the backend
	var (
		msg      *stream.Message
		ctx      context.Context
		cancelFn context.CancelFunc

		processNextMessage = func() {
			ctx, cancelFn = context.WithTimeout(context.Background(), streamReadTimeout)
			defer cancelFn()

			msg, err = backend.ProcessNextMessage(ctx)

			switch err {
			case nil:
				c.log.Info("Processed message %s on chain %s", msg.ID(), msg.ChainID())
			case mangos.ErrRecvTimeout:
				c.log.Debug("IPC socket timeout")
			case kafka.RequestTimedOut:
				c.log.Debug("Kafka timeout")
			default:
				c.log.Error("Unknown error: %s", err.Error())
			}
		}
	)

	// Process messages until asked to stop
	for !c.isStopping() {
		processNextMessage()
	}
	return nil
}
