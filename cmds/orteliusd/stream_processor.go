package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ava-labs/gecko/ids"
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

type streamProcessorFactory func(cfg.ClientConfig, uint32, ids.ID) (stream.Processor, error)

// StreamProcessorManager reads or writes from/to the event stream backend
type StreamProcessorManager struct {
	conf    cfg.ClientConfig
	factory streamProcessorFactory
	quitCh  chan struct{}
	doneCh  chan struct{}
}

// newStreamProcessorManager creates a new *StreamProcessorManager ready for listening
func newStreamProcessorManager(conf cfg.ClientConfig, factory streamProcessorFactory) *StreamProcessorManager {
	return &StreamProcessorManager{
		conf:    conf,
		factory: factory,
		quitCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Listen sets a client to listen for and handle incoming messages
func (c *StreamProcessorManager) Listen() error {
	// Create a logger for our backends to use
	log, err := logging.New(c.conf.Logging)
	if err != nil {
		return err
	}

	// Create a backend for each chain we want to watch and wait for them to exit
	wg := &sync.WaitGroup{}
	wg.Add(len(c.conf.ChainsConfig))
	for chainID := range c.conf.ChainsConfig {
		backend, err := c.factory(c.conf, c.conf.NetworkID, chainID)
		if err != nil {
			return err
		}
		log.Info("Initialized %s with chainID=%s", c.conf.Context, chainID)
		go startWorker(backend, log, wg, c.quitCh)
	}
	wg.Wait()
	log.Debug("All workers stopped")
	close(c.doneCh)

	return nil
}

// Close tells the workers to shutdown and waits for them to all stop
func (c *StreamProcessorManager) Close() error {
	close(c.quitCh)
	<-c.doneCh
	return nil
}

// startWorker starts the processing loop for the backend and closes it when
// finished
func startWorker(backend stream.Processor, log *logging.Log, wg *sync.WaitGroup, quitCh chan struct{}) {
	var (
		err      error
		msg      *stream.Message
		ctx      context.Context
		cancelFn context.CancelFunc

		processNextMessage = func() {
			ctx, cancelFn = context.WithTimeout(context.Background(), streamReadTimeout)
			defer cancelFn()

			msg, err = backend.ProcessNextMessage(ctx)

			switch err {
			case nil:
				log.Info("Processed message %s on chain %s", msg.ID(), msg.ChainID())
			case mangos.ErrRecvTimeout:
				log.Debug("IPC socket timeout")
			case kafka.RequestTimedOut:
				log.Debug("Kafka timeout")
			default:
				log.Error("Unknown error:", err.Error())
			}
		}
	)

	for {
		select {

		// If a shutdown has been requested then stop now
		case <-quitCh:
			log.Debug("Worker shutting down")
			if err := backend.Close(); err != nil {
				log.Error("Error closing backend: %s", err.Error())
			}
			wg.Done()
			return

		// If no shutdown is requested then try to process the next message
		default:
			processNextMessage()
		}
	}
}
