package main

import (
	"errors"
	"sync"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/ipc"
)

var (
	// ErrUnknownStreamProcessorType is returned when encountering a client type with no
	// known implementation
	ErrUnknownStreamProcessorType = errors.New("unknown stream processor type")
)

type streamProcessorFactory func(*cfg.ClientConfig, uint32, ids.ID) (stream.Processor, error)

// StreamProcessorManager reads or writes from/to the event stream backend
type StreamProcessorManager struct {
	conf    *cfg.ClientConfig
	factory streamProcessorFactory
	quitCh  chan struct{}
	doneCh  chan struct{}
}

// newStreamProcessorManager creates a new *StreamProcessorManager ready for listening
func newStreamProcessorManager(conf *cfg.ClientConfig, factory streamProcessorFactory) *StreamProcessorManager {
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
			log.Error("Initialization error: %s", err.Error())
			return err
		}
		log.Info("Initialized %s with chainID=%s", c.conf.Context, chainID)
		go startWorker(backend, log, wg, c.quitCh)
	}
	wg.Wait()
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
	var err error
	var msg *stream.Message

	for {
		// Handle shutdown when requested
		select {
		case <-quitCh:
			if err := backend.Close(); err != nil {
				log.Error("Error closing backend: %s", err.Error())
			}
			wg.Done()
			return
		default:
		}

		// Process next message
		msg, err = backend.ProcessNextMessage()
		if err != nil {
			log.Error("Accept error: %s", err.Error())
			continue
		}
		log.Info("Processed message %s on chain %s", msg.ID(), msg.ChainID())
	}
}
