package client

import (
	"errors"
	"sync"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol"
	"nanomsg.org/go/mangos/v2/protocol/sub"

	"github.com/ava-labs/ortelius/cfg"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/ipc"
)

var (
	// ErrUnknownClientType is returned when encountering a client type with no
	// known implementation
	ErrUnknownClientType = errors.New("unknown client type")
)

// backend instance for the configured context
type backend interface {
	ProcessNextMessage() (*message, error)
	Close() error
}
type backendFactory func(*cfg.ClientConfig, uint32, ids.ID) (backend, error)

type message struct {
	id        ids.ID
	chainID   ids.ID
	body      []byte
	timestamp uint64
}

func (m *message) ID() ids.ID        { return m.id }
func (m *message) ChainID() ids.ID   { return m.chainID }
func (m *message) Body() []byte      { return m.body }
func (m *message) Timestamp() uint64 { return m.timestamp }

// Client a Kafka client; a producer or a consumer
type Client struct {
	conf    *cfg.ClientConfig
	factory backendFactory

	quitCh chan struct{}
	doneCh chan struct{}
}

// New creates a new *Client ready for listening
func New(conf *cfg.ClientConfig) (*Client, error) {
	c := &Client{
		conf:   conf,
		quitCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	switch conf.Context {
	case "consumer":
		c.factory = newConsumer
	case "producer":
		c.factory = newProducer
	default:
		return nil, ErrUnknownClientType
	}
	return c, nil
}

// Listen sets a client to listen for and handle incoming messages
func (c *Client) Listen() error {
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
func (c *Client) Close() {
	close(c.quitCh)
	<-c.doneCh
}

// startWorker starts the processing loop for the backend and closes it when
// finished
func startWorker(backend backend, log *logging.Log, wg *sync.WaitGroup, quitCh chan struct{}) {
	var err error
	var msg *message

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

// createIPCSocket creates a new socket connection to the configured IPC URL
func createIPCSocket(url string) (protocol.Socket, error) {
	// Create and open a connection to the IPC socket
	sock, err := sub.NewSocket()
	if err != nil {
		return nil, err
	}

	if err = sock.Dial(url); err != nil {
		return nil, err
	}

	// Subscribe to all topics
	if err = sock.SetOption(mangos.OptionSubscribe, []byte("")); err != nil {
		return nil, err
	}

	return sock, nil
}
