package client

import (
	"errors"

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

// Client a Kafka client; a producer or a consumer
type Client struct {
	conf *cfg.ClientConfig
}

// New creates a new *Client ready for listening
func New(conf *cfg.ClientConfig) *Client {
	return &Client{conf: conf}
}

// Listen sets a client to listen for and handle incoming messages
func (c *Client) Listen() error {
	// backend instance for the configured context
	var backend interface {
		ProcessNextMessage() (*message, error)
		Close() error
	}

	// Create a logger for out backend to use
	log, err := logging.New(c.conf.Logging)
	if err != nil {
		return err
	}

	// Create a backend producer or consumer based on the config
	switch c.conf.Context {
	default:
		return ErrUnknownClientType
	case "consumer":
		backend, err = newConsumer(c.conf)
	case "producer":
		backend, err = newProducer(c.conf)
	}
	if err != nil {
		log.Error("Initialization error: %s", err.Error())
		return err
	}
	log.Info("Initialized %s with chainID=%s", c.conf.Context, c.conf.ChainID)

	// Set it to close when we're done
	defer func() {
		if err := backend.Close(); err != nil {
			log.Error("Error closing backend: %s", err.Error())
		}
	}()

	// Loop over the backend until it's finished
	var msg *message
	for {
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
