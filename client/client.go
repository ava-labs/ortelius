package client

import (
	"errors"

	"github.com/ava-labs/gecko/utils/logging"
	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol"
	"nanomsg.org/go/mangos/v2/protocol/sub"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/consumers"
	"github.com/ava-labs/ortelius/producers"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/ipc"
)

var (
	// ErrUnknownDataType is returned when encountering a data type with no known
	// backend client
	ErrUnknownDataType = errors.New("unknown data type")

	// ErrUnknownClientType is returned when encountering a client type with no
	// known implementation
	ErrUnknownClientType = errors.New("unknown client type")
)

// backend represents the backend producer or consumer for the Client
type backend interface {
	ProcessNextMessage() error
	Close() error
}

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
	var backend backend

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
		backend, err = createConsumer(c.conf, log)
	case "producer":
		backend, err = createProducer(c.conf, log)
	}

	if err != nil {
		log.Error("Initialization error: %s", err.Error())
		return err
	}

	// Loop over the backend until it's finished
	for {
		if err := backend.ProcessNextMessage(); err != nil {
			log.Error("Accept error: %s", err.Error())
		}
	}
}

// createConsumer returns a new consumer for the configured data type
func createConsumer(conf *cfg.ClientConfig, log logging.Logger) (backend, error) {
	c := consumers.Select(conf.DataType)
	if c == nil {
		return nil, ErrUnknownDataType
	}
	return c, c.Initialize(log, conf)
}

// createProducer returns a new producer for the configured data type
func createProducer(conf *cfg.ClientConfig, log logging.Logger) (backend, error) {
	sock, err := createIPCSocket(conf.IPCURL)
	if err != nil {
		return nil, err
	}

	p := producers.Select(conf.DataType)
	if p == nil {
		return nil, ErrUnknownDataType
	}
	return p, p.Initialize(log, conf, sock)
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
