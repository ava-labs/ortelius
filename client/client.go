package client

import (
	"errors"

	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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
	ErrUnknownDataType   = errors.New("unknown data type")
	ErrUnknownClientType = errors.New("unknown client type")
)

// Client an IPC client
type Client struct {
	kafkaConf kafka.ConfigMap
}

type clientBackend interface {
	Accept() error
	Close() error
}

// New creates a new *Client ready for use
func New(kafkaConf kafka.ConfigMap) *Client {
	return &Client{kafkaConf: kafkaConf}
}

// Listen sets a client to listen to the URL for a UNIX Domain socket IPC file
func (c *Client) Listen() error {
	var (
		// backend instance for the configured context
		backend    clientBackend
		clientType = cfg.Viper.GetString("context")
		dataType   = cfg.Viper.GetString("dataType")
	)

	// Create a logger for our client to use
	log, err := getLogger()
	if err != nil {
		return err
	}

	// Create a backend producer or consumer based on the config
	switch clientType {
	default:
		return ErrUnknownClientType
	case "consumer":
		backend, err = createConsumer(dataType, log, c.kafkaConf)
	case "producer":
		backend, err = createProducer(dataType, log, c.kafkaConf)
	}

	if err != nil {
		log.Error("Initialization error: %s", err.Error())
		return err
	}

	// Loop over the backend until it's finished
	for {
		if err := backend.Accept(); err != nil {
			log.Error("Accept error: %s", err.Error())
		}
	}
}

// getLogger returns a configured logger
func getLogger() (logging.Logger, error) {
	var err error
	var conf logging.Config
	if conf, err = logging.DefaultConfig(); err != nil {
		return nil, err
	}
	conf.Directory = cfg.Viper.GetString("logDirectory")
	return logging.New(conf)
}

func createConsumer(dataType string, log logging.Logger, kconf kafka.ConfigMap) (clientBackend, error) {
	c := consumers.Select(dataType)
	if c == nil {
		return nil, ErrUnknownDataType
	}
	return c, c.Initialize(log, kconf)
}

func createProducer(dataType string, log logging.Logger, kconf kafka.ConfigMap) (clientBackend, error) {
	sock, err := createIPCSocket()
	if err != nil {
		return nil, err
	}

	p := producers.Select(dataType)
	if p == nil {
		return nil, ErrUnknownDataType
	}
	return p, p.Initialize(log, kconf, sock)
}

// createIPCSocket creates a new socket connection to the configured IPC URL
func createIPCSocket() (protocol.Socket, error) {
	sock, err := sub.NewSocket()
	if err != nil {
		return nil, err
	}

	if err = sock.Dial(cfg.Viper.GetString("ipcURL")); err != nil {
		return nil, err
	}

	// Empty byte array effectively subscribes to everything
	if err = sock.SetOption(mangos.OptionSubscribe, []byte("")); err != nil {
		return nil, err
	}

	return sock, nil
}
