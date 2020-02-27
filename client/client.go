package client

import (
	"github.com/ava-labs/gecko/utils/logging"
	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol/sub"

	"github.com/ava-labs/ortelius/producers"
	"github.com/ava-labs/ortelius/utils"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/ipc"
)

// Client an IPC client
type Client struct {
	log        logging.Logger
	sock       mangos.Socket
	isConsumer bool
}

// Initialize sets up the client
func (c *Client) Initialize(logdir string, isConsumer bool) error {
	var err error
	var conf logging.Config
	var log logging.Logger
	if conf, err = logging.DefaultConfig(); err != nil {
		return err
	}
	conf.Directory = logdir
	if log, err = logging.New(conf); err != nil {
		return err
	}
	c.log = log
	c.isConsumer = isConsumer
	return nil
}

// Listen sets a client to listen to the URL for a UNIX Domain socket IPC file
func (c *Client) Listen(topic string, url string, dataType string, filter string) {
	var sock mangos.Socket
	var err error
	var msg []byte

	if sock, err = sub.NewSocket(); err != nil {
		utils.Die("can't get new sub socket: %s", err.Error())
	}

	if err = sock.Dial(url); err != nil {
		utils.Die("can't dial on sub socket: %s", err.Error())
	}

	// Empty byte array effectively subscribes to everything
	err = sock.SetOption(mangos.OptionSubscribe, []byte(""))

	if err != nil {
		utils.Die("cannot subscribe: %s", err.Error())
	}
	switch c.isConsumer {
	case true:
		for {
			if msg, err = sock.Recv(); err != nil {
				utils.Die("Cannot recv: %s", err.Error())
			}
			if err := c.consumerMsg(dataType, filter, msg); err != nil {
				c.log.Error(err.Error())
			}
		}
	case false:
		p = producers.Select()
		for {
			if msg, err = sock.Recv(); err != nil {
				utils.Die("Cannot recv: %s", err.Error())
			}
			if err = c.producerMsg(filter, msg); err != nil {
				c.log.Error(err.Error())
			}
		}
	}
}

func (c *Client) producerMsg(dataType string, filter string, msg []byte) error {
	if dataType == "avm" {
		// AVM producer
	}
	return nil
}

func (c *Client) consumerMsg(dataType string, filter string, msg []byte) error {
	if dataType == "avm" {
		// AVM consumer
	}
	return nil
}
