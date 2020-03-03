package client

import (
	"github.com/ava-labs/gecko/utils/logging"

	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol/sub"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/consumers"
	"github.com/ava-labs/ortelius/producers"
	"github.com/ava-labs/ortelius/utils"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/ipc"
)

// Client an IPC client
type Client struct {
	log      logging.Logger
	sock     mangos.Socket
	dataType string
}

// Initialize sets up the client
func (c *Client) Initialize() error {
	var err error
	var conf logging.Config
	var log logging.Logger
	if conf, err = logging.DefaultConfig(); err != nil {
		return err
	}
	conf.Directory = cfg.Viper.GetString("logDirectory")
	if log, err = logging.New(conf); err != nil {
		return err
	}
	c.log = log
	c.dataType = cfg.Viper.GetString("kafka")
	return nil
}

// Listen sets a client to listen to the URL for a UNIX Domain socket IPC file
func (c *Client) Listen(clientType string) {
	var sock mangos.Socket
	var err error
	var msg []byte

	if sock, err = sub.NewSocket(); err != nil {
		utils.Die("can't get new sub socket: %s", err.Error())
	}

	if err = sock.Dial(cfg.Viper.GetString("ipcURL")); err != nil {
		utils.Die("can't dial on sub socket: %s", err.Error())
	}

	// Empty byte array effectively subscribes to everything
	err = sock.SetOption(mangos.OptionSubscribe, []byte(""))

	if err != nil {
		utils.Die("cannot subscribe: %s", err.Error())
	}
	dataType := cfg.Viper.GetString("dataType")
	switch clientType {
	case "consumer":
		for {
			con := consumers.Select(dataType)
			if msg, err = sock.Recv(); err != nil {
				utils.Die("Cannot recv: %s", err.Error())
			}
			if err := con.ReadMessage(msg); err != nil {
				c.log.Error(err.Error())
			}
		}
	case "producer":
		prod := producers.Select(dataType)
		prod.Initialize(c.log)
		for {
			if msg, err = sock.Recv(); err != nil {
				utils.Die("Cannot recv: %s", err.Error())
			}
			if err = prod.Produce(msg); err != nil {
				c.log.Error(err.Error())
			}
		}
	}
}
