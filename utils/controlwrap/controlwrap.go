package controlwrap

import (
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/services/servicesgenesis"
)

type ControlWrap interface {
	DatabaseOnly() (*servicesconn.Connections, error)
	Logger() logging.Logger
	Genesis() *servicesgenesis.GenesisContainer
}
