package controlwrap

import (
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/services/servicesconn"
)

type ControlWrap interface {
	DatabaseOnly() (*servicesconn.Connections, error)
	Logger() logging.Logger
}
