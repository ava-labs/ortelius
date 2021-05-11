package utils

import (
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/servicesctrl"
)

// ListenCloser listens for messages until it's asked to close
type ListenCloser interface {
	Listen() error
	Close() error
}

type ListenCloserFactory func(*servicesctrl.Control, cfg.Config, int, int) ListenCloser
