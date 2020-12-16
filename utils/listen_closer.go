package utils

import "github.com/ava-labs/ortelius/cfg"

// ListenCloser listens for messages until it's asked to close
type ListenCloser interface {
	Listen() error
	Close() error
}

type ListenCloserFactory func(cfg.Config) ListenCloser
