// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"errors"
	"time"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"
)

var (
	processorFailureRetryInterval = 200 * time.Millisecond

	// ErrNoMessage is no message
	ErrNoMessage = errors.New("no message")
)

type ProcessorFactoryChainDB func(*services.Control, cfg.Config, string, string) (ProcessorDB, error)
type ProcessorFactoryInstDB func(*services.Control, cfg.Config) (ProcessorDB, error)

type ProcessorDB interface {
	Process(*services.Connections, *services.TxPool) error
	Close() error
	ID() string
	Topic() []string
}
