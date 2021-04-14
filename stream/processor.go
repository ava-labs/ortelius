// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"errors"
	"strings"
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

func UpdateTxPool(ctxTimeout time.Duration, conns *services.Connections, persist services.Persist, txPool *services.TxPool) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("update-tx-pool"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancelCtx()

	return persist.InsertTxPool(ctx, sess, txPool)
}

func TrimNL(msg string) string {
	oldmsg := msg
	for {
		msg = strings.TrimPrefix(msg, "\n")
		if msg == oldmsg {
			break
		}
		oldmsg = msg
	}
	oldmsg = msg
	for {
		msg = strings.TrimSuffix(msg, "\n")
		if msg == oldmsg {
			break
		}
		oldmsg = msg
	}
	return msg
}
