// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
)

var (
	processorFailureRetryInterval = 200 * time.Millisecond

	// ErrNoMessage is no message
	ErrNoMessage = errors.New("no message")
)

type ProcessorFactoryChainDB func(*servicesctrl.Control, cfg.Config, string, string) (ProcessorDB, error)
type ProcessorFactoryInstDB func(*servicesctrl.Control, cfg.Config) (ProcessorDB, error)

type ProcessorDB interface {
	Process(*utils.Connections, *db.TxPool) error
	Close() error
	ID() string
	Topic() []string
}

func UpdateTxPool(
	ctxTimeout time.Duration,
	conns *utils.Connections,
	persist db.Persist,
	txPool *db.TxPool,
	sc *servicesctrl.Control,
) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("update-tx-pool"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancelCtx()

	err := persist.InsertTxPool(ctx, sess, txPool)
	if err == nil {
		sc.Enqueue(txPool)
	}
	return err
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
