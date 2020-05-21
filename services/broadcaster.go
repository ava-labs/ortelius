// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"fmt"

	"github.com/ava-labs/ortelius/cfg"
)

type Broadcaster struct {
	chainConfig cfg.Chain
}

func NewBroadcaster(c cfg.Config, chainVM string, chainID string) *Broadcaster {
	return &Broadcaster{}
}

func (*Broadcaster) Name() string { return "broadcaster" }

func (*Broadcaster) Bootstrap() error { return nil }

func (*Broadcaster) Consume(c Consumable) error {
	fmt.Println("Record", c.ChainID(), c.ID(), c.Timestamp())
	return nil
}
