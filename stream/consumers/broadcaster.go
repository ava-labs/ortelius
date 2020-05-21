// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/stream"
)

func NewBroadcasterFactory() stream.ProcessorFactory {
	return stream.NewConsumerFactory(createBroadcasterConsumer)
}

func createBroadcasterConsumer(c cfg.Config, _ uint32, chainVM string, chainID string) (indexer services.Consumer, err error) {
	return services.NewBroadcaster(c, chainVM, chainID), nil
}
