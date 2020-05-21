// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
)

func NewIndexerFactory() stream.ProcessorFactory {
	return stream.NewConsumerFactory(createIndexerConsumer)
}

func createIndexerConsumer(conf cfg.Config, networkID uint32, chainVM string, chainID string) (indexer services.Consumer, err error) {
	switch chainVM {
	case avm.VMName:
		indexer, err = avm.New(conf.Services, networkID, chainID)
	case pvm.VMName:
		indexer, err = pvm.New(conf.Services, networkID, chainID)
	default:
		return nil, stream.ErrUnknownVM
	}
	return indexer, err
}
