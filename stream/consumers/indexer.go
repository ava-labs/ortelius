// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
)

const (
	IndexerAVMName = "avm"
	IndexerPVMName = "pvm"
)

var Indexer = stream.NewConsumerFactory(func(conns *services.Connections, networkID uint32, chainVM string, chainID string) (indexer services.Consumer, err error) {
	switch chainVM {
	case IndexerAVMName:
		indexer, err = avm.NewWriter(conns, networkID, chainID)
	case IndexerPVMName:
		indexer, err = pvm.NewWriter(conns, networkID, chainID)
	default:
		return nil, stream.ErrUnknownVM
	}
	return indexer, err
})

var IndexerConsensus = stream.NewConsumerConsensusFactory(func(conns *services.Connections, networkID uint32, chainVM string, chainID string) (indexer services.Consumer, err error) {
	switch chainVM {
	case IndexerAVMName:
		indexer, err = avm.NewWriter(conns, networkID, chainID)
	case IndexerPVMName:
		indexer, err = pvm.NewWriter(conns, networkID, chainID)
	default:
		return nil, stream.ErrUnknownVM
	}
	return indexer, err
})

var IndexerCChain = stream.NewConsumerCChain
