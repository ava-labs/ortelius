// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"context"
	"sync"

	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/cvm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
)

const (
	IndexerAVMName = "avm"
	IndexerPVMName = "pvm"
)

type ConsumerFactory func(uint32, string, string) (services.Consumer, error)

var IndexerConsumer = func(networkID uint32, chainVM string, chainID string) (indexer services.Consumer, err error) {
	switch chainVM {
	case IndexerAVMName:
		indexer, err = avm.NewWriter(networkID, chainID)
	case IndexerPVMName:
		indexer, err = pvm.NewWriter(networkID, chainID)
	default:
		return nil, stream.ErrUnknownVM
	}
	return indexer, err
}

var Indexer = stream.NewConsumerFactory(IndexerConsumer)

var IndexerConsensus = stream.NewConsumerConsensusFactory(func(networkID uint32, chainVM string, chainID string) (indexer services.Consumer, err error) {
	switch chainVM {
	case IndexerAVMName:
		indexer, err = avm.NewWriter(networkID, chainID)
	case IndexerPVMName:
		indexer, err = pvm.NewWriter(networkID, chainID)
	default:
		return nil, stream.ErrUnknownVM
	}
	return indexer, err
})

var IndexerConsumerCChain = func(networkID uint32, chainID string) (indexer services.ConsumerCChain, err error) {
	return cvm.NewWriter(networkID, chainID)
}

var IndexerCChain = stream.NewConsumerCChain

func Bootstrap(sc *services.Control, networkID uint32, chains cfg.Chains, factories []ConsumerFactory) error {
	conns, err := sc.DatabaseOnly()
	if err != nil {
		return err
	}
	defer func() {
		_ = conns.Close()
	}()

	ctx := context.Background()

	errs := avlancheGoUtils.AtomicInterface{}

	wg := sync.WaitGroup{}
	for _, chain := range chains {
		for _, factory := range factories {
			bootstrapfactory, err := factory(networkID, chain.VMType, chain.ID)
			if err != nil {
				return err
			}
			sc.Log.Info("bootstrap %d vm %s chain %s", networkID, chain.VMType, chain.ID)
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = bootstrapfactory.Bootstrap(ctx, conns, sc.Persist)
				if err != nil {
					errs.SetValue(err)
				}
				sc.Log.Info("bootstrap complete %d vm %s chain %s", networkID, chain.VMType, chain.ID)
			}()
		}
	}

	wg.Wait()

	if errs.GetValue() != nil {
		return errs.GetValue().(error)
	}

	return nil
}
