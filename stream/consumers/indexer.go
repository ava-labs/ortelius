// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"context"
	"sync"
	"time"

	"github.com/ava-labs/ortelius/utils"

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

var Indexer = stream.NewConsumerFactory(IndexerConsumer, stream.EventTypeDecisions)
var IndexerConsensus = stream.NewConsumerFactory(IndexerConsumer, stream.EventTypeConsensus)

var IndexerConsumerCChain = func(networkID uint32, chainID string) (indexer services.ConsumerCChain, err error) {
	return cvm.NewWriter(networkID, chainID)
}

var IndexerCChain = stream.NewConsumerCChain

type ConsumerDBFactory func(uint32, string, string) (stream.ProcessorFactoryDB, error)

var IndexerDB = stream.NewConsumerDBFactory(IndexerConsumer, stream.EventTypeDecisions)
var IndexerConsensusDB = stream.NewConsumerDBFactory(IndexerConsumer, stream.EventTypeConsensus)

var IndexerCChainDB = stream.NewConsumerCChainDB

func Bootstrap(sc *services.Control, networkID uint32, chains cfg.Chains, factories []ConsumerFactory) error {
	if sc.IsDisableBootstrap {
		return nil
	}

	conns, err := sc.DatabaseOnly()
	if err != nil {
		return err
	}
	defer func() {
		_ = conns.Close()
	}()

	persist := services.NewPersist()
	ctx := context.Background()
	job := conns.QuietStream().NewJob("bootstrap-key-value")
	sess := conns.DB().NewSessionForEventReceiver(job)

	bootstrapValue := "true"

	// check if we have bootstrapped..
	keyValueStore := &services.KeyValueStore{
		K: utils.KeyValueBootstrap,
	}
	keyValueStore, _ = persist.QueryKeyValueStore(ctx, sess, keyValueStore)
	if keyValueStore.V == bootstrapValue {
		sc.Log.Info("skipping bootstrap")
		return nil
	}

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

	// write a complete row.
	keyValueStore = &services.KeyValueStore{
		K: utils.KeyValueBootstrap,
		V: bootstrapValue,
	}
	return persist.InsertKeyValueStore(ctx, sess, keyValueStore)
}

type IndexerFactoryControl struct {
	msgChan chan *services.TxPool
	sc      *services.Control
	doneCh  chan struct{}
	fsm     map[string]stream.ProcessorDB
	errs    avlancheGoUtils.AtomicInterface
}

func (c *IndexerFactoryControl) handleTxPool(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	updateStatus := func(txPoll *services.TxPool) error {
		job := conns.QuietStream().NewJob("update-txpoll-status")
		sess := conns.DB().NewSessionForEventReceiver(job)
		ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
		defer cancelFn()
		return c.sc.Persist.UpdateTxPoolStatus(ctx, sess, txPoll)
	}

	for {
		select {
		case txd := <-c.msgChan:
			if p, ok := c.fsm[txd.Topic]; ok {
				err := p.Process(conns, txd)
				if err != nil {
					c.errs.SetValue(err)
					return
				}
				txd.Processed = 1
				err = updateStatus(txd)
				if err != nil {
					c.errs.SetValue(err)
					return
				}
			}
		case <-c.doneCh:
			return
		}
	}
}

func IndexerFactories(sc *services.Control, config *cfg.Config, factoriesDB []stream.ProcessorFactoryDB, factoriesCChainDB []stream.ProcessorFactoryCChainDB) error {
	ctrl := &IndexerFactoryControl{sc: sc}
	ctrl.fsm = make(map[string]stream.ProcessorDB)
	var topicNames []string

	for _, factory := range factoriesDB {
		for _, chainConfig := range config.Chains {
			f, err := factory(sc, *config, chainConfig.VMType, chainConfig.ID)
			if err != nil {
				return err
			}
			for _, topic := range f.Topic() {
				ctrl.fsm[topic] = f
				topicNames = append(topicNames, topic)
			}
		}
	}
	for _, factory := range factoriesCChainDB {
		f, err := factory(sc, *config)
		if err != nil {
			return err
		}
		for _, topic := range f.Topic() {
			ctrl.fsm[topic] = f
			topicNames = append(topicNames, topic)
		}
	}

	conns, err := sc.DatabaseOnly()
	if err != nil {
		return err
	}
	defer func() {
		_ = conns.Close()
	}()

	ctrl.msgChan = make(chan *services.TxPool, 100)
	ctrl.doneCh = make(chan struct{})

	defer func() {
		close(ctrl.doneCh)
	}()

	for ipos := 0; ipos < 10; ipos++ {
		conns1, err := sc.DatabaseOnly()
		if err != nil {
			return err
		}
		go ctrl.handleTxPool(conns1)
	}

	job := conns.StreamDBDedup().NewJob("tx-poll")
	sess := conns.DB().NewSessionForEventReceiver(job)

	for {
		// ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
		// defer cancelFn()

		// var rowdata []*services.TxPool
		iterator, err := sess.Select(
			"id",
			"network_id",
			"chain_id",
			"msg_key",
			"serialization",
			"processed",
			"topic",
			"created_at",
		).From(services.TableTxPool).
			Where("processed=? and topic in ?", 0, topicNames).
			OrderAsc("topic").OrderAsc("processed").OrderAsc("created_at").
			Iterate()
		if err != nil {
			sc.Log.Warn("iter %v", err)
			continue
		}

		var icnt uint64
		for iterator.Next() {
			txp := &services.TxPool{}
			err = iterator.Scan(txp)
			if err != nil {
				sc.Log.Warn("scan %v", err)
				continue
			}
			icnt++
			ctrl.msgChan <- txp
			err = iterator.Err()
			if err != nil {
				sc.Log.Warn("err %v", err)
				continue
			}
		}
		if icnt == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
}
