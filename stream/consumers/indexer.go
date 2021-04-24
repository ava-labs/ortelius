// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
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

	MaximumRecordsRead = 10000
	MaxTheads          = 10
	MaxChanSize        = 1000
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

var IndexerConsumerCChain = func(networkID uint32, chainID string) (indexer services.ConsumerCChain, err error) {
	return cvm.NewWriter(networkID, chainID)
}

type ConsumerDBFactory func(uint32, string, string) (stream.ProcessorFactoryChainDB, error)

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

type IndexerFactoryContainer struct {
	txPool *services.TxPool
	errs   *avlancheGoUtils.AtomicInterface
}

type IndexerFactoryControl struct {
	sc        *services.Control
	fsm       map[string]stream.ProcessorDB
	msgChan   chan *IndexerFactoryContainer
	doneCh    chan struct{}
	msgChanSz int64
}

func (c *IndexerFactoryControl) updateTxPollStatus(conns *services.Connections, txPoll *services.TxPool) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.QuietStream().NewJob("update-txpoll-status"))
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.sc.Persist.UpdateTxPoolStatus(ctx, sess, txPoll)
}

func (c *IndexerFactoryControl) handleTxPool(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	for {
		select {
		case txd := <-c.msgChan:
			atomic.AddInt64(&c.msgChanSz, -1)
			if txd.errs != nil && txd.errs.GetValue() != nil {
				continue
			}
			if p, ok := c.fsm[txd.txPool.Topic]; ok {
				err := p.Process(conns, txd.txPool)
				if err != nil {
					if txd.errs != nil {
						txd.errs.SetValue(err)
					}
					continue
				}
				txd.txPool.Processed = 1
				err = c.updateTxPollStatus(conns, txd.txPool)
				if err != nil {
					if txd.errs != nil {
						txd.errs.SetValue(err)
					}
					continue
				}
				c.sc.SizedList.Add(txd.txPool.ID)
			}
		case <-c.doneCh:
			return
		}
	}
}

func IndexerFactories(
	sc *services.Control,
	config *cfg.Config,
	factoriesChainDB []stream.ProcessorFactoryChainDB,
	factoriesInstDB []stream.ProcessorFactoryInstDB,
	wg *sync.WaitGroup,
	runningControl utils.Running,
) error {
	ctrl := &IndexerFactoryControl{sc: sc}
	ctrl.fsm = make(map[string]stream.ProcessorDB)
	var topicNames []string

	for _, factory := range factoriesChainDB {
		for _, chainConfig := range config.Chains {
			f, err := factory(sc, *config, chainConfig.VMType, chainConfig.ID)
			if err != nil {
				return err
			}
			for _, topic := range f.Topic() {
				if _, ok := ctrl.fsm[topic]; ok {
					return fmt.Errorf("duplicate topic %v", topic)
				}
				ctrl.fsm[topic] = f
				topicNames = append(topicNames, topic)
			}
		}
	}
	for _, factory := range factoriesInstDB {
		f, err := factory(sc, *config)
		if err != nil {
			return err
		}
		for _, topic := range f.Topic() {
			if _, ok := ctrl.fsm[topic]; ok {
				return fmt.Errorf("duplicate topic %v", topic)
			}
			ctrl.fsm[topic] = f
			topicNames = append(topicNames, topic)
		}
	}

	conns, err := sc.DatabaseOnly()
	if err != nil {
		return err
	}

	ctrl.msgChan = make(chan *IndexerFactoryContainer, MaxChanSize)
	ctrl.doneCh = make(chan struct{})

	for ipos := 0; ipos < MaxTheads; ipos++ {
		conns1, err := sc.DatabaseOnly()
		if err != nil {
			_ = conns.Close()
			close(ctrl.doneCh)
			return err
		}
		go ctrl.handleTxPool(conns1)
	}

	wg.Add(1)
	go func() {
		wg.Done()

		for {
			select {
			case txPool := <-sc.LocalTxPool:
				ctrl.msgChan <- &IndexerFactoryContainer{txPool: txPool, errs: nil}
				atomic.AddInt64(&ctrl.msgChanSz, 1)
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			close(ctrl.doneCh)
			_ = conns.Close()
		}()
		for !runningControl.IsStopped() {
			sess := conns.DB().NewSessionForEventReceiver(conns.QuietStream().NewJob("tx-poll"))
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
				OrderAsc("processed").OrderAsc("created_at").
				Iterate()
			if err != nil {
				sc.Log.Warn("iter %v", err)
				time.Sleep(250 * time.Millisecond)
				continue
			}

			errs := &avlancheGoUtils.AtomicInterface{}

			var readMessages uint64

			for iterator.Next() {
				if errs.GetValue() != nil {
					break
				}

				if readMessages > MaximumRecordsRead {
					break
				}

				err = iterator.Err()
				if err != nil {
					if err != io.EOF {
						sc.Log.Error("iterator err %v", err)
					}
					break
				}

				txp := &services.TxPool{}
				err = iterator.Scan(txp)
				if err != nil {
					sc.Log.Error("scan %v", err)
					break
				}
				readMessages++

				// skip previously processed
				if sc.SizedList.Exists(txp.ID) {
					continue
				}

				ctrl.msgChan <- &IndexerFactoryContainer{txPool: txp, errs: errs}
				atomic.AddInt64(&ctrl.msgChanSz, 1)
			}

			for atomic.LoadInt64(&ctrl.msgChanSz) > 0 {
				time.Sleep(1 * time.Millisecond)
			}

			if errs.GetValue() != nil {
				err := errs.GetValue().(error)
				sc.Log.Error("processing err %v", err)
				time.Sleep(250 * time.Millisecond)
				continue
			}

			if readMessages == 0 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	return nil
}
