package replay

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync/atomic"
	"time"

	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/ortelius/services/indexes/cvm"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/avalanchego/ids"
	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
	"github.com/ava-labs/ortelius/stream/consumers"
)

type DBReplay interface {
	Start() error
}

func NewDB(sc *services.Control, config *cfg.Config, replayqueuesize int, replayqueuethreads int) Replay {
	return &dbReplay{
		sc:           sc,
		config:       config,
		counterAdded: utils.NewCounterID(),
		counterWaits: utils.NewCounterID(),
		queueSize:    replayqueuesize,
		queueTheads:  replayqueuethreads,
	}
}

type dbReplay struct {
	errs   *avlancheGoUtils.AtomicInterface
	sc     *services.Control
	config *cfg.Config
	conns  *services.Connections

	counterAdded *utils.CounterID
	counterWaits *utils.CounterID

	queueSize   int
	queueTheads int

	persist services.Persist
}

func (replay *dbReplay) Start() error {
	cfg.PerformUpdates = true
	replay.persist = services.NewPersist()

	replay.errs = &avlancheGoUtils.AtomicInterface{}

	worker := utils.NewWorker(replay.queueSize, replay.queueTheads, replay.workerProcessor())

	waitGroup := new(int64)

	conns, err := replay.sc.DatabaseOnly()
	if err != nil {
		return err
	}

	replay.conns = conns

	for _, chainID := range replay.config.Chains {
		err := replay.handleReader(chainID, waitGroup, worker, conns)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return err
		}
	}

	err = replay.handleCReader(replay.config.CchainID, waitGroup, worker)
	if err != nil {
		log.Fatalln("reader failed", replay.config.CchainID, ":", err.Error())
		return err
	}

	timeLog := time.Now()

	logemit := func(waitGroupCnt int64) {
		type CounterValues struct {
			Added int64
			Waits int64
		}

		ctot := make(map[string]*CounterValues)
		countersValues := replay.counterAdded.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Added = countersValues[cnter]
		}
		countersValues = replay.counterWaits.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Waits = countersValues[cnter]
		}

		replay.sc.Log.Info("wgc: %d, jobs: %d", waitGroupCnt, worker.JobCnt())

		var sortedcnters []string
		for cnter := range ctot {
			sortedcnters = append(sortedcnters, cnter)
		}
		sort.Strings(sortedcnters)
		for _, cnter := range sortedcnters {
			if ctot[cnter].Waits != 0 {
				newlogline := fmt.Sprintf("key:%s add:%d wait:%d", cnter, ctot[cnter].Added, ctot[cnter].Waits)
				replay.sc.Log.Info(newlogline)
			}
		}
	}

	var waitGroupCnt int64
	for {
		waitGroupCnt = atomic.LoadInt64(waitGroup)
		if waitGroupCnt == 0 && worker.JobCnt() == 0 {
			break
		}

		if time.Since(timeLog).Seconds() > 30 {
			timeLog = time.Now()
			logemit(waitGroupCnt)
		}

		time.Sleep(time.Second)
	}

	logemit(waitGroupCnt)

	if replay.errs.GetValue() != nil {
		replay.sc.Log.Error("replay failed %v", replay.errs.GetValue().(error))
		return replay.errs.GetValue().(error)
	}

	return nil
}

func (replay *dbReplay) handleCReader(chain string, waitGroup *int64, worker utils.Worker) error {
	writer, err := cvm.NewWriter(replay.config.NetworkID, chain)
	if err != nil {
		return err
	}

	err = replay.startCchain(chain, waitGroup, worker, writer)
	if err != nil {
		return err
	}

	return nil
}

func (replay *dbReplay) handleReader(chain cfg.Chain, waitGroup *int64, worker utils.Worker, conns *services.Connections) error {
	var err error
	var writer services.Consumer
	switch chain.VMType {
	case consumers.IndexerAVMName:
		writer, err = avm.NewWriter(replay.config.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	case consumers.IndexerPVMName:
		writer, err = pvm.NewWriter(replay.config.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown vmtype")
	}

	{
		tn := fmt.Sprintf("%d-%s", replay.config.NetworkID, chain.ID)
		ctx := context.Background()
		replay.sc.Log.Info("replay for topic %s bootstrap start", tn)
		err := writer.Bootstrap(ctx, conns, replay.persist)
		replay.sc.Log.Info("replay for topic %s bootstrap end %v", tn, err)
		if err != nil {
			replay.errs.SetValue(err)
			return err
		}
	}

	err = replay.startDecision(chain, waitGroup, worker, writer)
	if err != nil {
		return err
	}
	err = replay.startConsensus(chain, waitGroup, worker, writer)
	if err != nil {
		return err
	}

	return nil
}

func (replay *dbReplay) workerProcessor() func(int, interface{}) {
	return func(_ int, valuei interface{}) {
		ctx := context.Background()

		switch value := valuei.(type) {
		case *WorkerPacket:
			var consumererr error
			switch value.consumeType {
			case CONSUME:
				for {
					consumererr = value.writer.Consume(ctx, replay.conns, value.message, replay.persist)
					if !db.ErrIsLockError(consumererr) {
						break
					}
				}
				if consumererr != nil {
					replay.errs.SetValue(consumererr)
					return
				}
			case CONSUMECONSENSUS:
				for {
					consumererr = value.writer.ConsumeConsensus(ctx, replay.conns, value.message, replay.persist)
					if !db.ErrIsLockError(consumererr) {
						break
					}
				}
				if consumererr != nil {
					replay.errs.SetValue(consumererr)
					return
				}
			case CONSUMEC:
				for {
					consumererr = value.cwriter.Consume(ctx, replay.conns, value.message, value.block, replay.persist)
					if !db.ErrIsLockError(consumererr) {
						break
					}
				}
				if consumererr != nil {
					replay.errs.SetValue(consumererr)
					return
				}
			}
		default:
		}
	}
}

func (replay *dbReplay) startCchain(chain string, waitGroup *int64, worker utils.Worker, writer *cvm.Writer) error {
	tn := fmt.Sprintf("%d-%s-cchain", replay.config.NetworkID, chain)

	replay.counterWaits.Inc(tn)
	replay.counterAdded.Add(tn, 0)

	atomic.AddInt64(waitGroup, 1)
	go func() {
		defer atomic.AddInt64(waitGroup, -1)
		defer replay.counterWaits.Add(tn, -1)

		job := replay.conns.Stream().NewJob("query-replay-txpoll")
		sess := replay.conns.DB().NewSessionForEventReceiver(job)

		ctx := context.Background()
		var txPools []services.TxPool
		_, err := sess.Select("id").
			From(services.TableTxPool).
			Where("topic=?", tn).
			LoadContext(ctx, &txPools)
		if err != nil {
			replay.errs.SetValue(err)
			return
		}

		for _, txPool := range txPools {
			if replay.errs.GetValue() != nil {
				replay.sc.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			txPoolQ := services.TxPool{
				ID: txPool.ID,
			}
			txPoolres, err := replay.persist.QueryTxPool(ctx, sess, &txPoolQ)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			id, err := ids.FromString(txPoolres.MsgKey)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			replay.counterAdded.Inc(tn)

			block, err := cblock.Unmarshal(txPoolres.Serialization)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			if block.BlockExtraData == nil {
				block.BlockExtraData = []byte("")
			}

			msgc := stream.NewMessage(
				id.String(),
				chain,
				block.BlockExtraData,
				txPoolres.CreatedAt.UTC().Unix(),
				int64(txPoolres.CreatedAt.UTC().Nanosecond()),
			)

			worker.Enque(&WorkerPacket{cwriter: writer, message: msgc, block: block, consumeType: CONSUMEC})
		}
	}()

	return nil
}

func (replay *dbReplay) startConsensus(chain cfg.Chain, waitGroup *int64, worker utils.Worker, writer services.Consumer) error {
	tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeConsensus)

	replay.counterWaits.Inc(tn)
	replay.counterAdded.Add(tn, 0)

	atomic.AddInt64(waitGroup, 1)
	go func() {
		defer atomic.AddInt64(waitGroup, -1)
		defer replay.counterWaits.Add(tn, -1)

		job := replay.conns.Stream().NewJob("query-replay-txpoll")
		sess := replay.conns.DB().NewSessionForEventReceiver(job)

		ctx := context.Background()
		var txPools []services.TxPool
		_, err := sess.Select("id").
			From(services.TableTxPool).
			Where("topic=?", tn).
			LoadContext(ctx, &txPools)
		if err != nil {
			replay.errs.SetValue(err)
			return
		}

		for _, txPool := range txPools {
			if replay.errs.GetValue() != nil {
				replay.sc.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			txPoolQ := services.TxPool{
				ID: txPool.ID,
			}
			txPoolres, err := replay.persist.QueryTxPool(ctx, sess, &txPoolQ)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			id, err := ids.FromString(txPoolres.MsgKey)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			replay.counterAdded.Inc(tn)

			msgc := stream.NewMessage(
				id.String(),
				chain.ID,
				txPool.Serialization,
				txPool.CreatedAt.UTC().Unix(),
				int64(txPool.CreatedAt.UTC().Nanosecond()),
			)

			worker.Enque(&WorkerPacket{writer: writer, message: msgc, consumeType: CONSUMECONSENSUS})
		}
	}()

	return nil
}

func (replay *dbReplay) startDecision(chain cfg.Chain, waitGroup *int64, worker utils.Worker, writer services.Consumer) error {
	tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeDecisions)

	replay.counterWaits.Inc(tn)
	replay.counterAdded.Add(tn, 0)

	atomic.AddInt64(waitGroup, 1)
	go func() {
		defer atomic.AddInt64(waitGroup, -1)
		defer replay.counterWaits.Add(tn, -1)

		job := replay.conns.Stream().NewJob("query-replay-txpoll")
		sess := replay.conns.DB().NewSessionForEventReceiver(job)

		ctx := context.Background()
		var txPools []services.TxPool
		_, err := sess.Select("id").
			From(services.TableTxPool).
			Where("topic=?", tn).
			LoadContext(ctx, &txPools)
		if err != nil {
			replay.errs.SetValue(err)
			return
		}

		for _, txPool := range txPools {
			if replay.errs.GetValue() != nil {
				replay.sc.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			txPoolQ := services.TxPool{
				ID: txPool.ID,
			}
			txPoolres, err := replay.persist.QueryTxPool(ctx, sess, &txPoolQ)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			id, err := ids.FromString(txPoolres.MsgKey)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			replay.counterAdded.Inc(tn)

			msgc := stream.NewMessage(
				id.String(),
				chain.ID,
				txPool.Serialization,
				txPool.CreatedAt.UTC().Unix(),
				int64(txPool.CreatedAt.UTC().Nanosecond()),
			)

			worker.Enque(&WorkerPacket{writer: writer, message: msgc, consumeType: CONSUME})
		}
	}()

	return nil
}
