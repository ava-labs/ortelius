package replay

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/avalanchego/ids"
	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
	"github.com/ava-labs/ortelius/stream/consumers"
	"github.com/segmentio/kafka-go"
)

type Replay interface {
	Start() error
}

func New(config *cfg.Config) Replay {
	return &replay{config: config,
		counterRead:  utils.NewCounterID(),
		counterAdded: utils.NewCounterID(),
		uniqueID:     make(map[string]utils.UniqueID),
	}
}

type replay struct {
	uniqueIDLock sync.RWMutex
	uniqueID     map[string]utils.UniqueID

	errs   *avlancheGoUtils.AtomicInterface
	config *cfg.Config

	counterRead  *utils.CounterID
	counterAdded *utils.CounterID
}

func (replay *replay) Start() error {
	cfg.PerformUpdates = true

	replay.errs = &avlancheGoUtils.AtomicInterface{}

	worker := utils.NewWorker(5000, 10, replay.workerProcessor())
	// stop when you see messages after this time.
	replayEndTime := time.Now().UTC().Add(time.Minute)
	waitGroup := new(int64)

	for _, chainID := range replay.config.Chains {
		err := replay.handleReader(chainID, replayEndTime, waitGroup, worker)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return err
		}
	}

	timeLog := time.Now()

	logemit := func(waitGroupCnt int64) {
		type CounterValues struct {
			Read  uint64
			Added uint64
		}

		ctot := make(map[string]*CounterValues)
		countersValues := replay.counterRead.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Read = countersValues[cnter]
		}
		countersValues = replay.counterAdded.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Added = countersValues[cnter]
		}

		replay.config.Services.Log.Info("wgc: %d, jobs: %d", waitGroupCnt, worker.JobCnt())
		for cnter := range ctot {
			replay.config.Services.Log.Info("key:%s read:%d add:%d", cnter, ctot[cnter].Read, ctot[cnter].Added)
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
		replay.config.Services.Log.Error("replay failed %w", replay.errs.GetValue().(error))
		return replay.errs.GetValue().(error)
	}

	return nil
}

type WorkerPacket struct {
	writer  services.Consumer
	message services.Consumable
	consume bool
}

func (replay *replay) handleReader(chain cfg.Chain, replayEndTime time.Time, waitGroup *int64, worker utils.Worker) error {
	conns, err := services.NewConnectionsFromConfig(replay.config.Services, false)
	if err != nil {
		return err
	}

	var writer services.Consumer
	switch chain.VMType {
	case consumers.IndexerAVMName:
		writer, err = avm.NewWriter(conns, replay.config.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	case consumers.IndexerPVMName:
		writer, err = pvm.NewWriter(conns, replay.config.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown vmtype")
	}

	uidkeyconsensus := fmt.Sprintf("%s:%s", stream.EventTypeConsensus, chain.ID)
	uidkeydecision := fmt.Sprintf("%s:%s", stream.EventTypeDecisions, chain.ID)

	replay.uniqueIDLock.Lock()
	replay.uniqueID[uidkeyconsensus] = utils.NewMemoryUniqueID()
	replay.uniqueID[uidkeydecision] = utils.NewMemoryUniqueID()
	replay.uniqueIDLock.Unlock()

	atomic.AddInt64(waitGroup, 1)
	go func() {
		defer atomic.AddInt64(waitGroup, -1)
		tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeDecisions)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Topic:       tn,
			Brokers:     replay.config.Kafka.Brokers,
			GroupID:     replay.config.Consumer.GroupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    stream.ConsumerMaxBytesDefault,
		})

		ctx := context.Background()

		err := writer.Bootstrap(ctx)
		if err != nil {
			replay.errs.SetValue(err)
			return
		}

		ctxDeadline := time.Now()

		for {
			if replay.errs.GetValue() != nil {
				replay.config.Services.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			if time.Since(ctxDeadline).Minutes() > 4 {
				replay.config.Services.Log.Info("replay for topic %s stopped ctx deadline", tn)
				return
			}

			msg, err := replay.readMessage(10*time.Second, reader)
			if err != nil {
				if err != context.DeadlineExceeded {
					continue
				}
				replay.errs.SetValue(err)
				return
			}

			ctxDeadline = time.Now()

			if msg.Time.UTC().After(replayEndTime) {
				replay.config.Services.Log.Info("replay for topic %s reached %s", tn, replayEndTime.String())
				return
			}

			replay.counterRead.Inc(tn)

			id, err := ids.ToID(msg.Key)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			replay.uniqueIDLock.RLock()
			present, err := replay.uniqueID[uidkeydecision].Get(id.String())
			replay.uniqueIDLock.RUnlock()
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
			if present {
				continue
			}

			replay.counterAdded.Inc(tn)

			msgc := stream.NewMessage(
				id.String(),
				chain.ID,
				msg.Value,
				msg.Time.UTC().Unix(),
			)

			worker.Enque(&WorkerPacket{writer: writer, message: msgc, consume: true})

			replay.uniqueIDLock.RLock()
			err = replay.uniqueID[uidkeydecision].Put(id.String())
			replay.uniqueIDLock.RUnlock()
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
		}
	}()

	atomic.AddInt64(waitGroup, 1)
	go func() {
		defer atomic.AddInt64(waitGroup, -1)
		tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeConsensus)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Topic:       tn,
			Brokers:     replay.config.Kafka.Brokers,
			GroupID:     replay.config.Consumer.GroupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    stream.ConsumerMaxBytesDefault,
		})

		ctxDeadline := time.Now()

		for {
			if replay.errs.GetValue() != nil {
				replay.config.Services.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			if time.Since(ctxDeadline).Minutes() > 4 {
				replay.config.Services.Log.Info("replay for topic %s stopped ctx deadline", tn)
				return
			}

			msg, err := replay.readMessage(10*time.Second, reader)
			if err != nil {
				if err != context.DeadlineExceeded {
					continue
				}
				replay.errs.SetValue(err)
				return
			}

			ctxDeadline = time.Now()

			if msg.Time.UTC().After(replayEndTime) {
				replay.config.Services.Log.Info("replay for topic %s reached %s", tn, replayEndTime.String())
				return
			}

			replay.counterRead.Inc(tn)

			id, err := ids.ToID(msg.Key)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			replay.uniqueIDLock.RLock()
			present, err := replay.uniqueID[uidkeyconsensus].Get(id.String())
			replay.uniqueIDLock.RUnlock()
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
			if present {
				continue
			}

			replay.counterAdded.Inc(tn)

			msgc := stream.NewMessage(
				id.String(),
				chain.ID,
				msg.Value,
				msg.Time.UTC().Unix(),
			)

			worker.Enque(&WorkerPacket{writer: writer, message: msgc, consume: false})

			replay.uniqueIDLock.RLock()
			err = replay.uniqueID[uidkeyconsensus].Put(id.String())
			replay.uniqueIDLock.RUnlock()
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
		}
	}()

	return nil
}

func (replay *replay) readMessage(duration time.Duration, reader *kafka.Reader) (kafka.Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), duration)
	defer cancelFn()
	return reader.ReadMessage(ctx)
}

func (replay *replay) workerProcessor() func(int, interface{}) {
	return func(_ int, valuei interface{}) {
		switch value := valuei.(type) {
		case *WorkerPacket:
			if value.consume {
				var consumererr error
				for icnt := 0; icnt < 100; icnt++ {
					consumererr = value.writer.Consume(context.Background(), value.message)
					if consumererr == nil {
						break
					}
					time.Sleep(500 * time.Millisecond)
				}
				if consumererr != nil {
					replay.errs.SetValue(consumererr)
					return
				}
			} else {
				var consumererr error
				for icnt := 0; icnt < 100; icnt++ {
					consumererr = value.writer.ConsumeConsensus(context.Background(), value.message)
					if consumererr == nil {
						break
					}
					time.Sleep(500 * time.Millisecond)
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
