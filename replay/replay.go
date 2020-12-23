package replay

import (
	"context"
	"fmt"
	"log"
	"net"
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

	addr, err := net.ResolveTCPAddr("tcp", replay.config.Kafka.Brokers[0])
	if err != nil {
		return err
	}

	err = replay.startDecision(addr, chain, replayEndTime, waitGroup, worker, writer, uidkeydecision)
	if err != nil {
		return err
	}
	err = replay.startConsensus(addr, chain, replayEndTime, waitGroup, worker, writer, uidkeyconsensus)
	if err != nil {
		return err
	}

	return nil
}

func (replay *replay) isPresent(uidkey string, id string) (bool, error) {
	replay.uniqueIDLock.RLock()
	present, err := replay.uniqueID[uidkey].Get(id)
	replay.uniqueIDLock.RUnlock()
	if err != nil {
		return false, err
	}
	if present {
		return present, nil
	}

	replay.uniqueIDLock.Lock()
	present, err = replay.uniqueID[uidkey].Get(id)
	if err == nil && !present {
		err = replay.uniqueID[uidkey].Put(id)
		if err == nil {
			present = true
		}
	}
	replay.uniqueIDLock.Unlock()
	if err != nil {
		return false, err
	}
	return present, nil
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

func (replay *replay) startConsensus(addr *net.TCPAddr, chain cfg.Chain, replayEndTime time.Time, waitGroup *int64, worker utils.Worker, writer services.Consumer, uidkey string) error {
	tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeConsensus)
	tu := utils.NewTopicUtil(addr, time.Duration(0), tn)

	parts, err := tu.Partitions(context.Background())
	if err != nil {
		return err
	}

	for part := range parts {
		partOffset := parts[part]

		replay.config.Services.Log.Info("processing part %d offset %d on topic %s", part, partOffset, tn)

		atomic.AddInt64(waitGroup, 1)
		go func() {
			defer atomic.AddInt64(waitGroup, -1)

			reader := kafka.NewReader(kafka.ReaderConfig{
				Topic:       tn,
				Brokers:     replay.config.Kafka.Brokers,
				Partition:   part,
				StartOffset: partOffset.FirstOffset,
				MaxBytes:    stream.ConsumerMaxBytesDefault,
			})

			// ctxDeadline := time.Now()

			for {
				if replay.errs.GetValue() != nil {
					replay.config.Services.Log.Info("replay for topic %s stopped for errors", tn)
					return
				}

				// if time.Since(ctxDeadline).Minutes() > 4 {
				// 	replay.config.Services.Log.Info("replay for topic %s stopped ctx deadline", tn)
				// 	return
				// }

				msg, err := replay.readMessage(10*time.Second, reader)
				if err != nil {
					if err != context.DeadlineExceeded {
						continue
					}
					replay.errs.SetValue(err)
					return
				}

				// ctxDeadline = time.Now()

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

				present, err := replay.isPresent(uidkey, id.String())
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
			}
		}()
	}
	return nil
}

func (replay *replay) startDecision(addr *net.TCPAddr, chain cfg.Chain, replayEndTime time.Time, waitGroup *int64, worker utils.Worker, writer services.Consumer, uidkey string) error {
	tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeDecisions)
	tu := utils.NewTopicUtil(addr, time.Duration(0), tn)

	parts, err := tu.Partitions(context.Background())
	if err != nil {
		return err
	}

	for part := range parts {
		partOffset := parts[part]

		replay.config.Services.Log.Info("processing part %d offset %d on topic %s", part, partOffset, tn)

		atomic.AddInt64(waitGroup, 1)
		go func() {
			defer atomic.AddInt64(waitGroup, -1)

			reader := kafka.NewReader(kafka.ReaderConfig{
				Topic:       tn,
				Brokers:     replay.config.Kafka.Brokers,
				Partition:   part,
				StartOffset: partOffset.FirstOffset,
				MaxBytes:    stream.ConsumerMaxBytesDefault,
			})

			ctx := context.Background()

			err := writer.Bootstrap(ctx)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			// ctxDeadline := time.Now()

			for {
				if replay.errs.GetValue() != nil {
					replay.config.Services.Log.Info("replay for topic %s stopped for errors", tn)
					return
				}

				// if time.Since(ctxDeadline).Minutes() > 4 {
				// 	replay.config.Services.Log.Info("replay for topic %s stopped ctx deadline", tn)
				// 	return
				// }

				msg, err := replay.readMessage(10*time.Second, reader)
				if err != nil {
					if err == context.DeadlineExceeded {
						continue
					}
					replay.errs.SetValue(err)
					return
				}

				// ctxDeadline = time.Now()

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

				present, err := replay.isPresent(uidkey, id.String())
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
			}
		}()
	}

	return nil
}
