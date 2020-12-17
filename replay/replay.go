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

	// stop when you see messages after this time.
	replayEndTime := time.Now().UTC().Add(time.Minute)
	waitGroup := new(int64)

	for _, chainID := range replay.config.Chains {
		err := replay.handleReader(chainID, replayEndTime, waitGroup)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return err
		}
	}

	timeLog := time.Now()

	for {
		waitGroupCnt := atomic.LoadInt64(waitGroup)

		if time.Now().Sub(timeLog).Seconds() > 30 {
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

			for cnter := range ctot {
				replay.config.Services.Log.Info("wgc: %d, key:%s read:%d add:%d", waitGroupCnt, cnter, ctot[cnter].Read, ctot[cnter].Added)
			}
		}

		if waitGroupCnt == 0 {
			break
		}

		time.Sleep(time.Second)

		timeLog = time.Now()
	}

	if replay.errs.GetValue() != nil {
		replay.config.Services.Log.Error("replay failed %w", replay.errs.GetValue().(error))
		return replay.errs.GetValue().(error)
	}

	return nil
}

func (replay *replay) handleReader(chain cfg.Chain, replayEndTime time.Time, waitGroup *int64) error {
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

		for {
			if replay.errs.GetValue() != nil {
				replay.config.Services.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			if msg.Time.After(replayEndTime) {
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

			var consumererr error
			for icnt := 0; icnt < 100; icnt++ {
				consumererr = writer.Consume(context.Background(), msgc)
				if consumererr == nil {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if consumererr != nil {
				replay.errs.SetValue(err)
				return
			}

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

		ctx := context.Background()

		for {
			if replay.errs.GetValue() != nil {
				replay.config.Services.Log.Info("replay for topic %s stopped for errors", tn)
				return
			}

			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			if msg.Time.After(replayEndTime) {
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

			var consumererr error
			for icnt := 0; icnt < 100; icnt++ {
				consumererr = writer.ConsumeConsensus(context.Background(), msgc)
				if consumererr == nil {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if consumererr != nil {
				replay.errs.SetValue(err)
				return
			}

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
