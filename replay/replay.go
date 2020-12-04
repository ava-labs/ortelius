package replay

import (
	"context"
	"fmt"
	"log"
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
		uniqueID:     utils.NewMemoryUniqueID(),
	}
}

type replay struct {
	uniqueID utils.UniqueID
	errs     *avlancheGoUtils.AtomicInterface
	running  *avlancheGoUtils.AtomicBool
	config   *cfg.Config

	counterRead  *utils.CounterID
	counterAdded *utils.CounterID
}

func (replay *replay) Start() error {
	cfg.PerformUpdates = true

	replay.errs = &avlancheGoUtils.AtomicInterface{}
	replay.running = &avlancheGoUtils.AtomicBool{}
	replay.running.SetValue(true)

	for _, chainID := range replay.config.Chains {
		err := replay.handleReader(chainID)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return err
		}
	}

	for replay.running.GetValue() {
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
			replay.config.Services.Log.Info("key:%s read:%d add:%d", cnter, ctot[cnter].Read, ctot[cnter].Added)
		}

		time.Sleep(5 * time.Second)
	}

	if replay.errs.GetValue() != nil {
		replay.config.Services.Log.Info("replay failed %w", replay.errs.GetValue().(error))
		return replay.errs.GetValue().(error)
	}

	return nil
}

func (replay *replay) handleReader(chain cfg.Chain) error {
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

	tn := stream.GetTopicName(replay.config.NetworkID, chain.ID, stream.EventTypeDecisions)

	go func() {
		defer replay.running.SetValue(false)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Topic:       tn,
			Brokers:     replay.config.Kafka.Brokers,
			GroupID:     replay.config.Consumer.GroupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    stream.ConsumerMaxBytesDefault,
		})

		ctx := context.Background()

		for replay.running.GetValue() {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			replay.counterRead.Inc(tn)

			id, err := ids.ToID(msg.Key)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			present, err := replay.uniqueID.Get(id.String())
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

			err = writer.Consume(msgc)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			err = replay.uniqueID.Put(id.String())
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
		}
	}()

	return nil
}
