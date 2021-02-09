package export

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/utils/hashing"
	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/avalanchego/ids"
	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/stream"
	"github.com/segmentio/kafka-go"
)

type Export interface {
	Start() error
}

func New(sc *services.Control, config *cfg.Config, replayqueuesize int, replayqueuethreads int) Export {
	return &export{
		sc:           sc,
		config:       config,
		counterRead:  utils.NewCounterID(),
		counterWaits: utils.NewCounterID(),
		queueSize:    replayqueuesize,
		queueTheads:  replayqueuethreads,
	}
}

type export struct {
	errs   *avlancheGoUtils.AtomicInterface
	sc     *services.Control
	config *cfg.Config

	counterRead  *utils.CounterID
	counterWaits *utils.CounterID

	queueSize   int
	queueTheads int

	persist  services.Persist
	file     *os.File
	fileLock sync.Mutex
}

func (e *export) Start() error {
	cfg.PerformUpdates = true
	e.persist = services.NewPersist()

	e.errs = &avlancheGoUtils.AtomicInterface{}

	worker := utils.NewWorker(e.queueSize, e.queueTheads, e.workerProcessor())
	// stop when you see messages after this time.
	replayEndTime := time.Now().UTC().Add(time.Minute)
	waitGroup := new(int64)

	tnow := time.Now().UTC().Unix()
	file, err := os.OpenFile(fmt.Sprintf("extract.%d.json", tnow), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	e.file = file

	for _, chainID := range e.config.Chains {
		err := e.handleReader(chainID, replayEndTime, waitGroup, worker)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return err
		}
	}

	err = e.handleCReader(e.config.CchainID, replayEndTime, waitGroup, worker)
	if err != nil {
		log.Fatalln("reader failed", e.config.CchainID, ":", err.Error())
		return err
	}

	timeLog := time.Now()

	logemit := func(waitGroupCnt int64) {
		type CounterValues struct {
			Read  int64
			Added int64
			Waits int64
		}

		ctot := make(map[string]*CounterValues)
		countersValues := e.counterRead.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Read = countersValues[cnter]
		}
		countersValues = e.counterWaits.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Waits = countersValues[cnter]
		}

		e.sc.Log.Info("wgc: %d, jobs: %d", waitGroupCnt, worker.JobCnt())

		var sortedcnters []string
		for cnter := range ctot {
			sortedcnters = append(sortedcnters, cnter)
		}
		sort.Strings(sortedcnters)
		for _, cnter := range sortedcnters {
			if ctot[cnter].Waits != 0 {
				newlogline := fmt.Sprintf("key:%s read:%d add:%d wait:%d", cnter, ctot[cnter].Read, ctot[cnter].Added, ctot[cnter].Waits)
				e.sc.Log.Info(newlogline)
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

	if e.errs.GetValue() != nil {
		e.sc.Log.Error("e failed %v", e.errs.GetValue().(error))
		return e.errs.GetValue().(error)
	}

	return nil
}

type ConsumeType uint32

var (
	CONSUME          ConsumeType = 1
	CONSUMECONSENSUS ConsumeType = 2
	CONSUMEC         ConsumeType = 3
)

type WorkerPacket struct {
	message     services.Consumable
	consumeType ConsumeType
	block       *cblock.Block
}

type JSONData struct {
	Key     string    `json:"key"`
	Data    string    `json:"data"`
	Time    time.Time `json:"time"`
	ChainID string    `json:"chainID"`
}

func (e *export) handleCReader(chain string, replayEndTime time.Time, waitGroup *int64, worker utils.Worker) error {
	addr, err := net.ResolveTCPAddr("tcp", e.config.Kafka.Brokers[0])
	if err != nil {
		return err
	}

	err = e.startCchain(addr, chain, replayEndTime, waitGroup, worker)
	if err != nil {
		return err
	}

	return nil
}

func (e *export) handleReader(chain cfg.Chain, replayEndTime time.Time, waitGroup *int64, worker utils.Worker) error {
	var err error

	addr, err := net.ResolveTCPAddr("tcp", e.config.Kafka.Brokers[0])
	if err != nil {
		return err
	}

	err = e.startDecision(addr, chain, replayEndTime, waitGroup, worker)
	if err != nil {
		return err
	}
	err = e.startConsensus(addr, chain, replayEndTime, waitGroup, worker)
	if err != nil {
		return err
	}

	return nil
}

func (e *export) readMessage(duration time.Duration, reader *kafka.Reader) (kafka.Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), duration)
	defer cancelFn()
	return reader.ReadMessage(ctx)
}

func (e *export) workerProcessor() func(int, interface{}) {
	return func(_ int, valuei interface{}) {
		switch value := valuei.(type) {
		case *WorkerPacket:
			switch value.consumeType {
			case CONSUME:
				exportJSON := &JSONData{
					Key:     value.message.ID(),
					Data:    hex.EncodeToString(value.message.KafkaMessage().Value),
					Time:    value.message.KafkaMessage().Time,
					ChainID: value.message.ChainID(),
				}
				j, err := json.Marshal(exportJSON)
				if err != nil {
					e.errs.SetValue(err)
					return
				}
				e.fileLock.Lock()
				_, _ = e.file.Write(j)
				_, _ = e.file.Write([]byte("\n"))
				e.fileLock.Unlock()
			case CONSUMECONSENSUS:
				exportJSON := &JSONData{
					Key:     value.message.ID(),
					Data:    hex.EncodeToString(value.message.KafkaMessage().Value),
					Time:    value.message.KafkaMessage().Time,
					ChainID: value.message.ChainID(),
				}
				j, err := json.Marshal(exportJSON)
				if err != nil {
					e.errs.SetValue(err)
					return
				}
				e.fileLock.Lock()
				_, _ = e.file.Write(j)
				_, _ = e.file.Write([]byte("\n"))
				e.fileLock.Unlock()
			case CONSUMEC:
				bl, err := json.Marshal(value.block)
				if err != nil {
					e.errs.SetValue(err)
					return
				}
				exportJSON := &JSONData{
					Key:     value.message.ID(),
					Data:    string(bl),
					Time:    value.message.KafkaMessage().Time,
					ChainID: value.message.ChainID(),
				}
				j, err := json.Marshal(exportJSON)
				if err != nil {
					e.errs.SetValue(err)
					return
				}
				e.fileLock.Lock()
				_, _ = e.file.Write(j)
				_, _ = e.file.Write([]byte("\n"))
				e.fileLock.Unlock()
			}
		default:
		}
	}
}

func (e *export) startCchain(addr *net.TCPAddr, chain string, replayEndTime time.Time, waitGroup *int64, worker utils.Worker) error {
	tn := fmt.Sprintf("%d-%s-cchain", e.config.NetworkID, chain)
	tu := utils.NewTopicUtil(addr, time.Duration(0), tn)

	parts, err := tu.Partitions(context.Background())
	if err != nil {
		return err
	}

	for part := range parts {
		partOffset := parts[part]

		e.sc.Log.Info("processing part %d offset %d on topic %s", partOffset.Partition, partOffset.FirstOffset, tn)

		e.counterWaits.Inc(tn)
		e.counterRead.Add(tn, 0)

		atomic.AddInt64(waitGroup, 1)
		go func() {
			defer atomic.AddInt64(waitGroup, -1)
			defer e.counterWaits.Add(tn, -1)

			e.sc.Log.Info("e for topic %s:%d init", tn, partOffset.Partition)
			reader := kafka.NewReader(kafka.ReaderConfig{
				Topic:       tn,
				Brokers:     e.config.Kafka.Brokers,
				Partition:   partOffset.Partition,
				StartOffset: partOffset.FirstOffset,
				MaxBytes:    stream.ConsumerMaxBytesDefault,
			})
			e.sc.Log.Info("e for topic %s:%d reading", tn, partOffset.Partition)

			for {
				if e.errs.GetValue() != nil {
					e.sc.Log.Info("e for topic %s:%d stopped for errors", tn, partOffset.Partition)
					return
				}

				msg, err := e.readMessage(10*time.Second, reader)
				if err != nil {
					if err == context.DeadlineExceeded {
						continue
					}
					e.errs.SetValue(err)
					return
				}

				if msg.Time.UTC().After(replayEndTime) {
					e.sc.Log.Info("e for topic %s:%d reached %s", tn, partOffset.Partition, replayEndTime.String())
					return
				}

				e.counterRead.Inc(tn)

				block, err := cblock.Unmarshal(msg.Value)
				if err != nil {
					e.errs.SetValue(err)
					return
				}

				if block.BlockExtraData == nil {
					block.BlockExtraData = []byte("")
				}

				hid := hashing.ComputeHash256(block.BlockExtraData)

				msgc := stream.NewMessageWithKafka(
					string(hid),
					chain,
					block.BlockExtraData,
					msg.Time.UTC().Unix(),
					int64(msg.Time.UTC().Nanosecond()),
					&msg,
				)

				worker.Enque(&WorkerPacket{message: msgc, block: block, consumeType: CONSUMEC})
			}
		}()
	}
	return nil
}

func (e *export) startConsensus(addr *net.TCPAddr, chain cfg.Chain, replayEndTime time.Time, waitGroup *int64, worker utils.Worker) error {
	tn := stream.GetTopicName(e.config.NetworkID, chain.ID, stream.EventTypeConsensus)
	tu := utils.NewTopicUtil(addr, time.Duration(0), tn)

	parts, err := tu.Partitions(context.Background())
	if err != nil {
		return err
	}

	for part := range parts {
		partOffset := parts[part]

		e.sc.Log.Info("processing part %d offset %d on topic %s", partOffset.Partition, partOffset.FirstOffset, tn)

		e.counterWaits.Inc(tn)
		e.counterRead.Add(tn, 0)

		atomic.AddInt64(waitGroup, 1)
		go func() {
			defer atomic.AddInt64(waitGroup, -1)
			defer e.counterWaits.Add(tn, -1)

			e.sc.Log.Info("e for topic %s:%d init", tn, partOffset.Partition)
			reader := kafka.NewReader(kafka.ReaderConfig{
				Topic:       tn,
				Brokers:     e.config.Kafka.Brokers,
				Partition:   partOffset.Partition,
				StartOffset: partOffset.FirstOffset,
				MaxBytes:    stream.ConsumerMaxBytesDefault,
			})
			e.sc.Log.Info("e for topic %s:%d reading", tn, partOffset.Partition)

			for {
				if e.errs.GetValue() != nil {
					e.sc.Log.Info("e for topic %s:%d stopped for errors", tn, partOffset.Partition)
					return
				}

				msg, err := e.readMessage(10*time.Second, reader)
				if err != nil {
					if err == context.DeadlineExceeded {
						continue
					}
					e.errs.SetValue(err)
					return
				}

				if msg.Time.UTC().After(replayEndTime) {
					e.sc.Log.Info("e for topic %s:%d reached %s", tn, partOffset.Partition, replayEndTime.String())
					return
				}

				e.counterRead.Inc(tn)

				id, err := ids.ToID(msg.Key)
				if err != nil {
					e.errs.SetValue(err)
					return
				}

				msgc := stream.NewMessageWithKafka(
					id.String(),
					chain.ID,
					msg.Value,
					msg.Time.UTC().Unix(),
					int64(msg.Time.UTC().Nanosecond()),
					&msg,
				)

				worker.Enque(&WorkerPacket{message: msgc, consumeType: CONSUMECONSENSUS})
			}
		}()
	}
	return nil
}

func (e *export) startDecision(addr *net.TCPAddr, chain cfg.Chain, replayEndTime time.Time, waitGroup *int64, worker utils.Worker) error {
	tn := stream.GetTopicName(e.config.NetworkID, chain.ID, stream.EventTypeDecisions)
	tu := utils.NewTopicUtil(addr, time.Duration(0), tn)

	parts, err := tu.Partitions(context.Background())
	if err != nil {
		return err
	}

	for part := range parts {
		partOffset := parts[part]

		e.sc.Log.Info("processing part %d offset %d on topic %s", partOffset.Partition, partOffset.FirstOffset, tn)

		e.counterWaits.Inc(tn)
		e.counterRead.Add(tn, 0)

		atomic.AddInt64(waitGroup, 1)
		go func() {
			defer atomic.AddInt64(waitGroup, -1)
			defer e.counterWaits.Add(tn, -1)

			e.sc.Log.Info("e for topic %s:%d init", tn, partOffset.Partition)
			reader := kafka.NewReader(kafka.ReaderConfig{
				Topic:       tn,
				Brokers:     e.config.Kafka.Brokers,
				Partition:   partOffset.Partition,
				StartOffset: partOffset.FirstOffset,
				MaxBytes:    stream.ConsumerMaxBytesDefault,
			})
			e.sc.Log.Info("e for topic %s:%d reading", tn, partOffset.Partition)

			for {
				if e.errs.GetValue() != nil {
					e.sc.Log.Info("e for topic %s:%d stopped for errors", tn, partOffset.Partition)
					return
				}

				msg, err := e.readMessage(10*time.Second, reader)
				if err != nil {
					if err == context.DeadlineExceeded {
						continue
					}
					e.errs.SetValue(err)
					return
				}

				if msg.Time.UTC().After(replayEndTime) {
					e.sc.Log.Info("e for topic %s:%d reached %s", tn, partOffset.Partition, replayEndTime.String())
					return
				}

				e.counterRead.Inc(tn)

				id, err := ids.ToID(msg.Key)
				if err != nil {
					e.errs.SetValue(err)
					return
				}

				msgc := stream.NewMessageWithKafka(
					id.String(),
					chain.ID,
					msg.Value,
					msg.Time.UTC().Unix(),
					int64(msg.Time.UTC().Nanosecond()),
					&msg,
				)

				worker.Enque(&WorkerPacket{message: msgc, consumeType: CONSUME})
			}
		}()
	}

	return nil
}
