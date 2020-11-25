package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
	"github.com/ava-labs/ortelius/stream/consumers"
	"github.com/segmentio/kafka-go"
)

type DBHolder struct {
	m    map[string]int
	Lock sync.Mutex
}

func (replay *DBHolder) init() error {
	replay.Lock.Lock()
	defer replay.Lock.Unlock()
	return nil
}

//nolint:unparam
func (replay *DBHolder) get(id string) (bool, error) {
	replay.Lock.Lock()
	defer replay.Lock.Unlock()

	if res, ok := replay.m[id]; ok {
		return res != 0, nil
	}
	return false, nil
}

//nolint:unparam
func (replay *DBHolder) put(id string) error {
	replay.Lock.Lock()
	defer replay.Lock.Unlock()

	replay.m[id] = 1
	return nil
}

type Counter struct {
	countersLock sync.RWMutex
	counters     map[string]*uint64
}

func NewCounter() *Counter {
	return &Counter{counters: make(map[string]*uint64)}
}

func (c *Counter) Inc(v string) {
	found := false
	c.countersLock.RLock()
	if counter, ok := c.counters[v]; ok {
		atomic.AddUint64(counter, 1)
		found = true
	}
	c.countersLock.RUnlock()

	if found {
		return
	}

	c.countersLock.Lock()
	if _, ok := c.counters[v]; !ok {
		c.counters[v] = new(uint64)
	}
	atomic.AddUint64(c.counters[v], 1)
	c.countersLock.Unlock()
}

func (c *Counter) Clone() map[string]uint64 {
	countersValues := make(map[string]uint64)
	c.countersLock.RLock()
	for cnter := range c.counters {
		cv := atomic.LoadUint64(c.counters[cnter])
		if cv != 0 {
			countersValues[cnter] = cv
		}
	}
	c.countersLock.RUnlock()
	return countersValues
}

type Replay struct {
	ConfigFile *string
	GroupName  *string
	DedupDB    *string
	DBInst     *DBHolder
	errs       *utils.AtomicInterface
	running    *utils.AtomicBool
	Config     *cfg.Config

	CounterRead  *Counter
	CounterAdded *Counter
}

func main() {
	cfg.PerformUpdates = true

	config := flag.String("config", "", "config file")
	groupName := flag.String("groupName", "", "group name")
	dedupDB := flag.String("dedupDb", "dedupDb.db", "dedupDb")

	flag.Parse()

	replay := &Replay{
		ConfigFile:   config,
		GroupName:    groupName,
		DedupDB:      dedupDB,
		CounterRead:  NewCounter(),
		CounterAdded: NewCounter(),
	}
	replay.Start()
}

func (replay *Replay) Start() {
	config, err := cfg.NewFromFile(*replay.ConfigFile)
	if err != nil {
		log.Fatalln("config file not found", replay.ConfigFile, ":", err.Error())
		return
	}

	replay.Config = config

	var alog *logging.Log
	alog, err = logging.New(config.Logging)
	if err != nil {
		log.Fatalln("Failed to create log", config.Logging.Directory, ":", err.Error())
		return
	}

	replay.DBInst = &DBHolder{}

	err = replay.DBInst.init()
	if err != nil {
		log.Fatalln("create dedup table failed", ":", err.Error())
		return
	}

	replay.errs = &utils.AtomicInterface{}
	replay.running = &utils.AtomicBool{}
	replay.running.SetValue(true)

	for _, chainID := range config.Chains {
		err = replay.handleReader(chainID)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return
		}
	}

	for replay.running.GetValue() {
		type CounterValues struct {
			Read  uint64
			Added uint64
		}

		ctot := make(map[string]*CounterValues)
		countersValues := replay.CounterRead.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Read = countersValues[cnter]
		}
		countersValues = replay.CounterAdded.Clone()
		for cnter := range countersValues {
			if _, ok := ctot[cnter]; !ok {
				ctot[cnter] = &CounterValues{}
			}
			ctot[cnter].Added = countersValues[cnter]
		}

		for cnter := range ctot {
			alog.Info("%s %d %d", cnter, ctot[cnter].Read, ctot[cnter].Added)
		}

		time.Sleep(5 * time.Second)
	}

	if replay.errs.GetValue() != nil {
		alog.Info("err %v", replay.errs.GetValue())
	}
}

type MessageR struct {
	id        string
	chainID   string
	body      []byte
	timestamp int64
}

func (m *MessageR) ID() string       { return m.id }
func (m *MessageR) ChainID() string  { return m.chainID }
func (m *MessageR) Body() []byte     { return m.body }
func (m *MessageR) Timestamp() int64 { return m.timestamp }

func (replay *Replay) handleReader(chain cfg.Chain) error {
	conns, err := services.NewConnectionsFromConfig(replay.Config.Services, false)
	if err != nil {
		return err
	}

	var writer services.Consumer
	switch chain.VMType {
	case consumers.IndexerAVMName:
		writer, err = avm.NewWriter(conns, replay.Config.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	case consumers.IndexerPVMName:
		writer, err = pvm.NewWriter(conns, replay.Config.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown vmtype")
	}

	tn := stream.GetTopicName(replay.Config.NetworkID, chain.ID, stream.ConsumerEventTypeDefault)

	go func() {
		defer replay.running.SetValue(false)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Topic:       tn,
			Brokers:     replay.Config.Kafka.Brokers,
			GroupID:     *replay.GroupName,
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

			replay.CounterRead.Inc(tn)

			id, err := ids.ToID(msg.Key)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			present, err := replay.DBInst.get(id.String())
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
			if present {
				continue
			}

			replay.CounterAdded.Inc(tn)

			msgc := &MessageR{
				chainID:   chain.ID,
				body:      msg.Value,
				id:        id.String(),
				timestamp: msg.Time.UTC().Unix(),
			}

			err = writer.Consume(ctx, msgc)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			err = replay.DBInst.put(id.String())
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
		}
	}()

	return nil
}
