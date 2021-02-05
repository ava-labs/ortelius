package services

import (
	"context"
	"time"

	"github.com/ava-labs/avalanchego/ids"

	cblock "github.com/ava-labs/ortelius/models"

	kafkaMessage "github.com/segmentio/kafka-go"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
)

type Consumable interface {
	ID() string
	ChainID() string
	Body() []byte
	Timestamp() int64
	Nanosecond() int64
	KafkaMessage() *kafkaMessage.Message
}

// Consumer takes in Consumables and adds them to the service's backend
type Consumer interface {
	Name() string
	Bootstrap(context.Context, *Connections, Persist, ConsumeState) error
	Consume(context.Context, *Connections, Consumable, Persist, ConsumeState) error
	ConsumeConsensus(context.Context, *Connections, Consumable, Persist, ConsumeState) error
	ParseJSON([]byte) ([]byte, error)
}

type ConsumerCChain interface {
	Name() string
	Consume(context.Context, *Connections, Consumable, *cblock.Block, Persist, ConsumeState) error
	ParseJSON([]byte) ([]byte, error)
}

type ConsumeState interface {
	AddOutputIds(ids.ID)
	GetOutputIds() []ids.ID
	AddAddresses(id ids.ShortID)
	GetAddresses() []ids.ShortID
}

type consumeState struct {
	outputIds map[ids.ID]struct{}
	addresses map[ids.ShortID]struct{}
}

func NewConsumerState() ConsumeState {
	return &consumeState{
		outputIds: make(map[ids.ID]struct{}),
		addresses: make(map[ids.ShortID]struct{}),
	}
}

func (c *consumeState) AddOutputIds(i ids.ID) {
	c.outputIds[i] = struct{}{}
}

func (c *consumeState) GetOutputIds() []ids.ID {
	idl := make([]ids.ID, 0, len(c.outputIds))
	for k := range c.outputIds {
		idl = append(idl, k)
	}
	return idl
}

func (c *consumeState) AddAddresses(i ids.ShortID) {
	c.addresses[i] = struct{}{}
}

func (c *consumeState) GetAddresses() []ids.ShortID {
	idl := make([]ids.ShortID, 0, len(c.addresses))
	for k := range c.addresses {
		idl = append(idl, k)
	}
	return idl
}

// ConsumerCtx
type ConsumerCtx struct {
	ctx          context.Context
	job          *health.Job
	db           dbr.SessionRunner
	time         time.Time
	persist      Persist
	consumeState ConsumeState
}

func NewConsumerContext(
	ctx context.Context,
	job *health.Job,
	db dbr.SessionRunner,
	ts int64,
	nanosecond int64,
	persist Persist,
	consumeState ConsumeState,
) ConsumerCtx {
	return ConsumerCtx{
		ctx:          ctx,
		job:          job,
		db:           db,
		time:         time.Unix(ts, nanosecond),
		persist:      persist,
		consumeState: consumeState,
	}
}

func (ic *ConsumerCtx) Time() time.Time            { return ic.time }
func (ic *ConsumerCtx) Job() *health.Job           { return ic.job }
func (ic *ConsumerCtx) DB() dbr.SessionRunner      { return ic.db }
func (ic *ConsumerCtx) Ctx() context.Context       { return ic.ctx }
func (ic *ConsumerCtx) Persist() Persist           { return ic.persist }
func (ic *ConsumerCtx) ConsumeState() ConsumeState { return ic.consumeState }

type NoopConsumable struct {
	id        string
	chainID   string
	body      []byte
	timestamp int64
	ns        int64
}

func (m *NoopConsumable) ID() string        { return m.id }
func (m *NoopConsumable) ChainID() string   { return m.chainID }
func (m *NoopConsumable) Body() []byte      { return m.body }
func (m *NoopConsumable) Timestamp() int64  { return m.timestamp }
func (m *NoopConsumable) Nanosecond() int64 { return m.ns }
func (m *NoopConsumable) KafkaMessage() *kafkaMessage.Message {
	return nil
}
