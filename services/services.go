package services

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/modelsc"
	"github.com/ava-labs/ortelius/utils"
	"github.com/gocraft/dbr/v2"
)

type Consumable interface {
	ID() string
	ChainID() string
	Body() []byte
	Timestamp() int64
	Nanosecond() int64
}

// Consumer takes in Consumables and adds them to the service's backend
type Consumer interface {
	Name() string
	Bootstrap(context.Context, *utils.Connections, db.Persist) error
	Consume(context.Context, *utils.Connections, Consumable, db.Persist) error
	ConsumeConsensus(context.Context, *utils.Connections, Consumable, db.Persist) error
	ParseJSON([]byte) ([]byte, error)
}

type ConsumerCChain interface {
	Name() string
	Consume(context.Context, *utils.Connections, Consumable, *modelsc.Block, db.Persist) error
	ParseJSON([]byte) ([]byte, error)
}

type ConsumerCtx struct {
	ctx     context.Context
	db      dbr.SessionRunner
	time    time.Time
	persist db.Persist
}

func NewConsumerContext(ctx context.Context, db dbr.SessionRunner, ts int64, nanosecond int64, persist db.Persist) ConsumerCtx {
	return ConsumerCtx{
		ctx:     ctx,
		db:      db,
		time:    time.Unix(ts, nanosecond),
		persist: persist,
	}
}

func (ic *ConsumerCtx) Time() time.Time       { return ic.time }
func (ic *ConsumerCtx) DB() dbr.SessionRunner { return ic.db }
func (ic *ConsumerCtx) Ctx() context.Context  { return ic.ctx }
func (ic *ConsumerCtx) Persist() db.Persist   { return ic.persist }
