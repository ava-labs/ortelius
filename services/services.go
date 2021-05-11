package services

import (
	"context"
	"time"

	cblock "github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services/idb"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
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
	Bootstrap(context.Context, *servicesconn.Connections, idb.Persist) error
	Consume(context.Context, *servicesconn.Connections, Consumable, idb.Persist) error
	ConsumeConsensus(context.Context, *servicesconn.Connections, Consumable, idb.Persist) error
	ParseJSON([]byte) ([]byte, error)
}

type ConsumerCChain interface {
	Name() string
	Consume(context.Context, *servicesconn.Connections, Consumable, *cblock.Block, idb.Persist) error
	ParseJSON([]byte) ([]byte, error)
}

// ConsumerCtx
type ConsumerCtx struct {
	ctx     context.Context
	job     *health.Job
	db      dbr.SessionRunner
	time    time.Time
	persist idb.Persist
}

func NewConsumerContext(
	ctx context.Context,
	job *health.Job,
	db dbr.SessionRunner,
	ts int64,
	nanosecond int64,
	persist idb.Persist,
) ConsumerCtx {
	return ConsumerCtx{
		ctx:     ctx,
		job:     job,
		db:      db,
		time:    time.Unix(ts, nanosecond),
		persist: persist,
	}
}

func (ic *ConsumerCtx) Time() time.Time       { return ic.time }
func (ic *ConsumerCtx) Job() *health.Job      { return ic.job }
func (ic *ConsumerCtx) DB() dbr.SessionRunner { return ic.db }
func (ic *ConsumerCtx) Ctx() context.Context  { return ic.ctx }
func (ic *ConsumerCtx) Persist() idb.Persist  { return ic.persist }
