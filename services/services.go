package services

import (
	"context"
	"time"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
)

type Consumable interface {
	ID() string
	ChainID() string
	Body() []byte
	Timestamp() int64
}

// Consumer takes in Consumables and adds them to the service's backend
type Consumer interface {
	Name() string
	Bootstrap(context.Context, Persist) error
	Consume(context.Context, Consumable, Persist) error
	ConsumeConsensus(context.Context, Consumable, Persist) error
}

// ConsumerCtx
type ConsumerCtx struct {
	ctx     context.Context
	job     *health.Job
	db      dbr.SessionRunner
	time    time.Time
	persist Persist
}

func NewConsumerContext(ctx context.Context, job *health.Job, db dbr.SessionRunner, ts int64, persist Persist) ConsumerCtx {
	return ConsumerCtx{
		ctx:     ctx,
		job:     job,
		db:      db,
		time:    time.Unix(ts, 0),
		persist: persist,
	}
}

func (ic ConsumerCtx) Time() time.Time       { return ic.time }
func (ic ConsumerCtx) Job() *health.Job      { return ic.job }
func (ic ConsumerCtx) DB() dbr.SessionRunner { return ic.db }
func (ic ConsumerCtx) Ctx() context.Context  { return ic.ctx }
func (ic ConsumerCtx) Persist() Persist      { return ic.persist }
