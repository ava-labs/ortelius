package services

import (
	"context"
	"time"

	"github.com/gocraft/dbr"
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
	Bootstrap() error
	Consume(Consumable) error
}

// FanOutConsumer takes in items and sends them to multiple backend Indexers
type FanOutConsumer []Consumer

// Bootstrap initializes all underlying backends
func (fos FanOutConsumer) Bootstrap() (err error) {
	for _, service := range fos {
		if err = service.Bootstrap(); err != nil {
			return err
		}
	}
	return nil
}

// Consume takes in a Consumable and sends it to all underlying indexers
func (foc FanOutConsumer) Consume(c Consumable) (err error) {
	for _, consumer := range foc {
		if err = consumer.Consume(c); err != nil {
			return err
		}
	}
	return nil
}

// ConsumerCtx
type ConsumerCtx struct {
	ctx context.Context
	job *health.Job
	db  dbr.SessionRunner

	time time.Time
}

func NewConsumerContext(job *health.Job, db dbr.SessionRunner, ts int64) ConsumerCtx {
	return ConsumerCtx{
		job:  job,
		db:   db,
		time: time.Unix(ts, 0),
	}
}

func (ic ConsumerCtx) Time() time.Time          { return ic.time }
func (ic ConsumerCtx) Job() *health.Job         { return ic.job }
func (ic ConsumerCtx) DB() dbr.SessionRunner    { return ic.db }
func (ic ConsumerCtx) Context() context.Context { return ic.ctx }
