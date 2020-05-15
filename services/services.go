package services

import (
	"context"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"
)

type Indexable interface {
	ID() ids.ID
	ChainID() ids.ID
	Body() []byte
	Timestamp() int64
}

// Indexer takes in Indexables and adds them to the services backend
type Indexer interface {
	Bootstrap() error
	Index(Indexable) error
}

// FanOutIndexer takes in items and sends them to multiple backend Indexers
type FanOutIndexer []Indexer

// Bootstrap initializes all underlying backends
func (fos FanOutIndexer) Bootstrap() (err error) {
	for _, service := range fos {
		if err = service.Bootstrap(); err != nil {
			return err
		}
	}
	return nil
}

// Index takes in an Indexable and sends it to all underlying indexers
func (fos FanOutIndexer) Index(i Indexable) (err error) {
	for _, service := range fos {
		if err = service.Index(i); err != nil {
			return err
		}
	}
	return nil
}

// IndexerCtx
type IndexerCtx struct {
	ctx context.Context
	job *health.Job
	db  dbr.SessionRunner

	time time.Time
}

func NewIndexerContext(job *health.Job, db dbr.SessionRunner, ts int64) IndexerCtx {
	return IndexerCtx{
		job:  job,
		db:   db,
		time: time.Unix(ts, 0),
	}
}

func (ic IndexerCtx) Time() time.Time          { return ic.time }
func (ic IndexerCtx) Job() *health.Job         { return ic.job }
func (ic IndexerCtx) DB() dbr.SessionRunner    { return ic.db }
func (ic IndexerCtx) Context() context.Context { return ic.ctx }
