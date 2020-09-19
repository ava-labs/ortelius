// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-go/utils/wrappers"
	"github.com/go-redis/redis/v8"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/cache"
	"github.com/ava-labs/ortelius/services/db"
)

type Connections struct {
	stream *health.Stream
	db     *dbr.Connection

	redis *redis.Client
	cache *cache.Cache
}

func NewConnectionsFromConfig(conf cfg.Services) (*Connections, error) {
	stream := NewStream()

	var (
		err         error
		dbConn      *dbr.Connection
		redisClient *redis.Client
	)

	if conf.Redis != nil && conf.Redis.Addr != "" {
		kvs := health.Kvs{"addr": conf.Redis.Addr, "db": strconv.Itoa(conf.Redis.DB)}
		redisClient, err = NewRedisConn(&redis.Options{
			DB:       conf.Redis.DB,
			Addr:     conf.Redis.Addr,
			Password: conf.Redis.Password,
		})
		if err != nil {
			stream.EventErrKv("connect.redis", err, kvs)
			return nil, err
		}
		stream.EventKv("connect.redis", kvs)
	} else {
		stream.Event("connect.redis.skip")
	}

	if conf.DB != nil || conf.DB.Driver == db.DriverNone {
		// Setup logging kvs
		kvs := health.Kvs{"driver": conf.DB.Driver}
		loggableDSN, err := db.SanitizedDSN(conf.DB)
		if err != nil {
			stream.EventErrKv("connect.db.sanitize_dsn", err, kvs)
			return nil, err
		}
		kvs["dsn"] = loggableDSN

		// Create connection
		dbConn, err = db.New(stream, *conf.DB)
		if err != nil {
			stream.EventErrKv("connect.db", err, kvs)
			return nil, err
		}
		stream.EventKv("connect.db", kvs)
	} else {
		stream.Event("connect.db.skip")
	}

	return NewConnections(stream, dbConn, redisClient), nil
}

func NewConnections(stream *health.Stream, dbConn *dbr.Connection, redisClient *redis.Client) *Connections {
	var c *cache.Cache
	if redisClient != nil {
		c = cache.New(redisClient)
	}

	return &Connections{
		stream: stream,
		db:     dbConn,
		redis:  redisClient,
		cache:  c,
	}
}

func (c Connections) Stream() *health.Stream { return c.stream }
func (c Connections) DB() *dbr.Connection    { return c.db }
func (c Connections) Redis() *redis.Client   { return c.redis }
func (c Connections) Cache() *cache.Cache    { return c.cache }

func (c Connections) Close() error {
	errs := wrappers.Errs{}
	errs.Add(c.db.Close(), c.redis.Close())
	return errs.Err
}

func NewStream() *health.Stream {
	s := health.NewStream()
	s.AddSink(&health.WriterSink{Writer: os.Stdout})
	return s
}

func NewRedisConn(opts *redis.Options) (*redis.Client, error) {
	client := redis.NewClient(opts)

	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	return client, nil
}
