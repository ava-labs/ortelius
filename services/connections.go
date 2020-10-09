// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/go-redis/redis/v8"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/cache"
	"github.com/ava-labs/ortelius/services/db"
)

type Connections struct {
	stream *health.Stream
	logger logging.Logger

	db    *db.Conn
	redis *redis.Client
	cache *cache.Cache
}

func NewConnectionsFromConfig(conf cfg.Services) (*Connections, error) {
	// Always create a stream and log
	stream := NewStream()
	log, err := logging.New(conf.Logging)
	if err != nil {
		return nil, err
	}

	// Create db and redis connections if configured
	var (
		dbConn      *db.Conn
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
			return nil, stream.EventErrKv("connect.redis", err, kvs)
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
			return nil, stream.EventErrKv("connect.db.sanitize_dsn", err, kvs)
		}
		kvs["dsn"] = loggableDSN

		// Create connection
		dbConn, err = db.New(stream, *conf.DB)
		if err != nil {
			return nil, stream.EventErrKv("connect.db", err, kvs)
		}
		stream.EventKv("connect.db", kvs)
	} else {
		stream.Event("connect.db.skip")
	}

	return NewConnections(log, stream, dbConn, redisClient), nil
}

func NewConnections(l logging.Logger, s *health.Stream, db *db.Conn, r *redis.Client) *Connections {
	var c *cache.Cache
	if r != nil {
		c = cache.New(r)
	}

	return &Connections{
		logger: l,
		stream: s,

		db:    db,
		redis: r,
		cache: c,
	}
}

func (c Connections) Stream() *health.Stream { return c.stream }
func (c Connections) Logger() logging.Logger { return c.logger }
func (c Connections) DB() *db.Conn           { return c.db }
func (c Connections) Redis() *redis.Client   { return c.redis }
func (c Connections) Cache() *cache.Cache    { return c.cache }

func (c Connections) Close() error {
	errs := wrappers.Errs{}
	errs.Add(c.db.Close(context.Background()))
	if c.redis != nil {
		errs.Add(c.redis.Close())
	}
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
