// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"context"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gocraft/dbr"
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
		redisClient, err = NewRedisConn(&redis.Options{
			DB:       conf.Redis.DB,
			Addr:     conf.Redis.Addr,
			Password: conf.Redis.Password,
		})
		if err != nil {
			return nil, err
		}
	}

	if conf.DB != nil {
		dbConn, err = db.New(stream, *conf.DB)
		if err != nil {
			return nil, err
		}
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
