// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"os"

	"github.com/go-redis/redis"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/db"
)

type Connections struct {
	redis  *redis.Client
	stream *health.Stream
	db     *dbr.Connection
}

func NewConnectionsFromConfig(conf cfg.Services) (*Connections, error) {
	stream := NewStream()

	var (
		err         error
		dbConn      *dbr.Connection
		redisClient *redis.Client
	)
	if conf.Redis != nil {
		redisClient, err = newRedisConn(&redis.Options{
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
	return &Connections{
		stream: stream,
		db:     dbConn,
		redis:  redisClient,
	}
}

func (c Connections) Redis() *redis.Client   { return c.redis }
func (c Connections) Stream() *health.Stream { return c.stream }
func (c Connections) DB() *dbr.Connection    { return c.db }

func NewStream() *health.Stream {
	s := health.NewStream()
	s.AddSink(&health.WriterSink{Writer: os.Stdout})
	return s
}

func newRedisConn(opts *redis.Options) (*redis.Client, error) {
	client := redis.NewClient(opts)

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	return client, nil
}
