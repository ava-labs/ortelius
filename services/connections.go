// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"github.com/go-redis/redis"
	"github.com/gocraft/dbr"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
)

type Connections struct {
	redis  *redis.Client
	stream *health.Stream
	db     *dbr.Connection
}

func NewConnections(conf cfg.ServiceConfig) (*Connections, error) {
	c := &Connections{stream: health.NewStream()}
	var err error

	if conf.Redis != nil {
		c.redis, err = newRedisConn(conf.Redis)
		if err != nil {
			return nil, err
		}
	}

	if conf.DB != nil {
		c.db, err = newDBConn(c.stream, *conf.DB)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c Connections) Redis() *redis.Client   { return c.redis }
func (c Connections) Stream() *health.Stream { return c.stream }
func (c Connections) DB() *dbr.Connection    { return c.db }

func newRedisConn(opts *redis.Options) (*redis.Client, error) {
	client := redis.NewClient(opts)

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func newDBConn(stream *health.Stream, conf cfg.DBConfig) (*dbr.Connection, error) {
	conn, err := dbr.Open(conf.Driver, conf.DSN, stream)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return conn, err
}
