// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/go-redis/redis/v8"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/cache"
	"github.com/ava-labs/ortelius/services/db"
)

type Connections struct {
	stream        *health.Stream
	quietStream   *health.Stream
	streamDBDedup *health.Stream

	db    *db.Conn
	redis *redis.Client
	cache *cache.Cache
}

func NewConnectionsFromConfig(conf cfg.Services, ro bool) (*Connections, error) {
	// Always create a stream and log
	stream := NewStream()
	quietStream := NewQuietStream()
	streamDBDedup := NewStreamDBDups()

	// Create db and redis connections if configured
	var (
		err         error
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
		// Create connection
		dbConn, err = db.New(stream, *conf.DB, ro)
		if err != nil {
			return nil, stream.EventErrKv("connect.db", err, kvs)
		}
		stream.EventKv("connect.db", kvs)
	} else {
		stream.Event("connect.db.skip")
	}

	return NewConnections(stream, quietStream, streamDBDedup, dbConn, redisClient), nil
}

func NewDBFromConfig(conf cfg.Services, ro bool) (*Connections, error) {
	// Always create a stream and log
	stream := NewStream()
	quietStream := NewQuietStream()
	streamDBDedup := NewStreamDBDups()

	// Create db and redis connections if configured
	var (
		dbConn *db.Conn
		err    error
	)

	if conf.DB != nil || conf.DB.Driver == db.DriverNone {
		kvs := health.Kvs{}
		// Create connection
		dbConn, err = db.New(stream, *conf.DB, ro)
		if err != nil {
			return nil, stream.EventErrKv("connect.db", err, kvs)
		}
	} else {
		return nil, fmt.Errorf("invalid database")
	}

	return NewConnections(stream, quietStream, streamDBDedup, dbConn, nil), nil
}

func NewConnections(s *health.Stream, quietStream *health.Stream, streamDBDedup *health.Stream, db *db.Conn, r *redis.Client) *Connections {
	var c *cache.Cache
	if r != nil {
		c = cache.New(r)
	}

	return &Connections{
		stream:        s,
		quietStream:   quietStream,
		streamDBDedup: streamDBDedup,

		db:    db,
		redis: r,
		cache: c,
	}
}

func (c Connections) Stream() *health.Stream        { return c.stream }
func (c Connections) QuietStream() *health.Stream   { return c.quietStream }
func (c Connections) StreamDBDedup() *health.Stream { return c.streamDBDedup }

func (c Connections) DB() *db.Conn         { return c.db }
func (c Connections) Redis() *redis.Client { return c.redis }
func (c Connections) Cache() *cache.Cache  { return c.cache }

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

func NewStreamDBDups() *health.Stream {
	s := health.NewStream()
	s.AddSink(&WriterSinkExcludeDBDups{Writer: os.Stdout})
	return s
}

func NewQuietStream() *health.Stream {
	return health.NewStream()
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

func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

type WriterSinkExcludeDBDups struct {
	io.Writer
}

func (s *WriterSinkExcludeDBDups) EmitEvent(job string, event string, kvs map[string]string) {
	if event == "dbr.begin" || event == "dbr.commit" {
		return
	}
	var b bytes.Buffer
	b.WriteRune('[')
	b.WriteString(timestamp())
	b.WriteString("]: job:")
	b.WriteString(job)
	b.WriteString(" event:")
	b.WriteString(event)
	writeMapConsistently(&b, kvs)
	b.WriteRune('\n')
	_, _ = s.Writer.Write(b.Bytes())
}

func (s *WriterSinkExcludeDBDups) EmitEventErr(job string, event string, inputErr error, kvs map[string]string) {
	errmsg := inputErr.Error()
	if strings.HasPrefix(errmsg, "Error 1062: Duplicate entry") {
		return
	}

	var b bytes.Buffer
	b.WriteRune('[')
	b.WriteString(timestamp())
	b.WriteString("]: job:")
	b.WriteString(job)
	b.WriteString(" event:")
	b.WriteString(event)
	b.WriteString(" err:")
	b.WriteString(inputErr.Error())
	writeMapConsistently(&b, kvs)
	b.WriteRune('\n')
	_, _ = s.Writer.Write(b.Bytes())
}

func (s *WriterSinkExcludeDBDups) EmitTiming(job string, event string, nanos int64, kvs map[string]string) {
	var b bytes.Buffer
	b.WriteRune('[')
	b.WriteString(timestamp())
	b.WriteString("]: job:")
	b.WriteString(job)
	b.WriteString(" event:")
	b.WriteString(event)
	b.WriteString(" time:")
	writeNanoseconds(&b, nanos)
	writeMapConsistently(&b, kvs)
	b.WriteRune('\n')
	_, _ = s.Writer.Write(b.Bytes())
}

func (s *WriterSinkExcludeDBDups) EmitGauge(job string, event string, value float64, kvs map[string]string) {
	var b bytes.Buffer
	b.WriteRune('[')
	b.WriteString(timestamp())
	b.WriteString("]: job:")
	b.WriteString(job)
	b.WriteString(" event:")
	b.WriteString(event)
	b.WriteString(" gauge:")
	fmt.Fprintf(&b, "%g", value)
	writeMapConsistently(&b, kvs)
	b.WriteRune('\n')
	_, _ = s.Writer.Write(b.Bytes())
}

func (s *WriterSinkExcludeDBDups) EmitComplete(job string, status health.CompletionStatus, nanos int64, kvs map[string]string) {
	var b bytes.Buffer
	b.WriteRune('[')
	b.WriteString(timestamp())
	b.WriteString("]: job:")
	b.WriteString(job)
	b.WriteString(" status:")
	b.WriteString(status.String())
	b.WriteString(" time:")
	writeNanoseconds(&b, nanos)
	writeMapConsistently(&b, kvs)
	b.WriteRune('\n')
	_, _ = s.Writer.Write(b.Bytes())
}

func writeMapConsistently(b *bytes.Buffer, kvs map[string]string) {
	if kvs == nil {
		return
	}
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	keysLenMinusOne := len(keys) - 1

	b.WriteString(" kvs:[")
	for i, k := range keys {
		b.WriteString(k)
		b.WriteRune(':')
		b.WriteString(kvs[k])

		if i != keysLenMinusOne {
			b.WriteRune(' ')
		}
	}
	b.WriteRune(']')
}

func writeNanoseconds(b *bytes.Buffer, nanos int64) {
	switch {
	case nanos > 2000000:
		fmt.Fprintf(b, "%d ms", nanos/1000000)
	case nanos > 2000:
		fmt.Fprintf(b, "%d μs", nanos/1000)
	default:
		fmt.Fprintf(b, "%d ns", nanos)
	}
}
