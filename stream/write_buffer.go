// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"time"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/segmentio/kafka-go"
)

const (
	defaultBufferedWriterSize         = 256
	defaultBufferedWriterMsgQueueSize = defaultBufferedWriterSize * 100
	defaultWriteTimeout               = 1 * time.Minute
	defaultWriteRetry                 = 10
	defaultWriteRetrySleep            = 1 * time.Second
)

var defaultBufferedWriterFlushInterval = 1 * time.Second

type BufferContainer struct {
	b []byte
}

// bufferedWriter takes in messages and writes them in batches to the backend.
type bufferedWriter struct {
	topic  string
	writer *kafka.Writer
	buffer chan (*BufferContainer)
	doneCh chan (struct{})
	sc     *services.Control

	// metrics
	metricSuccessCountKey       string
	metricFailureCountKey       string
	metricProcessMillisCountKey string
	flushTicker                 *time.Ticker
	conns                       *services.Connections
	chainID                     string
	networkID                   uint32
}

func newBufferedWriter(sc *services.Control, brokers []string, topic string, networkID uint32, chainID string) (*bufferedWriter, error) {
	size := defaultBufferedWriterSize

	wb := &bufferedWriter{
		topic: topic,
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			BatchBytes:   ConsumerMaxBytesDefault,
			BatchSize:    defaultBufferedWriterSize,
			WriteTimeout: defaultWriteTimeout,
			RequiredAcks: int(kafka.RequireAll),
		}),
		buffer:                      make(chan *BufferContainer, defaultBufferedWriterMsgQueueSize),
		doneCh:                      make(chan struct{}),
		sc:                          sc,
		metricSuccessCountKey:       "kafka_write_records_success",
		metricFailureCountKey:       "kafka_write_records_failure",
		metricProcessMillisCountKey: "kafka_write_records_process_millis",
		chainID:                     chainID,
		networkID:                   networkID,
	}

	metrics.Prometheus.CounterInit(wb.metricSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(wb.metricFailureCountKey, "records failure")
	metrics.Prometheus.CounterInit(wb.metricProcessMillisCountKey, "records processed millis")

	conns, err := wb.sc.DatabaseOnly()
	if err != nil {
		return nil, err
	}
	wb.conns = conns

	wb.flushTicker = time.NewTicker(defaultBufferedWriterFlushInterval)
	go wb.loop(size, defaultBufferedWriterFlushInterval)

	return wb, nil
}

// Write adds the message to the buffer.
func (wb *bufferedWriter) Write(msg []byte) {
	wb.buffer <- &BufferContainer{b: msg}
}

// loop takes in messages from the buffer and commits them to Kafka when in
// batches
func (wb *bufferedWriter) loop(size int, flushInterval time.Duration) {
	var (
		lastFlush = time.Now()

		bufferSize = 0
		buffer     = make([](*BufferContainer), size)
		buffer2    = make([]kafka.Message, size)
	)

	flush := func() error {
		defer func() { lastFlush = time.Now() }()

		if bufferSize == 0 {
			return nil
		}

		collectors := metrics.NewCollectors(
			metrics.NewCounterObserveMillisCollect(wb.metricProcessMillisCountKey),
			metrics.NewSuccessFailCounterAdd(wb.metricSuccessCountKey, wb.metricFailureCountKey, float64(bufferSize)),
		)
		defer func() {
			err := collectors.Collect()
			if err != nil {
				wb.sc.Log.Error("collectors.Collect: %s", err)
			}
		}()

		var err error

		if wb.sc.IsDBPoll {
			for _, b := range buffer[:bufferSize] {
				wp := &WorkPacket{b: b}
				err = wb.processWork(0, wp)
				if err != nil {
					break
				}
			}
		} else {
			for bpos, b := range buffer[:bufferSize] {
				// reset the message..
				buffer2[bpos] = kafka.Message{}
				buffer2[bpos].Value = b.b
				// compute hash before processing.
				buffer2[bpos].Key = hashing.ComputeHash256(buffer2[bpos].Value)
			}

			wm := func(bufmsg []kafka.Message, bufmsgsz int) error {
				ctx, cancelFn := context.WithTimeout(context.Background(), defaultWriteTimeout)
				defer cancelFn()

				return wb.writer.WriteMessages(ctx, bufmsg[:bufmsgsz]...)
			}

			for icnt := 0; icnt < defaultWriteRetry; icnt++ {
				err = wm(buffer2, bufferSize)
				if err == nil {
					break
				}
				wb.sc.Log.Warn("Error writing to kafka (retry):", err)
				time.Sleep(defaultWriteRetrySleep)
			}
		}

		if err != nil {
			collectors.Error()
			wb.sc.Log.Error("Error writing to kafka:", err)
		}

		bufferSize = 0

		return err
	}

	defer func() {
		wb.flushTicker.Stop()
		flush()
		close(wb.doneCh)
	}()

	for {
		select {
		case msg, ok := <-wb.buffer:
			if !ok {
				return
			}

			// If the buffer is full we must flush before we can add another message
			// This will exert backpressure
			if bufferSize >= size {
				flush()
			}

			// Add this message to the buffer and if it's full we flush and
			buffer[bufferSize] = msg
			bufferSize++
		case <-wb.flushTicker.C:
			// Don't flush if we've flushed recently from a full buffer
			if time.Now().After(lastFlush.Add(flushInterval)) {
				flush()
			}
		}
	}
}

// close stops the bufferedWriter and flushes any remaining items
func (wb *bufferedWriter) close() error {
	// Close buffer and wait for it to stop, flush, and signal back
	close(wb.buffer)
	wb.flushTicker.Stop()
	<-wb.doneCh
	if wb.conns != nil {
		_ = wb.conns.Close()
	}
	return wb.writer.Close()
}

type WorkPacket struct {
	b *BufferContainer
}

func (wb *bufferedWriter) processWork(_ int, workPacketI interface{}) error {
	wp, ok := workPacketI.(*WorkPacket)
	if !ok {
		return nil
	}

	var err error
	var id ids.ID
	id, err = ids.ToID(hashing.ComputeHash256(wp.b.b))
	if err != nil {
		return err
	}

	txPool := &services.TxPool{
		NetworkID:     wb.networkID,
		ChainID:       wb.chainID,
		MsgKey:        id.String(),
		Serialization: wp.b.b,
		Processed:     0,
		Topic:         wb.topic,
		CreatedAt:     time.Now(),
	}
	err = txPool.ComputeID()
	if err != nil {
		return err
	}

	wm := func(txPool *services.TxPool) error {
		job := wb.conns.Stream().NewJob("write-buffer")
		sess := wb.conns.DB().NewSessionForEventReceiver(job)

		ctx, cancelFn := context.WithTimeout(context.Background(), defaultWriteTimeout)
		defer cancelFn()

		return wb.sc.Persist.InsertTxPool(ctx, sess, txPool)
	}

	for icnt := 0; icnt < defaultWriteRetry; icnt++ {
		err = wm(txPool)
		if err == nil {
			break
		}
		wb.sc.Log.Warn("Error writing to db (retry):", err)
		time.Sleep(defaultWriteRetrySleep)
	}

	return err
}
