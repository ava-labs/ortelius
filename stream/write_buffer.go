// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/stream/record"
)

const defaultWriteBufferSize = 2500

var (
	defaultFlushInterval = 1 * time.Second

	_ io.Writer = &bufferedWriter{}
)

type bufferedWriter struct {
	writer *kafka.Writer
	buffer chan (*kafka.Message)

	stopCh chan (struct{})
	doneCh chan (struct{})
}

func newWriteBuffer(brokers []string, topic string) *bufferedWriter {
	size := defaultWriteBufferSize

	wb := &bufferedWriter{
		buffer: make(chan *kafka.Message, size*2),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go wb.loop(size, defaultFlushInterval)

	return wb
}

func (wb *bufferedWriter) Write(msg []byte) (int, error) {
	wb.buffer <- &kafka.Message{
		Key:   hashing.ComputeHash256(msg),
		Value: record.Marshal(msg),
	}
	return len(msg), nil
}

func (wb *bufferedWriter) loop(size int, flushInterval time.Duration) {
	var (
		lastFlush   = time.Now()
		flushTicker = time.NewTicker(flushInterval)
		localBuffer = make([]kafka.Message, 0, size)
	)

	flush := func() {
		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(writeTimeout))
		defer cancelFn()

		if err := wb.writer.WriteMessages(ctx, localBuffer...); err != nil {
			log.Print("Error writing to kafka:", err.Error())
		}

		lastFlush = time.Now()
		localBuffer = make([]kafka.Message, 0, 2500)
	}

	defer func() {
		flushTicker.Stop()
		flush()
		close(wb.doneCh)
	}()

	for {
		select {
		case <-wb.stopCh:
			return
		case msg, ok := <-wb.buffer:
			if ok {
				localBuffer = append(localBuffer, *msg)
			}

			// If the buffer is full we flush and exert back-pressure
			if len(localBuffer) >= size {
				flush()
			}
		case <-flushTicker.C:
			// Don't flush if we've flushed recently from a full buffer
			if time.Now().After(lastFlush.Add(flushInterval)) {
				flush()
			}
		}
	}
}

func (wb *bufferedWriter) close() error {
	close(wb.buffer)
	close(wb.stopCh)
	<-wb.doneCh
	return wb.writer.Close()
}
