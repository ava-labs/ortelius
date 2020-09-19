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

const defaultBufferedWriterSize = 256

var defaultBufferedWriterFlushInterval = 1 * time.Second

var _ io.Writer = &bufferedWriter{}

// bufferedWriter takes in messages and writes them in batches to the backend.
type bufferedWriter struct {
	writer *kafka.Writer
	buffer chan (*kafka.Message)
	doneCh chan (struct{})
}

func newBufferedWriter(brokers []string, topic string) *bufferedWriter {
	size := defaultBufferedWriterSize

	wb := &bufferedWriter{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}),
		buffer: make(chan *kafka.Message),
		doneCh: make(chan struct{}),
	}

	go wb.loop(size, defaultBufferedWriterFlushInterval)

	return wb
}

// Write adds the message to the buffer. It implements the io.Writer interface.
func (wb *bufferedWriter) Write(msg []byte) (int, error) {
	wb.buffer <- &kafka.Message{
		Key:   hashing.ComputeHash256(msg),
		Value: record.Marshal(msg),
	}
	return len(msg), nil
}

// loop takes in messages from the buffer and commits them to Kafka when in
// batches
func (wb *bufferedWriter) loop(size int, flushInterval time.Duration) {
	var (
		lastFlush   = time.Now()
		flushTicker = time.NewTicker(flushInterval)

		bufferSize = 0
		buffer     = make([]kafka.Message, size)
	)

	flush := func() {
		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(writeTimeout))
		defer cancelFn()

		if err := wb.writer.WriteMessages(ctx, buffer[:bufferSize]...); err != nil {
			log.Print("Error writing to kafka:", err.Error())
		}

		bufferSize = 0
		lastFlush = time.Now()
	}

	defer func() {
		flushTicker.Stop()
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
			buffer[bufferSize] = *msg
			bufferSize++
		case <-flushTicker.C:
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
	<-wb.doneCh
	return wb.writer.Close()
}
