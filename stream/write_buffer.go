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

var (
	defaultBufferedWriterFlushInterval = 1 * time.Second

	// Enforce adherence to the io.Writer interface
	_ io.Writer = &bufferedWriter{}
)

// bufferedWriter takes in messages and writes them in batches to the backend.
type bufferedWriter struct {
	writer *kafka.Writer
	buffer chan (*kafka.Message)
	stopCh chan (struct{})
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
		buffer: make(chan *kafka.Message, size*2),
		stopCh: make(chan struct{}),
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
		localBuffer = make([]kafka.Message, 0, size)
	)

	flush := func() {
		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(writeTimeout))
		defer cancelFn()

		if err := wb.writer.WriteMessages(ctx, localBuffer...); err != nil {
			log.Print("Error writing to kafka:", err.Error())
		}

		lastFlush = time.Now()
		localBuffer = make([]kafka.Message, 0, size)
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
			if !ok {
				return
			}

			// Add this message to the buffer and if it's full we flush and
			// exert back-pressure
			localBuffer = append(localBuffer, *msg)
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

// close stops the bufferedWriter and flushes any remaining items
func (wb *bufferedWriter) close() error {
	close(wb.buffer)
	close(wb.stopCh)
	<-wb.doneCh
	return wb.writer.Close()
}
