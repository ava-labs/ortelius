// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/stream/record"
)

const WriteBufferSize = 2500

var _ io.Writer = &writeBuffer{}

type writeBuffer struct {
	size int

	writer      *kafka.Writer
	buffer      chan (*kafka.Message)
	localBuffer []kafka.Message

	lastFlush time.Time

	stopCh chan (struct{})
	doneCh chan (struct{})
}

func newWriteBuffer(brokers []string, topic string) *writeBuffer {
	wb := &writeBuffer{
		size: WriteBufferSize,

		buffer: make(chan *kafka.Message, WriteBufferSize*2),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}),

		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go wb.loop()

	return wb
}

func (wb *writeBuffer) Write(msg []byte) (int, error) {
	wb.buffer <- &kafka.Message{
		Key:   hashing.ComputeHash256(msg),
		Value: record.Marshal(msg),
	}
	return len(msg), nil
}

func (wb *writeBuffer) loop() {
	flushTicker := time.NewTicker(5 * time.Second)
	wb.localBuffer = make([]kafka.Message, 0, wb.size)

	flush := func() {
		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(writeTimeout))
		defer cancelFn()

		if err := wb.writer.WriteMessages(ctx, wb.localBuffer...); err != nil {
			log.Print("Error writing to kafka:", err.Error())
		}

		wb.lastFlush = time.Now()
		wb.localBuffer = make([]kafka.Message, 0, 2500)
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
		case msg := <-wb.buffer:
			wb.localBuffer = append(wb.localBuffer, *msg)
			if len(wb.localBuffer) >= wb.size {
				flush()
			}
		case <-flushTicker.C:
			if wb.lastFlush.Add(5 * time.Second).Before(time.Now()) {
				flush()
			}
		}
	}
}

func (wb *writeBuffer) close() error {
	close(wb.buffer)
	close(wb.stopCh)
	<-wb.doneCh
	return wb.writer.Close()
}
