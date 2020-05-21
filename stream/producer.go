// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/binary"
	"io"
	"path"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/segmentio/kafka-go"
	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol"
	"nanomsg.org/go/mangos/v2/protocol/sub"
	_ "nanomsg.org/go/mangos/v2/transport/ipc" // Register transport

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream/record"
)

// producer reads from the socket and writes to the event stream
type producer struct {
	chainID     string
	sock        protocol.Socket
	binFilterFn binFilterFn
	writer      *kafka.Writer
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, _ uint32, _ string, chainID string) (*producer, error) {
	p := &producer{
		chainID:     chainID,
		binFilterFn: newBinFilterFn(conf.Filter.Min, conf.Filter.Max),
	}

	var err error
	p.sock, err = createIPCSocket("ipc://" + path.Join(conf.Producer.IPCRoot, chainID) + ".ipc")
	if err != nil {
		return nil, err
	}

	p.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  conf.Brokers,
		Topic:    chainID,
		Balancer: &kafka.LeastBytes{},
	})

	return p, nil
}

// NewProducerProcessor creates a producer as a Processor
func NewProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID)
}

// Close shuts down the producer
func (p *producer) Close() error {
	return p.writer.Close()
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *producer) ProcessNextMessage(ctx context.Context) error {
	rawMsg, err := p.receive(ctx)
	if err != nil {
		return err
	}

	if p.binFilterFn(rawMsg) {
		return nil
	}

	_, err = p.Write(rawMsg)
	return err
}

var _ io.Writer = &producer{}

func (p *producer) Write(rawMsg []byte) (int, error) {
	// Create a Message object
	msgHash := hashing.ComputeHash256Array(rawMsg)
	msg := &Message{
		id:      ids.NewID(msgHash).String(),
		chainID: p.chainID,
		body:    record.Marshal(rawMsg),
	}

	// Send Message to Kafka
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(writeTimeout))
	defer cancelFn()

	kMsg := kafka.Message{Value: msg.body, Key: msgHash[:]}
	if err := p.writer.WriteMessages(ctx, kMsg); err != nil {
		return 0, err
	}

	return len(rawMsg), nil
}

func (p *producer) receive(ctx context.Context) ([]byte, error) {
	deadline, _ := ctx.Deadline()

	// Get bytes from IPC
	err := p.sock.SetOption(mangos.OptionRecvDeadline, deadline.Sub(time.Now()))
	if err != nil {
		return nil, err
	}

	rawMsg, err := p.sock.Recv()
	if err != nil {
		return nil, err
	}
	return rawMsg, nil
}

// createIPCSocket creates a new socket connection to the configured IPC URL
func createIPCSocket(url string) (protocol.Socket, error) {
	// Create and open a connection to the IPC socket
	sock, err := sub.NewSocket()
	if err != nil {
		return nil, err
	}

	if err = sock.Dial(url); err != nil {
		return nil, err
	}

	// Subscribe to all topics
	if err = sock.SetOption(mangos.OptionSubscribe, []byte("")); err != nil {
		return nil, err
	}

	return sock, nil
}

type binFilterFn func([]byte) bool

// newBinFilterFn returns a binFilterFn with the given range
func newBinFilterFn(min uint32, max uint32) binFilterFn {
	return func(input []byte) bool {
		value := binary.LittleEndian.Uint32(input[:4])
		return !(value < min || value > max)
	}
}
