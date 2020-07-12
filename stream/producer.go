// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/binary"
	"path"
	"time"

	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol"
	"nanomsg.org/go/mangos/v2/protocol/sub"
	_ "nanomsg.org/go/mangos/v2/transport/ipc" // Register transport

	"github.com/ava-labs/ortelius/cfg"
)

// producer reads from the socket and writes to the event stream
type producer struct {
	chainID     string
	sock        protocol.Socket
	binFilterFn binFilterFn
	writeBuffer *writeBuffer
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, _ uint32, _ string, chainID string) (*producer, error) {
	p := &producer{
		chainID:     chainID,
		binFilterFn: newBinFilterFn(conf.Filter.Min, conf.Filter.Max),
		writeBuffer: newWriteBuffer(conf.Brokers, chainID),
	}

	var err error
	p.sock, err = createIPCSocket("ipc://" + path.Join(conf.Producer.IPCRoot, chainID) + ".ipc")
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewProducerProcessor creates a producer as a Processor
func NewProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID)
}

// Close shuts down the producer
func (p *producer) Close() error {
	return p.writeBuffer.close()
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

	_, err = p.writeBuffer.Write(rawMsg)
	return err
}

func (p *producer) Write(msg []byte) (int, error) {
	return p.writeBuffer.Write(msg)
}

// receive reads bytes from the IPC socket
func (p *producer) receive(ctx context.Context) ([]byte, error) {
	deadline, _ := ctx.Deadline()

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
