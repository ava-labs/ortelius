// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/binary"
	"time"

	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol"
	"nanomsg.org/go/mangos/v2/protocol/sub"
	_ "nanomsg.org/go/mangos/v2/transport/ipc" // Register transport

	"github.com/ava-labs/ortelius/cfg"
)

// producer reads from the socket and writes to the event stream
type Producer struct {
	chainID     string
	eventType   EventType
	sock        protocol.Socket
	binFilterFn binFilterFn
	writeBuffer *writeBuffer
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, networkID uint32, _ string, chainID string, eventType EventType) (*Producer, error) {
	p := &Producer{
		chainID:     chainID,
		eventType:   eventType,
		binFilterFn: newBinFilterFn(conf.Filter.Min, conf.Filter.Max),
		writeBuffer: newWriteBuffer(conf.Brokers, getTopicName(networkID, chainID, eventType)),
	}

	var err error
	p.sock, err = createIPCSocket(getSocketName(conf.Producer.IPCRoot, networkID, chainID, eventType))
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewConsensusProducerProcessor creates a producer for consensus events
func NewConsensusProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID, EventTypeConsensus)
}

// NewDecisionsProducerProcessor creates a producer for decision events
func NewDecisionsProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID, EventTypeDecisions)
}

// Close shuts down the producer
func (p *Producer) Close() error {
	return p.writeBuffer.close()
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *Producer) ProcessNextMessage(ctx context.Context) error {
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

func (p *Producer) Write(msg []byte) (int, error) {
	return p.writeBuffer.Write(msg)
}

// receive reads bytes from the IPC socket
func (p *Producer) receive(ctx context.Context) ([]byte, error) {
	deadline, _ := ctx.Deadline()

	err := p.sock.SetOption(mangos.OptionRecvDeadline, time.Until(deadline))
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
