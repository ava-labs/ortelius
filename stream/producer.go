// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"

	"github.com/ava-labs/avalanchego/ipcs/socket"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

// producer reads from the socket and writes to the event stream
type Producer struct {
	chainID     string
	eventType   EventType
	sock        *socket.Client
	writeBuffer *bufferedWriter
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, _ string, chainID string, eventType EventType) (*Producer, error) {
	p := &Producer{
		chainID:     chainID,
		eventType:   eventType,
		writeBuffer: newBufferedWriter(conf.Brokers, GetTopicName(conf.NetworkID, chainID, eventType)),
	}

	var err error
	p.sock, err = socket.Dial(getSocketName(conf.Producer.IPCRoot, conf.NetworkID, chainID, eventType))
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewConsensusProducerProcessor creates a producer for consensus events
func NewConsensusProducerProcessor(conf cfg.Config, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, chainVM, chainID, EventTypeConsensus)
}

// NewDecisionsProducerProcessor creates a producer for decision events
func NewDecisionsProducerProcessor(conf cfg.Config, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, chainVM, chainID, EventTypeDecisions)
}

// Close shuts down the producer
func (p *Producer) Close() error {
	return p.writeBuffer.close()
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *Producer) ProcessNextMessage(_ context.Context, log logging.Logger) error {
	rawMsg, err := p.sock.Recv()
	if err != nil {
		log.Error("sock.Recv: %s", err.Error())
		return err
	}

	if _, err = p.writeBuffer.Write(rawMsg); err != nil {
		log.Error("bufferedWriter.Write: %s", err.Error())
		return err
	}
	return nil
}

func (p *Producer) Write(msg []byte) (int, error) {
	return p.writeBuffer.Write(msg)
}
