// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/binary"

	"github.com/ava-labs/avalanchego/ipcs/socket"
	"github.com/ava-labs/avalanchego/utils/logging"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/ortelius/cfg"
)

// producer reads from the socket and writes to the event stream
type Producer struct {
	chainID     string
	eventType   EventType
	sock        *socket.Client
	binFilterFn binFilterFn
	writeBuffer *writeBuffer
	msgchan     chan<- []byte
	waitgroup   sync.WaitGroup
	cancel      context.CancelFunc
	successes   uint64
	log         logging.Logger
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, networkID uint32, _ string, chainID string, eventType EventType, log logging.Logger) (*Producer, error) {
	msgchan := make(chan []byte)
	ctx, cancel := context.WithCancel(context.Background())

	p := &Producer{
		chainID:     chainID,
		eventType:   eventType,
		binFilterFn: newBinFilterFn(conf.Filter.Min, conf.Filter.Max),
		writeBuffer: newWriteBuffer(conf.Brokers, GetTopicName(networkID, chainID, eventType)),
		msgchan:     msgchan,
		waitgroup:   sync.WaitGroup{},
		cancel:      cancel,
		log:         log,
	}

	var err error
	p.sock, err = socket.Dial(getSocketName(conf.Producer.IPCRoot, networkID, chainID, eventType))
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(conf.QueueSizeProducer); i++ {
		p.waitgroup.Add(1)
		go p.ProcessWorker(ctx, msgchan)
	}

	return p, nil
}

// NewConsensusProducerProcessor creates a producer for consensus events
func NewConsensusProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string, log logging.Logger) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID, EventTypeConsensus, log)
}

// NewDecisionsProducerProcessor creates a producer for decision events
func NewDecisionsProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string, log logging.Logger) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID, EventTypeDecisions, log)
}

// Close shuts down the producer
func (p *Producer) Close() error {
	p.cancel()
	p.waitgroup.Wait()
	return p.writeBuffer.close()
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *Producer) ProcessNextMessage(_ context.Context) error {
	rawMsg, err := p.sock.Recv()
	if err != nil {
		p.log.Error("sock.Recv: %s", err.Error())
		return err
	}

	p.msgchan <- rawMsg

	return nil
}

func (p *Producer) Successes() uint64 {
	return p.successes
}

func (p *Producer) ProcessWorker(ctx context.Context, msgchan <- chan []byte) {
	defer p.waitgroup.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case rawMsg := <- msgchan:
			if !p.binFilterFn(rawMsg) {
				if _, err := p.writeBuffer.Write(rawMsg); err != nil {
					p.log.Error("writeBuffer.Write: %s", err.Error())
				} else {
					atomic.AddUint64(&p.successes, 1)
				}
			}
		}
	}
}


func (p *Producer) Write(msg []byte) (int, error) {
	return p.writeBuffer.Write(msg)
}

type binFilterFn func([]byte) bool

// newBinFilterFn returns a binFilterFn with the given range
func newBinFilterFn(min uint32, max uint32) binFilterFn {
	return func(input []byte) bool {
		value := binary.LittleEndian.Uint32(input[:4])
		return !(value < min || value > max)
	}
}
