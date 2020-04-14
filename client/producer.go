// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client/record"
	"github.com/segmentio/kafka-go"
	"nanomsg.org/go/mangos/v2/protocol"
)

var (
	defaultKafkaWriteTimeout = 10 * time.Second
)

type binFilterFn func([]byte) bool

// newBinFilterFn returns a binFilterFn with the given range
func newBinFilterFn(min uint32, max uint32) binFilterFn {
	return func(input []byte) bool {
		value := binary.LittleEndian.Uint32(input[:4])
		return !(value < min || value > max)
	}
}

// producer produces for the producer, taking messages from the IPC socket and writing
// formatting txs to Kafka
type producer struct {
	chainID     ids.ID
	sock        protocol.Socket
	binFilterFn binFilterFn
	writer      *kafka.Writer
}

// newProducer creates a producer using the given config
func newProducer(conf *cfg.ClientConfig, _ uint32, chainID ids.ID) (backend, error) {
	p := &producer{
		chainID:     chainID,
		binFilterFn: newBinFilterFn(conf.FilterConfig.Min, conf.FilterConfig.Max),
	}

	var err error
	p.sock, err = createIPCSocket("ipc://" + path.Join(conf.IPCRoot, chainID.String()) + ".ipc")
	if err != nil {
		return nil, err
	}

	fmt.Println("kafka brokers:", conf.KafkaConfig.Brokers)
	p.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  conf.KafkaConfig.Brokers,
		Topic:    chainID.String(),
		Balancer: &kafka.LeastBytes{},
	})

	return p, nil
}

// Close shuts down the producer
func (p *producer) Close() error {
	p.writer.Close()
	return nil
}

// ProcessNextMessage takes in a message from the IPC socket and writes it to
// Kafka
func (p *producer) ProcessNextMessage() (*message, error) {
	// Get bytes from IPC
	rawMsg, err := p.sock.Recv()
	if err != nil {
		return nil, err
	}

	// If we match the filter then stop now
	if p.binFilterFn(rawMsg) {
		return nil, nil
	}

	// Create a message object
	msg := &message{
		id:      ids.NewID(hashing.ComputeHash256Array(rawMsg)),
		chainID: p.chainID,
		body:    record.Marshal(rawMsg),
	}

	// Send message to Kafka
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultKafkaReadTimeout)
	defer cancelFn()
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Value: msg.body,
		Key:   msg.id.Bytes(),
	})
	if err != nil {
		return nil, err
	}

	fmt.Println("Wrote message to Kafka:", msg.id.String())
	fmt.Println(rawMsg)

	return msg, err
}
