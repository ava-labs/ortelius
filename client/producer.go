// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package client

import (
	"encoding/binary"

	"github.com/ava-labs/gecko/ids"
	"nanomsg.org/go/mangos/v2/protocol"

	"github.com/ava-labs/gecko/utils/hashing"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client/record"
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
	sock           protocol.Socket
	chainID        ids.ID
	topicPartition kafka.TopicPartition

	binFilterFn binFilterFn
	producer    *kafka.Producer
}

// newProducer creates a producer using the given config
func newProducer(conf *cfg.ClientConfig) (*producer, error) {
	sock, err := createIPCSocket(conf.IPCURL)
	if err != nil {
		return nil, err
	}

	kProducer, err := kafka.NewProducer(conf.Kafka)
	if err != nil {
		return nil, err
	}

	chainIDStr := conf.ChainID.String()
	return &producer{
		sock:    sock,
		chainID: conf.ChainID,
		topicPartition: kafka.TopicPartition{
			Topic:     &chainIDStr,
			Partition: kafka.PartitionAny,
		},

		binFilterFn: newBinFilterFn(conf.Filter.Min, conf.Filter.Max),
		producer:    kProducer,
	}, nil
}

// Close shuts down the producer
func (p *producer) Close() error {
	p.producer.Close()
	return nil
}

// Events returns delivery events channel
func (p *producer) Events() chan kafka.Event {
	return p.producer.Events()
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
	msgHash := hashing.ComputeHash256Array(rawMsg)
	msg := &message{
		id:      ids.NewID(msgHash),
		chainID: p.chainID,
		body:    record.Marshal(rawMsg),
	}

	// Send message to Kafka
	err = p.producer.Produce(&kafka.Message{
		Value:          msg.body,
		Key:            msg.id.Bytes(),
		TopicPartition: p.topicPartition,
	}, nil)
	if err != nil {
		return nil, err
	}

	return msg, err
}
