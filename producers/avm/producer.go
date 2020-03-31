// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"nanomsg.org/go/mangos/v2/protocol"

	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/record"
)

// AVM produces for the AVM, taking messages from the IPC socket and writing
// formatting txs to Kafka
type AVM struct {
	log            logging.Logger
	sock           protocol.Socket
	filter         Filter
	topicPartition kafka.TopicPartition
	producer       *kafka.Producer
}

// Initialize the producer using the configs passed as an argument
func (p *AVM) Initialize(log logging.Logger, conf *cfg.ClientConfig, sock protocol.Socket) error {
	p.log = log
	p.sock = sock
	p.filter = Filter{}
	if err := p.filter.Initialize(conf.Filter); err != nil {
		return err
	}

	// Set the Kafka config and start the producer
	chainIDStr := conf.ChainID.String()
	p.topicPartition = kafka.TopicPartition{
		Topic:     &chainIDStr,
		Partition: kafka.PartitionAny,
	}
	var err error
	if p.producer, err = kafka.NewProducer(conf.Kafka); err != nil {
		return err
	}

	log.Info("Initialized producer with chainID=%s and filter=%s", conf.ChainID, conf.Filter)

	return nil
}

// Close shuts down the producer
func (p *AVM) Close() error {
	p.producer.Close()
	return nil
}

// Events returns delivery events channel
func (p *AVM) Events() chan kafka.Event {
	return p.producer.Events()
}

// ProcessNextMessage takes in a message from the IPC socket and writes it to
// Kafka
func (p *AVM) ProcessNextMessage() error {
	// Get bytes from IPC
	msg, err := p.sock.Recv()
	if err != nil {
		return err
	}

	// If we match the filter then stop now
	if p.filter.Filter(msg) {
		return nil
	}

	// Wrap message and send to Kafka
	return p.producer.Produce(&kafka.Message{
		Value:          record.Marshal(msg),
		Key:            hashing.ComputeHash256(msg),
		TopicPartition: p.topicPartition,
	}, nil)
}
