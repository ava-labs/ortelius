// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

// accumulatorConsumer consumes events from a single Kafka topic and writes to a
// service accumulator
type accumulatorConsumer struct {
	log        logging.Logger
	topic      string
	parser     txParser
	consumer   *kafka.Consumer
	serviceAcc services.Accumulator
}

// NewAccumulatorConsumer creates an accumulatorConsumer based on the passed in
// configuration
func NewAccumulatorConsumer(conf *cfg.ClientConfig, log logging.Logger) (*accumulatorConsumer, error) {
	parser, err := newParser(conf.DataType, conf.ChainID, conf.NetworkID)
	if err != nil {
		return nil, err
	}
	return newAccumulatorConsumerForParser(conf, log, parser)
}

// newAccumulatorConsumerForParser creates an accumulatorConsumer for the passed
// in parser
func newAccumulatorConsumerForParser(conf *cfg.ClientConfig, log logging.Logger, parser txParser) (*accumulatorConsumer, error) {
	c := &accumulatorConsumer{
		log:    log,
		topic:  conf.ChainID.String(),
		parser: parser,
	}

	// Create consumer for the topic
	var err error
	if c.consumer, err = kafka.NewConsumer(&conf.Kafka); err != nil {
		return nil, err
	}

	if err = c.consumer.Subscribe(c.topic, nil); err != nil {
		return nil, err
	}

	// Create service accumulator to send txs to
	if c.serviceAcc, err = services.NewRedisIndex(&conf.Redis); err != nil {
		return nil, err
	}

	return c, nil
}

// Close closes the consumer
func (c *accumulatorConsumer) Close() error {
	return c.consumer.Close()
}

// ProcessNextMessage waits for a new message and adds it to the services
func (c *accumulatorConsumer) ProcessNextMessage() error {
	// Read in a message and parse into a snowstorm.Tx
	txID, txPayload, err := readNextTxBytes(c.consumer)
	if err != nil {
		return err
	}

	snowstormTx, err := c.parser(txPayload)
	if err != nil {
		return err
	}

	// Get indexable bytes and send to the service accumulator
	indexableBytes, err := toIndexableBytes(snowstormTx)
	if err != nil {
		return err
	}

	err = c.serviceAcc.AddTx(txID, indexableBytes)
	if err != nil {
		return err
	}

	c.log.Info("Wrote tx: %s", txID.String())
	return nil
}
