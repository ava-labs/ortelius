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
	log         logging.Logger
	parser      txParser
	consumer    *kafka.Consumer
	serviceAccs []services.Accumulator
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
	serviceAccs, err := createServices(conf.ServiceConfig)
	if err != nil {
		return nil, err
	}

	c := &accumulatorConsumer{
		log:         log,
		parser:      parser,
		serviceAccs: serviceAccs,
	}

	// Create consumer for the topic
	if c.consumer, err = kafka.NewConsumer(conf.Kafka); err != nil {
		return nil, err
	}
	if err = c.consumer.Subscribe(conf.ChainID.String(), nil); err != nil {
		return nil, err
	}

	log.Info("Initialized Kafka consumer for topic %s at %s", conf.ChainID.String(), (*conf.Kafka)["bootstrap.servers"])

	return c, nil
}

// Close closes the consumer
func (c *accumulatorConsumer) Close() error {
	return c.consumer.Close()
}

// ProcessNextMessage waits for a new message and adds it to the services
func (c *accumulatorConsumer) ProcessNextMessage() error {
	// Read in a message and parse into a snowstorm.Tx
	chainID, txID, txBody, err := readNextTxBytes(c.consumer)
	if err != nil {
		return err
	}

	snowstormTx, err := c.parser(txBody)
	if err != nil {
		return err
	}

	// Get indexable bytes and send to the service accumulators
	indexableBytes, err := toIndexableBytes(snowstormTx)
	if err != nil {
		return err
	}

	for _, acc := range c.serviceAccs {
		if err = acc.AddTx(chainID, txID, indexableBytes); err != nil {
			return err
		}
	}

	c.log.Info("Wrote tx: %s", txID.String())
	return nil
}

func createServices(conf cfg.ServiceConfig) ([]services.Accumulator, error) {
	conns, err := services.NewConnections(conf)
	if err != nil {
		return nil, err
	}

	return []services.Accumulator{
		services.NewRedisIndex(conns.Redis()),
		services.NewRDBMSIndex(conns.Stream(), conns.DB()),
	}, nil
}
