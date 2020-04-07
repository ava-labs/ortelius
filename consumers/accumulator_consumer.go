// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/avm_index"
)

// accumulatorConsumer consumes events from a single Kafka topic and writes to a
// service accumulator
type accumulatorConsumer struct {
	log      logging.Logger
	consumer *kafka.Consumer
	accs     []services.Accumulator
}

// NewAccumulatorConsumer creates an accumulatorConsumer based on the passed in
// configuration
func NewAccumulatorConsumer(conf *cfg.ClientConfig, log logging.Logger) (*accumulatorConsumer, error) {
	accs, err := createAccumulators(conf.ServiceConfig)
	if err != nil {
		return nil, err
	}

	c := &accumulatorConsumer{
		log:  log,
		accs: accs,
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
	msg, err := getNextMessage(c.consumer)
	if err != nil {
		return err
	}

	for _, acc := range c.accs {
		if err = acc.AddTx(msg); err != nil {
			return err
		}
	}

	c.log.Info("Wrote tx: %s", msg.id.String())
	return nil
}

func createAccumulators(conf cfg.ServiceConfig) ([]services.Accumulator, error) {
	avmIndex, err := avm_index.New(conf)
	if err != nil {
		return nil, err
	}

	return []services.Accumulator{avmIndex}, nil
}
