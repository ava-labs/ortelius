package avm

import (
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cache"
	"github.com/ava-labs/ortelius/cfg"
)

var (
	defaultTimeout = 1 * time.Minute
)

// AVM consumes for the AVM and writes to a cache accumulator
type AVM struct {
	log      logging.Logger
	consumer *kafka.Consumer
	cache    cache.Accumulator
	topic    string
}

// Initialize prepares the consumer for listening
func (c *AVM) Initialize(log logging.Logger, conf *cfg.Config) error {
	var err error
	c.log = log
	c.topic = conf.ChainID.String()

	if c.consumer, err = kafka.NewConsumer(&conf.Kafka); err != nil {
		c.log.Error("Error creating consumer: %s", err.Error())
		return err
	}

	if err = c.consumer.Subscribe(c.topic, nil); err != nil {
		c.log.Error("Error subscribing to topic: %s", err.Error())
		return err
	}

	c.cache, err = cache.NewRedisBackend(&conf.Redis)
	if err != nil {
		return err
	}

	return err
}

// Close closes the consumer
func (c *AVM) Close() error {
	return c.consumer.Close()
}

// Accept waits for a new message, and if it receives one it and processes it
func (c *AVM) Accept() error {
	msg, err := c.consumer.ReadMessage(defaultTimeout)
	if err != nil {
		return err
	}

	return handleMessage(c.cache, msg)
}

// handleMessage takes in a raw message from Kafka and writes it to the cache
func handleMessage(cacheAcc cache.Accumulator, msg *kafka.Message) error {
	id, err := ids.ToID(msg.Key)
	if err != nil {
		return err
	}

	err = cacheAcc.AddTx(id, msg.Value)
	if err != nil {
		return err
	}

	return nil
}
