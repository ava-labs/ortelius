package avm

import (
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

var (
	defaultTimeout = 1 * time.Minute
)

// AVM consumes for the AVM and writes to a service accumulator
type AVM struct {
	log        logging.Logger
	consumer   *kafka.Consumer
	serviceAcc services.Accumulator
	topic      string
}

// Initialize prepares the consumer for listening
func (c *AVM) Initialize(log logging.Logger, conf *cfg.ClientConfig) error {
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

	c.serviceAcc, err = services.NewRedisIndex(&conf.Redis)
	if err != nil {
		return err
	}

	return err
}

// Close closes the consumer
func (c *AVM) Close() error {
	return c.consumer.Close()
}

// ProcessNextMessage waits for a new message and adds it to the services
func (c *AVM) ProcessNextMessage() error {
	msg, err := c.consumer.ReadMessage(defaultTimeout)
	if err != nil {
		return err
	}

	txID, err := ids.ToID(msg.Key)
	if err != nil {
		return err
	}

	err = c.serviceAcc.AddTx(txID, msg.Value)
	if err != nil {
		return err
	}

	c.log.Info("Wrote message: %s", txID.String())
	return nil
}
