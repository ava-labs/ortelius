package consumers

import (
	"github.com/ava-labs/gecko/utils/logging"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/consumers/avm"
)

// BaseType is a basic interface for consumers
type BaseType interface {
	Initialize(logging.Logger, kafka.ConfigMap) error
	Accept() error
	Close() error
}

// Select chooses the correct producer based on the dataType flag
func Select(dataType string) BaseType {
	var c BaseType
	switch dataType {
	case "avm":
		c = &avm.AVM{}
	default:
		c = nil
	}
	return c
}
