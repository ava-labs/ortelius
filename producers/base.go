package producers

import (
	"github.com/ava-labs/ortelius/producers/avm"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// BaseType is a generic interface for producers
type BaseType interface {
	Initialize(string, string) error
	Close()
	Events() chan kafka.Event
	Produce([]byte) error
}

// Select chooses the correct producer based on the dataType flag
func Select(dataType string) BaseType {
	var p BaseType
	switch dataType {
	case "avm":
		p = &avm.AVM{}
	}
	return p
}
