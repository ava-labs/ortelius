package producers

import (
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/ortelius/producers/avm"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"nanomsg.org/go/mangos/v2/protocol"
)

// BaseType is a generic interface for producers
type BaseType interface {
	Initialize(logging.Logger, kafka.ConfigMap, protocol.Socket) error
	Accept() error
	Close() error
	Events() chan kafka.Event
}

// Select chooses the correct producer based on the dataType flag
func Select(dataType string) BaseType {
	var p BaseType
	switch dataType {
	case "avm":
		p = &avm.AVM{}
	default:
		p = nil
	}
	return p
}
