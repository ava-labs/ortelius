// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanchego/utils/logging"

	"github.com/ava-labs/avalanchego/ipcs/socket"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

// producer reads from the socket and writes to the event stream
type Producer struct {
	chainID                 string
	eventType               EventType
	sock                    *socket.Client
	writeBuffer             *bufferedWriter
	log                     logging.Logger
	metricProcessedCountKey string
	metricWrittenCountKey   string
	metricSuccessCountKey   string
	metricFailureCountKey   string
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, _ string, chainID string, eventType EventType) (*Producer, error) {
	p := &Producer{
		chainID:                 chainID,
		eventType:               eventType,
		writeBuffer:             newBufferedWriter(conf.Log, conf.Brokers, GetTopicName(conf.NetworkID, chainID, eventType)),
		log:                     conf.Log,
		metricProcessedCountKey: fmt.Sprintf("records_processed_%s", eventType),
		metricWrittenCountKey:   fmt.Sprintf("records_written_%s", eventType),
		metricSuccessCountKey:   fmt.Sprintf("records_success_%s", eventType),
		metricFailureCountKey:   fmt.Sprintf("records_failure_%s", eventType),
	}
	metrics.Prometheus.CounterInit(p.metricProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(p.metricWrittenCountKey, "records written")
	metrics.Prometheus.CounterInit(p.metricSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(p.metricFailureCountKey, "records failure")

	var err error
	p.sock, err = socket.Dial(getSocketName(conf.Producer.IPCRoot, conf.NetworkID, chainID, eventType))
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewConsensusProducerProcessor creates a producer for consensus events
func NewConsensusProducerProcessor(conf cfg.Config, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, chainVM, chainID, EventTypeConsensus)
}

// NewDecisionsProducerProcessor creates a producer for decision events
func NewDecisionsProducerProcessor(conf cfg.Config, chainVM string, chainID string) (Processor, error) {
	return NewProducer(conf, chainVM, chainID, EventTypeDecisions)
}

// Close shuts down the producer
func (p *Producer) Close() error {
	return p.writeBuffer.close()
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *Producer) ProcessNextMessage(_ context.Context) error {
	rawMsg, err := p.sock.Recv()
	if err != nil {
		p.log.Error("sock.Recv: %s", err.Error())
		return err
	}

	p.writeBuffer.Write(rawMsg)

	err = metrics.Prometheus.CounterInc(p.metricProcessedCountKey)
	if err != nil {
		p.log.Error("prometheus.CounterInc %s", err)
	}

	return nil
}

func (p *Producer) Failure() {
	err := metrics.Prometheus.CounterInc(p.metricFailureCountKey)
	if err != nil {
		p.log.Error("prometheus.CounterInc %s", err)
	}
}

func (p *Producer) Success() {
	err := metrics.Prometheus.CounterInc(p.metricSuccessCountKey)
	if err != nil {
		p.log.Error("prometheus.CounterInc %s", err)
	}
}
