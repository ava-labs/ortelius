// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"fmt"

	"github.com/ava-labs/avalanchego/utils/wrappers"

	"github.com/ava-labs/avalanchego/ipcs/socket"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/metrics"
)

// producer reads from the socket and writes to the event stream
type Producer struct {
	id          string
	chainID     string
	eventType   EventType
	sock        *socket.Client
	writeBuffer *bufferedWriter
	sc          *services.Control

	// metrics
	metricProcessedCountKey string
	metricSuccessCountKey   string
	metricFailureCountKey   string
}

// NewProducer creates a producer using the given config
func NewProducer(sc *services.Control, conf cfg.Config, _ string, chainID string, eventType EventType) (*Producer, error) {
	sock, err := socket.Dial(getSocketName(conf.Producer.IPCRoot, conf.NetworkID, chainID, eventType))
	if err != nil {
		return nil, err
	}
	writer, err := newBufferedWriter(sc, GetTopicName(conf.NetworkID, chainID, eventType), conf.NetworkID, chainID)
	if err != nil {
		_ = sock.Close()
		return nil, err
	}

	p := &Producer{
		chainID:                 chainID,
		eventType:               eventType,
		writeBuffer:             writer,
		sc:                      sc,
		sock:                    sock,
		metricProcessedCountKey: fmt.Sprintf("produce_records_processed_%s_%s", chainID, eventType),
		metricSuccessCountKey:   fmt.Sprintf("produce_records_success_%s_%s", chainID, eventType),
		metricFailureCountKey:   fmt.Sprintf("produce_records_failure_%s_%s", chainID, eventType),
		id:                      fmt.Sprintf("producer %d %s %s", conf.NetworkID, chainID, eventType),
	}
	metrics.Prometheus.CounterInit(p.metricProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(p.metricSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(p.metricFailureCountKey, "records failure")
	sc.InitProduceMetrics()

	return p, nil
}

// NewConsensusProducerProcessor creates a producer for consensus events
func NewConsensusProducerProcessor(sc *services.Control, conf cfg.Config, chainVM string, chainID string, _ int, _ int) (Processor, error) {
	return NewProducer(sc, conf, chainVM, chainID, EventTypeConsensus)
}

// NewDecisionsProducerProcessor creates a producer for decision events
func NewDecisionsProducerProcessor(sc *services.Control, conf cfg.Config, chainVM string, chainID string, _ int, _ int) (Processor, error) {
	return NewProducer(sc, conf, chainVM, chainID, EventTypeDecisions)
}

// Close shuts down the producer
func (p *Producer) Close() error {
	p.sc.Log.Info("close %s", p.id)
	errs := wrappers.Errs{}
	if p.writeBuffer != nil {
		p.writeBuffer.close()
	}
	if p.sock != nil {
		errs.Add(p.sock.Close())
	}
	return errs.Err
}

func (p *Producer) ID() string {
	return p.id
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *Producer) ProcessNextMessage() error {
	rawMsg, err := p.sock.Recv()
	if err != nil {
		p.sc.Log.Error("sock.Recv: %s", err.Error())
		return err
	}

	p.writeBuffer.Write(rawMsg)

	_ = metrics.Prometheus.CounterInc(p.metricProcessedCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceProcessedCountKey)

	return nil
}

func (p *Producer) Failure() {
	err := metrics.Prometheus.CounterInc(p.metricFailureCountKey)
	if err != nil {
		p.sc.Log.Error("prometheus.CounterInc %s", err)
	}
}

func (p *Producer) Success() {
	err := metrics.Prometheus.CounterInc(p.metricSuccessCountKey)
	if err != nil {
		p.sc.Log.Error("prometheus.CounterInc %s", err)
	}
}
