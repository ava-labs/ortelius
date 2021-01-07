package services

import (
	"sync"
	"time"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

const (
	MetricProduceProcessedCountKey = "produce_records_processed"
	MetricProduceSuccessCountKey   = "produce_records_success"
	MetricProduceFailureCountKey   = "produce_records_failure"

	MetricConsumeProcessedCountKey       = "consume_records_processed"
	MetricConsumeProcessMillisCounterKey = "consume_records_process_millis"
	MetricConsumeSuccessCountKey         = "consume_records_success"
	MetricConsumeFailureCountKey         = "consume_records_failure"
)

type Control struct {
	Services      cfg.Services
	Log           logging.Logger `json:"log"`
	lock          sync.Mutex
	connections   *Connections
	connectionsRO *Connections
	Persist       Persist
}

func (s *Control) InitProduceMetrics() {
	metrics.Prometheus.CounterInit(MetricProduceProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(MetricProduceSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(MetricProduceFailureCountKey, "records failure")
}

func (s *Control) InitConsumeMetrics() {
	metrics.Prometheus.CounterInit(MetricConsumeProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(MetricConsumeProcessMillisCounterKey, "records processed millis")
	metrics.Prometheus.CounterInit(MetricConsumeSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(MetricConsumeFailureCountKey, "records failure")
}

func (s *Control) Database() (*Connections, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.connections != nil {
		return s.connections, nil
	}
	c, err := NewConnectionsFromConfig(s.Services, false)
	if err != nil {
		return nil, err
	}
	s.connections = c
	s.connections.DB().SetMaxIdleConns(32)
	s.connections.DB().SetConnMaxIdleTime(5 * time.Minute)
	s.connections.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, err
}

func (s *Control) DatabaseRO() (*Connections, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.connectionsRO != nil {
		return s.connectionsRO, nil
	}
	c, err := NewConnectionsFromConfig(s.Services, true)
	if err != nil {
		return nil, err
	}
	s.connectionsRO = c
	s.connectionsRO.DB().SetMaxIdleConns(32)
	s.connectionsRO.DB().SetConnMaxIdleTime(5 * time.Minute)
	s.connectionsRO.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, err
}
