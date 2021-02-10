package services

import (
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
	Services                   cfg.Services
	Log                        logging.Logger
	Persist                    Persist
	Features                   map[string]struct{}
	BalanceAccumulatorManager  *BalanceAccumulatorManager
	IsAccumulateBalanceIndexer bool
	IsAccumulateBalanceReader  bool
	GenesisContainer           *GenesisContainer
}

func (s *Control) Init(networkID uint32) error {
	if _, ok := s.Features["accumulate_balance_indexer"]; ok {
		s.Log.Info("enable feature accumulate_balance_indexer")
		s.IsAccumulateBalanceIndexer = true

		// reader will work only if we enable indexer.
		if _, ok := s.Features["accumulate_balance_reader"]; ok {
			s.Log.Info("enable feature accumulate_balance_reader")
			s.IsAccumulateBalanceReader = true
		}
	}
	s.BalanceAccumulatorManager = &BalanceAccumulatorManager{}

	var err error
	s.GenesisContainer, err = NewGenesisContainer(networkID)
	if err != nil {
		return err
	}

	return nil
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

func (s *Control) DatabaseOnly() (*Connections, error) {
	c, err := NewDBFromConfig(s.Services, false)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(5 * time.Minute)
	c.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, nil
}

func (s *Control) Database() (*Connections, error) {
	c, err := NewConnectionsFromConfig(s.Services, false)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(5 * time.Minute)
	c.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, nil
}

func (s *Control) DatabaseRO() (*Connections, error) {
	c, err := NewConnectionsFromConfig(s.Services, true)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(5 * time.Minute)
	c.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, nil
}
