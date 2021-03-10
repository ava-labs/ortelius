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
	GenesisContainer           *GenesisContainer
	IsAccumulateBalanceIndexer bool
	IsAccumulateBalanceReader  bool
	IsDisableBootstrap         bool
	IsAggregateCache           bool
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
	if _, ok := s.Features["disable_bootstrap"]; ok {
		s.IsDisableBootstrap = true
	}
	if _, ok := s.Features["aggregate_cache"]; ok {
		s.IsAggregateCache = true
	}
	var err error
	persist := NewPersist()
	s.BalanceAccumulatorManager, err = NewBalanceAccumulatorManager(persist, s)
	if err != nil {
		return err
	}

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
	c.DB().SetConnMaxIdleTime(10 * time.Second)
	return c, nil
}

func (s *Control) DatabaseRO() (*Connections, error) {
	c, err := NewConnectionsFromConfig(s.Services, true)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(10 * time.Second)
	return c, nil
}
