package servicesctrl

import (
	"time"

	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/balance"
	"github.com/ava-labs/ortelius/services/idb"
	"github.com/ava-labs/ortelius/services/metrics"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/services/servicesgenesis"
	"github.com/ava-labs/ortelius/utils/indexedlist"
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

type LocalTxPoolJob struct {
	TxPool *idb.TxPool
	Errs   *avlancheGoUtils.AtomicInterface
}

type Control struct {
	Services                   cfg.Services
	ServicesCfg                cfg.Config
	Chains                     map[string]cfg.Chain `json:"chains"`
	Log                        logging.Logger
	Persist                    idb.Persist
	Features                   map[string]struct{}
	BalanceManager             *balance.Manager
	GenesisContainer           *servicesgenesis.GenesisContainer
	IsAccumulateBalanceIndexer bool
	IsAccumulateBalanceReader  bool
	IsDisableBootstrap         bool
	IsAggregateCache           bool
	IndexedList                indexedlist.IndexedList
	LocalTxPool                chan *LocalTxPoolJob
}

func (s *Control) Logger() logging.Logger {
	return s.Log
}

func (s *Control) Init(networkID uint32) error {
	s.IndexedList = indexedlist.NewIndexedList(cfg.MaxSizedList)
	s.LocalTxPool = make(chan *LocalTxPoolJob, cfg.MaxTxPoolSize)

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
	persist := idb.NewPersist()
	s.BalanceManager = balance.NewManager(persist, s)

	s.GenesisContainer, err = servicesgenesis.NewGenesisContainer(networkID)
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

func (s *Control) DatabaseOnly() (*servicesconn.Connections, error) {
	c, err := servicesconn.NewDBFromConfig(s.Services, false)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(10 * time.Second)
	return c, nil
}

func (s *Control) DatabaseRO() (*servicesconn.Connections, error) {
	c, err := servicesconn.NewConnectionsFromConfig(s.Services, true)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(10 * time.Second)
	return c, nil
}

func (s *Control) Enqueue(pool *idb.TxPool) {
	select {
	case s.LocalTxPool <- &LocalTxPoolJob{TxPool: pool}:
	default:
	}
}
