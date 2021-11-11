// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package servicesctrl

import (
	"time"

	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/utils"
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
	TxPool *db.TxPool
	Errs   *avlancheGoUtils.AtomicInterface
}

type Control struct {
	Services                   cfg.Services
	ServicesCfg                cfg.Config
	Chains                     map[string]cfg.Chain `json:"chains"`
	Log                        logging.Logger
	Persist                    db.Persist
	Features                   map[string]struct{}
	BalanceManager             utils.ExecIface
	GenesisContainer           *utils.GenesisContainer
	IsAccumulateBalanceIndexer bool
	IsAccumulateBalanceReader  bool
	IsDisableBootstrap         bool
	IsAggregateCache           bool
	IsCChainIndex              bool
	IndexedList                utils.IndexedList
	LocalTxPool                chan *LocalTxPoolJob
}

func (s *Control) Logger() logging.Logger {
	return s.Log
}

func (s *Control) Init(networkID uint32) error {
	s.IndexedList = utils.NewIndexedList(cfg.MaxSizedList)
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
	if _, ok := s.Features["cchain_index"]; ok {
		s.IsCChainIndex = true
	}
	var err error
	s.GenesisContainer, err = utils.NewGenesisContainer(networkID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Control) InitProduceMetrics() {
	utils.Prometheus.CounterInit(MetricProduceProcessedCountKey, "records processed")
	utils.Prometheus.CounterInit(MetricProduceSuccessCountKey, "records success")
	utils.Prometheus.CounterInit(MetricProduceFailureCountKey, "records failure")
}

func (s *Control) InitConsumeMetrics() {
	utils.Prometheus.CounterInit(MetricConsumeProcessedCountKey, "records processed")
	utils.Prometheus.CounterInit(MetricConsumeProcessMillisCounterKey, "records processed millis")
	utils.Prometheus.CounterInit(MetricConsumeSuccessCountKey, "records success")
	utils.Prometheus.CounterInit(MetricConsumeFailureCountKey, "records failure")
}

func (s *Control) Database() (*utils.Connections, error) {
	c, err := utils.NewDBFromConfig(s.Services, false)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(10 * time.Second)
	c.Eventer.SetLog(s.Log)
	return c, nil
}

func (s *Control) DatabaseRO() (*utils.Connections, error) {
	c, err := utils.NewDBFromConfig(s.Services, true)
	if err != nil {
		return nil, err
	}
	c.DB().SetMaxIdleConns(32)
	c.DB().SetConnMaxIdleTime(10 * time.Second)
	c.Eventer.SetLog(s.Log)
	return c, nil
}

func (s *Control) Enqueue(pool *db.TxPool) {
	select {
	case s.LocalTxPool <- &LocalTxPoolJob{TxPool: pool}:
	default:
	}
}
