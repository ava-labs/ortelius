package servicesctrl

import (
	"errors"
	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/utils"
	"github.com/prometheus/client_golang/prometheus"
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
	BalancheManager            *balance.Manager
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
	s.BalancheManager = balance.NewManager(persist, s)

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

var (
	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")
)
const MaxCodecSize = 100_000_000

func NewAVMCodec(networkID uint32, chainID string) (*avm.VM, *snow.Context, codec.Manager, database.Database, error) {
	genesisBytes, _, err := genesis.Genesis(networkID, "")
	if err != nil {
		return nil, nil, nil, nil, err
	}

	g, err := genesis.VMGenesis(genesisBytes, avm.ID)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	createChainTx, ok := g.UnsignedTx.(*platformvm.UnsignedCreateChainTx)
	if !ok {
		return nil, nil, nil, nil, ErrIncorrectGenesisChainTxType
	}

	bcLookup := &ids.Aliaser{}
	bcLookup.Initialize()
	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err = bcLookup.Alias(id, "X"); err != nil {
		return nil, nil, nil, nil, err
	}

	var (
		fxIDs = createChainTx.FxIDs
		fxs   = make([]*common.Fx, 0, len(fxIDs))
		ctx   = &snow.Context{
			NetworkID:     networkID,
			ChainID:       id,
			Log:           logging.NoLog{},
			Metrics:       prometheus.NewRegistry(),
			BCLookup:      bcLookup,
			EpochDuration: time.Hour,
		}
	)
	for _, fxID := range fxIDs {
		switch {
		case fxID == secp256k1fx.ID:
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		case fxID == nftfx.ID:
			fxs = append(fxs, &common.Fx{
				Fx: &nftfx.Fx{},
				ID: fxID,
			})
		default:
			// return nil, fmt.Errorf("Unknown FxID: %s", fxID)
		}
	}

	db := &utils.NoopDatabase{}
	dbm := utils.NewNoopManager(db)

	// Initialize an producer to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err = vm.Initialize(ctx, dbm, createChainTx.GenesisData, nil, nil, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, nil, nil, nil, err
	}

	vm.Codec().SetMaxSize(MaxCodecSize)

	return vm, ctx, vm.Codec(), db, nil
}
