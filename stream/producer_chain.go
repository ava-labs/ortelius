// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"

	"github.com/ava-labs/avalanchego/utils/formatting"
	"github.com/ava-labs/avalanchego/utils/json"

	"github.com/ava-labs/avalanchego/utils/wrappers"

	indexer "github.com/ava-labs/ortelius/indexer_client"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

const (
	IndexerTimeout = 3 * time.Minute
	MaxTxRead      = 1024
)

type producerChainContainer struct {
	sc          *services.Control
	conns       *services.Connections
	quitCh      chan struct{}
	nodeIndexer *indexer.Client
	conf        cfg.Config
	nodeIndex   *services.NodeIndex
	topic       string
	chainID     string
}

func newContainer(sc *services.Control, conf cfg.Config, nodeIndexer *indexer.Client, topic string, chainID string) (*producerChainContainer, error) {
	conns, err := sc.DatabaseOnly()
	if err != nil {
		return nil, err
	}

	pc := &producerChainContainer{
		quitCh:      make(chan struct{}, 1),
		chainID:     chainID,
		conns:       conns,
		sc:          sc,
		nodeIndexer: nodeIndexer,
		conf:        conf,
		topic:       topic,
	}

	// init the node index table
	err = pc.insertNodeIndex(pc.conns, &services.NodeIndex{Topic: pc.topic, Idx: 0})
	if err != nil {
		return nil, err
	}

	err = pc.getIndex()
	if err != nil {
		return nil, err
	}

	return pc, nil
}

func (p *producerChainContainer) Close() error {
	errs := wrappers.Errs{}
	if p.conns != nil {
		errs.Add(p.conns.Close())
	}
	return errs.Err
}

func (p *producerChainContainer) getIndex() error {
	var err error
	sess := p.conns.DB().NewSessionForEventReceiver(p.conns.Stream().NewJob("get-index"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbReadTimeout)
	defer cancelCtx()

	qn := &services.NodeIndex{Topic: p.topic}
	nodeIndex, err := p.sc.Persist.QueryNodeIndex(ctx, sess, qn)
	if err != nil {
		return err
	}
	p.nodeIndex = nodeIndex
	p.nodeIndex.Topic = p.topic
	p.sc.Log.Info("starting processing %s", p.nodeIndex.Idx)
	return nil
}

func (p *producerChainContainer) ProcessNextMessage() error {
	containerRange := &indexer.GetContainerRange{
		StartIndex: json.Uint64(p.nodeIndex.Idx),
		NumToFetch: json.Uint64(MaxTxRead),
		Encoding:   formatting.Hex,
	}
	containers, err := p.nodeIndexer.GetContainerRange(containerRange)
	if err != nil {
		time.Sleep(readRPCTimeout)
		return err
	}
	if len(containers) == 0 {
		time.Sleep(readRPCTimeout)
		return ErrNoMessage
	}
	for _, container := range containers {
		decodeBytes, err := hex.DecodeString(strings.TrimPrefix(container.Bytes, "0x"))
		if err != nil {
			return err
		}
		hid, err := ids.ToID(hashing.ComputeHash256(decodeBytes))
		if err != nil {
			return err
		}

		txPool := &services.TxPool{
			NetworkID:     p.conf.NetworkID,
			ChainID:       p.chainID,
			MsgKey:        hid.String(),
			Serialization: decodeBytes,
			Processed:     0,
			Topic:         p.topic,
			CreatedAt:     container.Timestamp,
		}
		err = txPool.ComputeID()
		if err != nil {
			return err
		}
		err = p.updateTxPool(p.conns, txPool)
		if err != nil {
			return err
		}
	}

	nodeIdx := &services.NodeIndex{
		Topic: p.topic,
		Idx:   p.nodeIndex.Idx + uint64(len(containers)),
	}

	err = p.updateNodeIndex(p.conns, nodeIdx)
	if err != nil {
		return err
	}

	p.nodeIndex.Idx = nodeIdx.Idx

	if len(containers) < MaxTxRead {
		time.Sleep(readRPCTimeout)
	}

	return nil
}

func (p *producerChainContainer) insertNodeIndex(conns *services.Connections, nodeIndex *services.NodeIndex) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("update-node-index"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.InsertNodeIndex(ctx, sess, nodeIndex, cfg.PerformUpdates)
}

func (p *producerChainContainer) updateNodeIndex(conns *services.Connections, nodeIndex *services.NodeIndex) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("update-node-index"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.UpdateNodeIndex(ctx, sess, nodeIndex)
}

func (p *producerChainContainer) updateTxPool(conns *services.Connections, txPool *services.TxPool) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("update-tx-pool"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.InsertTxPool(ctx, sess, txPool)
}

func (p *producerChainContainer) isStopping() bool {
	select {
	case <-p.quitCh:
		return true
	default:
		return false
	}
}

type ProducerChain struct {
	id string
	sc *services.Control

	// metrics
	metricProcessedCountKey string
	metricSuccessCountKey   string
	metricFailureCountKey   string

	conf cfg.Config

	// Concurrency control
	quitCh chan struct{}

	topic string

	nodeIndexer *indexer.Client
	chainID     string
}

func NewProducerChain(sc *services.Control, conf cfg.Config, chainID string, eventType EventType, indexerType indexer.IndexType, indexerChain indexer.IndexedChain) (*ProducerChain, error) {
	topicName := GetTopicName(conf.NetworkID, chainID, eventType)

	nodeIndexer, err := indexer.NewClient("http://localhost:9650", indexerChain, indexerType, IndexerTimeout)
	if err != nil {
		return nil, err
	}

	p := &ProducerChain{
		chainID:                 chainID,
		topic:                   topicName,
		conf:                    conf,
		sc:                      sc,
		metricProcessedCountKey: fmt.Sprintf("produce_records_processed_%s_%s", chainID, eventType),
		metricSuccessCountKey:   fmt.Sprintf("produce_records_success_%s_%s", chainID, eventType),
		metricFailureCountKey:   fmt.Sprintf("produce_records_failure_%s_%s", chainID, eventType),
		id:                      fmt.Sprintf("producer %d %s %s", conf.NetworkID, chainID, eventType),
		quitCh:                  make(chan struct{}),
		nodeIndexer:             nodeIndexer,
	}
	metrics.Prometheus.CounterInit(p.metricProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(p.metricSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(p.metricFailureCountKey, "records failure")
	sc.InitProduceMetrics()

	return p, nil
}

func (p *ProducerChain) Close() error {
	close(p.quitCh)
	return nil
}

func (p *ProducerChain) ID() string {
	return p.id
}

func (p *ProducerChain) Failure() {
	_ = metrics.Prometheus.CounterInc(p.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceFailureCountKey)
}

func (p *ProducerChain) Success() {
	_ = metrics.Prometheus.CounterInc(p.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceSuccessCountKey)
}

func (p *ProducerChain) Listen() error {
	p.sc.Log.Info("Started worker manager for %s", p.ID())
	defer p.sc.Log.Info("Exiting worker manager for %s", p.ID())

	for !p.isStopping() {
		err := p.runProcessor()

		// If there was an error we want to log it, and iff we are not stopping
		// we want to add a retry delay.
		if err != nil {
			p.sc.Log.Error("Error running worker: %s", err.Error())
		}
		if p.isStopping() {
			break
		}
		if err != nil {
			<-time.After(processorFailureRetryInterval)
		}
	}

	return nil
}

// isStopping returns true iff quitCh has been signaled
func (p *ProducerChain) isStopping() bool {
	select {
	case <-p.quitCh:
		return true
	default:
		return false
	}
}

// runProcessor starts the processing loop for the backend and closes it when
// finished
func (p *ProducerChain) runProcessor() error {
	id := p.ID()

	if p.isStopping() {
		p.sc.Log.Info("Not starting worker for cchain because we're stopping")
		return nil
	}

	p.sc.Log.Info("Starting worker for %s", p.ID())
	defer p.sc.Log.Info("Exiting worker for %s", p.ID())

	var pc *producerChainContainer

	wgpc := &sync.WaitGroup{}

	t := time.NewTicker(30 * time.Second)
	tdoneCh := make(chan struct{})

	defer func() {
		t.Stop()
		close(tdoneCh)
		if pc != nil {
			close(pc.quitCh)
			wgpc.Wait()

			err := pc.Close()
			if err != nil {
				p.sc.Log.Warn("Stopping worker for chain %w", err)
			}
		}
	}()

	var err error
	pc, err = newContainer(p.sc, p.conf, p.nodeIndexer, p.topic, p.chainID)
	if err != nil {
		return err
	}

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := pc.ProcessNextMessage()

			switch err {
			case nil:
				successes++
				p.Success()
				return nil

			// This error is expected when the upstream service isn't producing
			case context.DeadlineExceeded:
				nomsg++
				p.sc.Log.Debug("context deadline exceeded")
				return nil

			case ErrNoMessage:
				nomsg++
				p.sc.Log.Debug("no message")
				return nil

			case io.EOF:
				p.sc.Log.Error("EOF")
				return io.EOF

			default:
				// if CChainNotReady(err) {
				// 	p.sc.Log.Warn("%s", TrimNL(err.Error()))
				// 	return nil
				// }

				failures++
				p.Failure()
				p.sc.Log.Error("Unknown error: %v", err)
				return err
			}
		}
	)

	go func() {
		for {
			select {
			case <-t.C:
				p.sc.Log.Info("IProcessor %s successes=%d failures=%d nomsg=%d", id, successes, failures, nomsg)
				if p.isStopping() || pc.isStopping() {
					return
				}
			case <-tdoneCh:
				return
			}
		}
	}()

	// Process messages until asked to stop
	for {
		if p.isStopping() || pc.isStopping() {
			break
		}
		err := processNextMessage()
		if err != nil {
			return err
		}
	}

	return nil
}
