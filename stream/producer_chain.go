// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/indexer"
	"github.com/ava-labs/avalanchego/utils/formatting"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/json"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
)

const (
	IndexerTimeout = 3 * time.Minute
	MaxTxRead      = 500
)

type IndexType byte

const (
	IndexTypeTransactions IndexType = iota
	IndexTypeVertices
	IndexTypeBlocks

	typeUnknown = "unknown"
)

func (t IndexType) String() string {
	switch t {
	case IndexTypeTransactions:
		return "tx"
	case IndexTypeVertices:
		return "vtx"
	case IndexTypeBlocks:
		return "block"
	}
	return typeUnknown
}

type IndexedChain byte

const (
	IndexXChain IndexedChain = iota
	IndexPChain
	IndexCChain
)

func (t IndexedChain) String() string {
	switch t {
	case IndexXChain:
		return "X"
	case IndexPChain:
		return "P"
	case IndexCChain:
		return "C"
	}
	// Should never happen
	return typeUnknown
}

type producerChainContainer struct {
	sc                      *servicesctrl.Control
	conns                   *utils.Connections
	runningControl          utils.Running
	nodeIndexer             indexer.Client
	conf                    cfg.Config
	nodeIndex               *db.NodeIndex
	nodeinstance            string
	topic                   string
	chainID                 string
	indexerType             IndexType
	indexerChain            IndexedChain
	metricProcessedCountKey string
}

func newContainer(
	sc *servicesctrl.Control,
	conf cfg.Config,
	nodeIndexer indexer.Client,
	topic string,
	chainID string,
	indexerType IndexType,
	indexerChain IndexedChain,
	metricProcessedCountKey string,
) (*producerChainContainer, error) {
	conns, err := sc.Database()
	if err != nil {
		return nil, err
	}

	pc := &producerChainContainer{
		indexerType:             indexerType,
		indexerChain:            indexerChain,
		runningControl:          utils.NewRunning(),
		chainID:                 chainID,
		conns:                   conns,
		sc:                      sc,
		nodeIndexer:             nodeIndexer,
		conf:                    conf,
		topic:                   topic,
		nodeinstance:            conf.NodeInstance,
		metricProcessedCountKey: metricProcessedCountKey,
	}

	// init the node index table
	err = pc.insertNodeIndex(pc.conns, &db.NodeIndex{Instance: pc.nodeinstance, Topic: pc.topic, Idx: 0})
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

	qn := &db.NodeIndex{Instance: p.nodeinstance, Topic: p.topic}
	nodeIndex, err := p.sc.Persist.QueryNodeIndex(ctx, sess, qn)
	if err != nil {
		return err
	}
	p.nodeIndex = nodeIndex
	p.nodeIndex.Instance = p.nodeinstance
	p.nodeIndex.Topic = p.topic
	p.sc.Log.Info("starting processing %d", p.nodeIndex.Idx)
	return nil
}

func (p *producerChainContainer) ProcessNextMessage() error {
	containerRangeArgs := &indexer.GetContainerRangeArgs{
		StartIndex: json.Uint64(p.nodeIndex.Idx),
		NumToFetch: json.Uint64(MaxTxRead),
		Encoding:   formatting.Hex,
	}
	containers, err := p.nodeIndexer.GetContainerRange(containerRangeArgs)
	if err != nil {
		time.Sleep(readRPCTimeout)
		if IndexNotReady(err) {
			return nil
		}
		return err
	}
	if len(containers) == 0 {
		time.Sleep(readRPCTimeout)
		return ErrNoMessage
	}
	for _, container := range containers {
		if err != nil {
			return err
		}

		var id ids.ID
		switch p.indexerChain {
		case IndexCChain:
			id = container.ID
		default:
			// x and p we compute the hash
			nid, err := ids.ToID(hashing.ComputeHash256(container.Bytes))
			if err != nil {
				return err
			}
			id = nid
		}

		txPool := &db.TxPool{
			NetworkID:     p.conf.NetworkID,
			ChainID:       p.chainID,
			MsgKey:        id.String(),
			Serialization: container.Bytes,
			Processed:     0,
			Topic:         p.topic,
			CreatedAt:     time.Unix(container.Timestamp, 0),
		}
		err = txPool.ComputeID()
		if err != nil {
			return err
		}
		err = UpdateTxPool(dbWriteTimeout, p.conns, p.sc.Persist, txPool, p.sc)
		if err != nil {
			return err
		}

		_ = utils.Prometheus.CounterInc(p.metricProcessedCountKey)
		_ = utils.Prometheus.CounterInc(servicesctrl.MetricProduceProcessedCountKey)
	}

	nodeIdx := &db.NodeIndex{
		Instance: p.nodeinstance,
		Topic:    p.topic,
		Idx:      p.nodeIndex.Idx + uint64(len(containers)),
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

func (p *producerChainContainer) insertNodeIndex(conns *utils.Connections, nodeIndex *db.NodeIndex) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("update-node-index"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.InsertNodeIndex(ctx, sess, nodeIndex, cfg.PerformUpdates)
}

func (p *producerChainContainer) updateNodeIndex(conns *utils.Connections, nodeIndex *db.NodeIndex) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("update-node-index"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.UpdateNodeIndex(ctx, sess, nodeIndex)
}

type ProducerChain struct {
	id string
	sc *servicesctrl.Control

	// metrics
	metricProcessedCountKey string
	metricSuccessCountKey   string
	metricFailureCountKey   string

	conf cfg.Config

	runningControl utils.Running

	topic string

	nodeIndexer  indexer.Client
	chainID      string
	indexerType  IndexType
	indexerChain IndexedChain
}

func NewProducerChain(sc *servicesctrl.Control, conf cfg.Config, chainID string, eventType EventType, indexerType IndexType, indexerChain IndexedChain) (*ProducerChain, error) {
	topicName := GetTopicName(conf.NetworkID, chainID, eventType)

	endpoint := fmt.Sprintf("/ext/index/%s/%s", indexerChain, indexerType)

	nodeIndexer := indexer.NewClient(conf.AvalancheGO, endpoint, IndexerTimeout)

	p := &ProducerChain{
		indexerType:             indexerType,
		indexerChain:            indexerChain,
		chainID:                 chainID,
		topic:                   topicName,
		conf:                    conf,
		sc:                      sc,
		metricProcessedCountKey: fmt.Sprintf("produce_records_processed_%s_%s", chainID, eventType),
		metricSuccessCountKey:   fmt.Sprintf("produce_records_success_%s_%s", chainID, eventType),
		metricFailureCountKey:   fmt.Sprintf("produce_records_failure_%s_%s", chainID, eventType),
		id:                      fmt.Sprintf("producer %d %s %s", conf.NetworkID, chainID, eventType),
		runningControl:          utils.NewRunning(),
		nodeIndexer:             nodeIndexer,
	}
	utils.Prometheus.CounterInit(p.metricProcessedCountKey, "records processed")
	utils.Prometheus.CounterInit(p.metricSuccessCountKey, "records success")
	utils.Prometheus.CounterInit(p.metricFailureCountKey, "records failure")
	sc.InitProduceMetrics()

	return p, nil
}

func (p *ProducerChain) Close() error {
	p.runningControl.Close()
	return nil
}

func (p *ProducerChain) ID() string {
	return p.id
}

func (p *ProducerChain) Failure() {
	_ = utils.Prometheus.CounterInc(p.metricFailureCountKey)
	_ = utils.Prometheus.CounterInc(servicesctrl.MetricProduceFailureCountKey)
}

func (p *ProducerChain) Success() {
	_ = utils.Prometheus.CounterInc(p.metricSuccessCountKey)
	_ = utils.Prometheus.CounterInc(servicesctrl.MetricProduceSuccessCountKey)
}

func (p *ProducerChain) Listen() error {
	p.sc.Log.Info("Started worker manager for %s", p.ID())
	defer p.sc.Log.Info("Exiting worker manager for %s", p.ID())

	for !p.runningControl.IsStopped() {
		err := p.runProcessor()

		// If there was an error we want to log it, and iff we are not stopping
		// we want to add a retry delay.
		if err != nil {
			p.sc.Log.Error("Error running worker: %s", err.Error())
		}
		if p.runningControl.IsStopped() {
			break
		}
		if err != nil {
			<-time.After(processorFailureRetryInterval)
		}
	}

	return nil
}

// runProcessor starts the processing loop for the backend and closes it when
// finished
func (p *ProducerChain) runProcessor() error {
	if p.runningControl.IsStopped() {
		p.sc.Log.Info("Not starting worker for cchain because we're stopping")
		return nil
	}

	p.sc.Log.Info("Starting worker for %s", p.ID())
	defer p.sc.Log.Info("Exiting worker for %s", p.ID())

	pc, err := newContainer(p.sc, p.conf, p.nodeIndexer, p.topic, p.chainID, p.indexerType, p.indexerChain, p.metricProcessedCountKey)
	if err != nil {
		return err
	}

	defer func() {
		pc.runningControl.Close()
		err := pc.Close()
		if err != nil {
			p.sc.Log.Warn("Stopping worker for chain %w", err)
		}
	}()

	processNextMessage := func() error {
		err := pc.ProcessNextMessage()

		switch err {
		case nil:
			p.Success()
			return nil

		// This error is expected when the upstream service isn't producing
		case context.DeadlineExceeded:
			p.sc.Log.Debug("context deadline exceeded")
			return nil

		case ErrNoMessage:
			return nil

		case io.EOF:
			p.sc.Log.Error("EOF")
			return io.EOF

		default:
			if ChainNotReady(err) {
				p.sc.Log.Warn("%s", TrimNL(err.Error()))
				return nil
			}

			p.Failure()
			p.sc.Log.Error("Unknown error: %v", err)
			return err
		}
	}

	// Process messages until asked to stop
	for {
		if p.runningControl.IsStopped() || pc.runningControl.IsStopped() {
			break
		}
		err := processNextMessage()
		if err != nil {
			return err
		}
	}

	return nil
}

func IndexNotReady(err error) bool {
	if strings.HasPrefix(err.Error(), "start index") && strings.Contains(err.Error(), "last accepted index") {
		return true
	}
	if strings.HasPrefix(err.Error(), "received status code '404'") {
		return true
	}
	return false
}

func ChainNotReady(err error) bool {
	if strings.HasPrefix(err.Error(), "no containers have been accepted") {
		return true
	}
	if strings.HasPrefix(err.Error(), "received status code '404'") {
		return true
	}
	return false
}
