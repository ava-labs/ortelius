// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	avalancheGoUtils "github.com/ava-labs/avalanchego/utils"

	"github.com/ava-labs/avalanchego/ids"
	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/avalanchego/utils/wrappers"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/coreth/core/types"

	"github.com/ava-labs/avalanchego/utils/hashing"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

const (
	rpcTimeout     = time.Minute
	dbReadTimeout  = 10 * time.Second
	dbWriteTimeout = time.Minute

	readRPCTimeout = 500 * time.Millisecond

	maxWorkerQueue = 1000
	maxWorkers     = 4
)

type producerCChainContainer struct {
	conns      *services.Connections
	block      *big.Int
	blockCount *big.Int

	client *cblock.Client

	msgChan     chan *blockWorkContainer
	msgChanSz   int64
	msgChanDone chan struct{}

	quitCh chan struct{}

	catchupErrs avalancheGoUtils.AtomicInterface
}

func (p *producerCChainContainer) isStopping() bool {
	select {
	case <-p.quitCh:
		return true
	default:
		return false
	}
}

func (p *producerCChainContainer) Close() error {
	close(p.quitCh)
	close(p.msgChanDone)
	if p.client != nil {
		p.client.Close()
	}
	errs := wrappers.Errs{}
	if p.conns != nil {
		errs.Add(p.conns.Close())
	}
	return errs.Err
}

type ProducerCChain struct {
	id string
	sc *services.Control

	// metrics
	metricProcessedCountKey string
	metricSuccessCountKey   string
	metricFailureCountKey   string

	conf cfg.Config

	// Concurrency control
	quitCh    chan struct{}
	doneCh    chan struct{}
	topic     string
	topicTrc  string
	topicLogs string
}

func NewProducerCChain() utils.ListenCloserFactory {
	return func(sc *services.Control, conf cfg.Config, _ int, _ int) utils.ListenCloser {
		topicName := fmt.Sprintf("%d-%s-cchain", conf.NetworkID, conf.CchainID)
		topicTrcName := fmt.Sprintf("%d-%s-cchain-trc", conf.NetworkID, conf.CchainID)
		topicLogsName := fmt.Sprintf("%d-%s-cchain-logs", conf.NetworkID, conf.CchainID)

		p := &ProducerCChain{
			topic:                   topicName,
			topicTrc:                topicTrcName,
			topicLogs:               topicLogsName,
			conf:                    conf,
			sc:                      sc,
			metricProcessedCountKey: fmt.Sprintf("produce_records_processed_%s_cchain", conf.CchainID),
			metricSuccessCountKey:   fmt.Sprintf("produce_records_success_%s_cchain", conf.CchainID),
			metricFailureCountKey:   fmt.Sprintf("produce_records_failure_%s_cchain", conf.CchainID),
			id:                      fmt.Sprintf("producer %d %s cchain", conf.NetworkID, conf.CchainID),
			quitCh:                  make(chan struct{}),
			doneCh:                  make(chan struct{}),
		}
		metrics.Prometheus.CounterInit(p.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(p.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(p.metricFailureCountKey, "records failure")
		sc.InitProduceMetrics()

		return p
	}
}

func (p *ProducerCChain) Close() error {
	close(p.quitCh)
	<-p.doneCh
	return nil
}

func (p *ProducerCChain) ID() string {
	return p.id
}

func (p *ProducerCChain) updateTxPool(conns *services.Connections, txPool *services.TxPool) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("update-tx-pool"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.InsertTxPool(ctx, sess, txPool)
}

func (p *ProducerCChain) updateBlock(conns *services.Connections, blockNumber *big.Int, updateTime time.Time) error {
	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("update-block"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	cvmBlocks := &services.CvmBlocks{
		Block:     blockNumber.String(),
		CreatedAt: updateTime,
	}
	return p.sc.Persist.InsertCvmBlocks(ctx, sess, cvmBlocks)
}

type localBlockObject struct {
	block  *types.Block
	time   time.Time
	traces []*cblock.TransactionTrace
	fls    []*types.Log
}

func (p *ProducerCChain) ProcessNextMessage(pc *producerCChainContainer) error {
	lblocknext, err := pc.client.Latest(rpcTimeout)
	if err != nil {
		time.Sleep(readRPCTimeout)
		return err
	}
	if lblocknext.Cmp(pc.block) <= 0 {
		time.Sleep(readRPCTimeout)
		return ErrNoMessage
	}

	errs := &avalancheGoUtils.AtomicInterface{}
	for !p.isStopping() && lblocknext.Cmp(pc.block) > 0 {
		if errs.GetValue() != nil {
			return errs.GetValue().(error)
		}
		if pc.catchupErrs.GetValue() != nil {
			return pc.catchupErrs.GetValue().(error)
		}
		ncurrent := big.NewInt(0).Add(pc.block, big.NewInt(1))
		p.enqueue(pc, &blockWorkContainer{errs: errs, blockNumber: ncurrent})
		pc.block = big.NewInt(0).Add(pc.block, big.NewInt(1))
	}

	for atomic.LoadInt64(&pc.msgChanSz) > 0 {
		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

func (p *ProducerCChain) Failure() {
	_ = metrics.Prometheus.CounterInc(p.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceFailureCountKey)
}

func (p *ProducerCChain) Success() {
	_ = metrics.Prometheus.CounterInc(p.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceSuccessCountKey)
}

func (p *ProducerCChain) getBlock(pc *producerCChainContainer) error {
	var err error

	sess := pc.conns.DB().NewSessionForEventReceiver(pc.conns.Stream().NewJob("get-block"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbReadTimeout)
	defer cancelCtx()

	type MaxBlock struct {
		Block      string
		BlockCount string
	}
	maxBlock := MaxBlock{}
	_, err = sess.Select(
		"cast(case when max(block) is null then -1 else max(block) end as char) as block",
		"cast(count(*) as char) as block_count",
	).
		From(services.TableCvmBlocks).
		LoadContext(ctx, &maxBlock)
	if err != nil {
		return err
	}

	mblock, ok := big.NewInt(0).SetString(maxBlock.Block, 10)
	if !ok {
		return fmt.Errorf("invalid block %s", maxBlock.Block)
	}
	cblockCount, ok := big.NewInt(0).SetString(maxBlock.BlockCount, 10)
	if !ok {
		return fmt.Errorf("invalid block %s", maxBlock.BlockCount)
	}
	pc.block = mblock
	pc.blockCount = cblockCount
	p.sc.Log.Info("starting processing block %s cnt %s", pc.block.String(), pc.blockCount.String())
	return nil
}

func (p *ProducerCChain) Listen() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		p.sc.Log.Info("Started worker manager for cchain")
		defer p.sc.Log.Info("Exiting worker manager for cchain")
		defer wg.Done()

		// Keep running the worker until we're asked to stop
		var err error
		for !p.isStopping() {
			err = p.runProcessor()

			// If there was an error we want to log it, and iff we are not stopping
			// we want to add a retry delay.
			if err != nil {
				p.sc.Log.Error("Error running worker: %s", err.Error())
			}
			if p.isStopping() {
				return
			}
			if err != nil {
				<-time.After(processorFailureRetryInterval)
			}
		}
	}()

	// Wait for all workers to finish
	wg.Wait()
	p.sc.Log.Info("All workers stopped")
	close(p.doneCh)

	return nil
}

// isStopping returns true iff quitCh has been signaled
func (p *ProducerCChain) isStopping() bool {
	select {
	case <-p.quitCh:
		return true
	default:
		return false
	}
}

func (p *ProducerCChain) init() (*producerCChainContainer, error) {
	conns, err := p.sc.DatabaseOnly()
	if err != nil {
		return nil, err
	}

	pc := &producerCChainContainer{
		msgChan:     make(chan *blockWorkContainer, maxWorkerQueue),
		msgChanDone: make(chan struct{}, 1),
		quitCh:      make(chan struct{}, 1),
	}
	pc.conns = conns

	err = p.getBlock(pc)
	if err != nil {
		_ = conns.Close()
		return nil, err
	}

	cl, err := cblock.NewClient(p.conf.Producer.CChainRPC)
	if err != nil {
		_ = conns.Close()
		return nil, err
	}
	pc.client = cl

	return pc, nil
}

// runProcessor starts the processing loop for the backend and closes it when
// finished
func (p *ProducerCChain) runProcessor() error {
	if p.isStopping() {
		p.sc.Log.Info("Not starting worker for cchain because we're stopping")
		return nil
	}

	p.sc.Log.Info("Starting worker for cchain")
	defer p.sc.Log.Info("Exiting worker for cchain")

	var pc *producerCChainContainer
	defer func() {
		if pc != nil {
			err := pc.Close()
			if err != nil {
				p.sc.Log.Warn("Stopping worker for cchain %w", err)
			}
		}
	}()
	var err error
	pc, err = p.init()
	if err != nil {
		return err
	}

	for icnt := 0; icnt < maxWorkers; icnt++ {
		cl, err := cblock.NewClient(p.conf.Producer.CChainRPC)
		if err != nil {
			return err
		}
		conns1, err := p.sc.DatabaseOnly()
		if err != nil {
			cl.Close()
			return err
		}
		go p.blockProcessor(pc, cl, conns1)
	}

	pblockp1 := big.NewInt(0).Add(pc.block, big.NewInt(1))
	if pc.blockCount.Cmp(pblockp1) < 0 {
		conns1, err := p.sc.DatabaseOnly()
		if err != nil {
			return err
		}
		go p.catchupBlock(conns1, pc, pblockp1)
	}

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := p.ProcessNextMessage(pc)
			if pc.catchupErrs.GetValue() != nil {
				err = pc.catchupErrs.GetValue().(error)
				p.sc.Log.Error("Catchup error: %v", err)
				return err
			}

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
				if strings.HasPrefix(err.Error(), "404 Not Found") {
					p.sc.Log.Warn("%s", err.Error())
					return nil
				}
				if strings.HasPrefix(err.Error(), "503 Service Unavailable") {
					p.sc.Log.Warn("%s", err.Error())
					return nil
				}
				if strings.HasSuffix(err.Error(), "connect: connection refused") {
					p.sc.Log.Warn("%s", err.Error())
					return nil
				}
				if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
					p.sc.Log.Warn("%s", err.Error())
					return nil
				}

				failures++
				p.Failure()
				p.sc.Log.Error("Unknown error: %v", err)
				return err
			}
		}
	)

	id := p.ID()

	t := time.NewTicker(30 * time.Second)
	tdoneCh := make(chan struct{})
	defer func() {
		t.Stop()
		close(tdoneCh)
	}()

	// Log run statistics periodically until asked to stop
	go func() {
		for {
			select {
			case <-t.C:
				p.sc.Log.Info("IProcessor %s successes=%d failures=%d nomsg=%d", id, successes, failures, nomsg)
				if p.isStopping() {
					return
				}
			case <-tdoneCh:
				return
			}
		}
	}()

	// Process messages until asked to stop
	for !p.isStopping() {
		err := processNextMessage()
		if err != nil {
			return err
		}
	}

	return nil
}

type WorkPacketCChain struct {
	localBlock *localBlockObject
}

func (p *ProducerCChain) processWork(conns *services.Connections, wp *WorkPacketCChain) error {
	cblk, err := cblock.New(wp.localBlock.block)
	if err != nil {
		return err
	}

	// wipe before re-encoding
	cblk.Txs = nil
	block, err := json.Marshal(cblk)
	if err != nil {
		return err
	}

	id, err := ids.ToID(hashing.ComputeHash256(block))
	if err != nil {
		return err
	}

	txPool := &services.TxPool{
		NetworkID:     p.conf.NetworkID,
		ChainID:       p.conf.CchainID,
		MsgKey:        id.String(),
		Serialization: block,
		Processed:     0,
		Topic:         p.topic,
		CreatedAt:     wp.localBlock.time,
	}
	err = txPool.ComputeID()
	if err != nil {
		return err
	}
	err = p.updateTxPool(conns, txPool)
	if err != nil {
		return err
	}

	for _, txTranactionTraces := range wp.localBlock.traces {
		txTransactionTracesBits, err := json.Marshal(txTranactionTraces)
		if err != nil {
			return err
		}

		id, err := ids.ToID(hashing.ComputeHash256(txTransactionTracesBits))
		if err != nil {
			return err
		}

		txPool := &services.TxPool{
			NetworkID:     p.conf.NetworkID,
			ChainID:       p.conf.CchainID,
			MsgKey:        id.String(),
			Serialization: txTransactionTracesBits,
			Processed:     0,
			Topic:         p.topicTrc,
			CreatedAt:     wp.localBlock.time,
		}
		err = txPool.ComputeID()
		if err != nil {
			return err
		}
		err = p.updateTxPool(conns, txPool)
		if err != nil {
			return err
		}
	}

	for _, fl := range wp.localBlock.fls {
		flBits, err := json.Marshal(fl)
		if err != nil {
			return err
		}

		id, err := ids.ToID(hashing.ComputeHash256(flBits))
		if err != nil {
			return err
		}

		txPool := &services.TxPool{
			NetworkID:     p.conf.NetworkID,
			ChainID:       p.conf.CchainID,
			MsgKey:        id.String(),
			Serialization: flBits,
			Processed:     0,
			Topic:         p.topicLogs,
			CreatedAt:     wp.localBlock.time,
		}
		err = txPool.ComputeID()
		if err != nil {
			return err
		}
		err = p.updateTxPool(conns, txPool)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *ProducerCChain) catchupBlock(conns *services.Connections, pc *producerCChainContainer, catchupBlock *big.Int) {
	defer func() {
		_ = conns.Close()
	}()

	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("catchup-block"))

	ctx := context.Background()
	var err error
	startBlock := big.NewInt(0)
	endBlock := big.NewInt(0)
	for !pc.isStopping() && !p.isStopping() && endBlock.Cmp(catchupBlock) < 0 {
		endBlock = big.NewInt(0).Add(startBlock, big.NewInt(100000))
		if endBlock.Cmp(catchupBlock) >= 0 {
			endBlock.Set(catchupBlock)
		}

		var cvmBlocks []*services.CvmBlocks
		_, err = sess.Select(
			"block",
		).From(services.TableCvmBlocks).
			Where("block >= "+startBlock.String()+" and block < "+endBlock.String()).
			LoadContext(ctx, &cvmBlocks)
		if err != nil {
			pc.catchupErrs.SetValue(err)
			return
		}
		blockMap := make(map[string]struct{})
		for _, bl := range cvmBlocks {
			blockMap[bl.Block] = struct{}{}
		}
		for startBlock.Cmp(endBlock) < 0 {
			if pc.catchupErrs.GetValue() != nil {
				return
			}
			if _, ok := blockMap[startBlock.String()]; !ok {
				p.sc.Log.Info("refill %v", startBlock.String())
				p.enqueue(pc, &blockWorkContainer{errs: &pc.catchupErrs, blockNumber: startBlock})
			}
			startBlock = big.NewInt(0).Add(startBlock, big.NewInt(1))
		}
	}

	p.sc.Log.Info("catchup complete")
}

func (p *ProducerCChain) enqueue(pc *producerCChainContainer, bw *blockWorkContainer) {
	pc.msgChan <- bw
	atomic.AddInt64(&pc.msgChanSz, 1)
}

type blockWorkContainer struct {
	errs        *avalancheGoUtils.AtomicInterface
	blockNumber *big.Int
}

func (p *ProducerCChain) blockProcessor(pc *producerCChainContainer, client *cblock.Client, conns *services.Connections) {
	defer func() {
		_ = conns.Close()
		client.Close()
	}()

	for {
		select {
		case blockWork := <-pc.msgChan:
			atomic.AddInt64(&pc.msgChanSz, -1)
			if blockWork.errs.GetValue() != nil {
				return
			}

			bl, traces, fls, err := cblock.ReadBlockFromRPC(client, blockWork.blockNumber, rpcTimeout)
			if err != nil {
				blockWork.errs.SetValue(err)
				return
			}

			localBlockObject := &localBlockObject{block: bl, traces: traces, fls: fls, time: time.Now().UTC()}

			wp := &WorkPacketCChain{localBlock: localBlockObject}
			err = p.processWork(conns, wp)
			if err != nil {
				blockWork.errs.SetValue(err)
				return
			}

			_ = metrics.Prometheus.CounterInc(p.metricProcessedCountKey)
			_ = metrics.Prometheus.CounterInc(services.MetricProduceProcessedCountKey)

			err = p.updateBlock(conns, blockWork.blockNumber, localBlockObject.time)
			if err != nil {
				blockWork.errs.SetValue(err)
				return
			}
		case <-pc.msgChanDone:
			return
		}
	}
}
