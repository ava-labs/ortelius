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
	sc *services.Control

	conns      *services.Connections
	block      *big.Int
	blockCount *big.Int

	client *cblock.Client

	msgChan     chan *blockWorkContainer
	msgChanSz   int64
	msgChanDone chan struct{}

	runningControl utils.Running

	catchupErrs avalancheGoUtils.AtomicInterface
}

func newContainerC(sc *services.Control, conf cfg.Config) (*producerCChainContainer, error) {
	conns, err := sc.DatabaseOnly()
	if err != nil {
		return nil, err
	}

	pc := &producerCChainContainer{
		msgChan:        make(chan *blockWorkContainer, maxWorkerQueue),
		msgChanDone:    make(chan struct{}, 1),
		runningControl: utils.NewRunning(),
		conns:          conns,
		sc:             sc,
	}

	err = pc.getBlock()
	if err != nil {
		_ = conns.Close()
		return nil, err
	}

	cl, err := cblock.NewClient(conf.Producer.CChainRPC)
	if err != nil {
		_ = conns.Close()
		return nil, err
	}
	pc.client = cl

	return pc, nil
}

func (p *producerCChainContainer) Close() error {
	if p.client != nil {
		p.client.Close()
	}
	errs := wrappers.Errs{}
	if p.conns != nil {
		errs.Add(p.conns.Close())
	}
	return errs.Err
}

func (p *producerCChainContainer) getBlock() error {
	var err error
	sess := p.conns.DB().NewSessionForEventReceiver(p.conns.Stream().NewJob("get-block"))

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
	p.block = mblock
	p.blockCount = cblockCount
	p.sc.Log.Info("starting processing block %s cnt %s", p.block.String(), p.blockCount.String())
	return nil
}

func (p *producerCChainContainer) enqueue(bw *blockWorkContainer) {
	p.msgChan <- bw
	atomic.AddInt64(&p.msgChanSz, 1)
}

func (p *producerCChainContainer) catchupBlock(conns *services.Connections, catchupBlock *big.Int, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		_ = conns.Close()
	}()

	sess := conns.DB().NewSessionForEventReceiver(conns.StreamDBDedup().NewJob("catchup-block"))

	ctx := context.Background()
	var err error
	startBlock := big.NewInt(0)
	endBlock := big.NewInt(0)
	for endBlock.Cmp(catchupBlock) < 0 {
		if p.runningControl.IsStopped() {
			break
		}
		if p.catchupErrs.GetValue() != nil {
			return
		}

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
			p.catchupErrs.SetValue(err)
			return
		}
		blockMap := make(map[string]struct{})
		for _, bl := range cvmBlocks {
			blockMap[bl.Block] = struct{}{}
		}
		for startBlock.Cmp(endBlock) < 0 {
			if p.runningControl.IsStopped() {
				break
			}

			if p.catchupErrs.GetValue() != nil {
				return
			}
			if _, ok := blockMap[startBlock.String()]; !ok {
				p.sc.Log.Info("refill %v", startBlock.String())
				p.enqueue(&blockWorkContainer{errs: &p.catchupErrs, blockNumber: startBlock})
			}
			startBlock = big.NewInt(0).Add(startBlock, big.NewInt(1))
		}
	}

	p.sc.Log.Info("catchup complete")
}

func (p *producerCChainContainer) ProcessNextMessage() error {
	lblocknext, err := p.client.Latest(rpcTimeout)
	if err != nil {
		time.Sleep(readRPCTimeout)
		return err
	}
	if lblocknext.Cmp(p.block) <= 0 {
		time.Sleep(readRPCTimeout)
		return ErrNoMessage
	}

	errs := &avalancheGoUtils.AtomicInterface{}
	for lblocknext.Cmp(p.block) > 0 {
		if p.runningControl.IsStopped() {
			break
		}
		if errs.GetValue() != nil {
			return errs.GetValue().(error)
		}
		if p.catchupErrs.GetValue() != nil {
			return p.catchupErrs.GetValue().(error)
		}
		ncurrent := big.NewInt(0).Add(p.block, big.NewInt(1))
		p.enqueue(&blockWorkContainer{errs: errs, blockNumber: ncurrent})
		p.block = big.NewInt(0).Add(p.block, big.NewInt(1))
	}

	for atomic.LoadInt64(&p.msgChanSz) > 0 {
		time.Sleep(1 * time.Millisecond)
	}

	if errs.GetValue() != nil {
		return errs.GetValue().(error)
	}

	return nil
}

type ProducerCChain struct {
	id string
	sc *services.Control

	// metrics
	metricProcessedCountKey string
	metricSuccessCountKey   string
	metricFailureCountKey   string

	conf cfg.Config

	runningControl utils.Running

	topic     string
	topicTrc  string
	topicLogs string
}

func NewProducerCChain(sc *services.Control, conf cfg.Config) utils.ListenCloser {
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
		runningControl:          utils.NewRunning(),
	}
	metrics.Prometheus.CounterInit(p.metricProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(p.metricSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(p.metricFailureCountKey, "records failure")
	sc.InitProduceMetrics()

	return p
}

func (p *ProducerCChain) Close() error {
	p.runningControl.Close()
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

func (p *ProducerCChain) Failure() {
	_ = metrics.Prometheus.CounterInc(p.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceFailureCountKey)
}

func (p *ProducerCChain) Success() {
	_ = metrics.Prometheus.CounterInc(p.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricProduceSuccessCountKey)
}

func (p *ProducerCChain) Listen() error {
	p.sc.Log.Info("Started worker manager for cchain")
	defer p.sc.Log.Info("Exiting worker manager for cchain")

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

func TrimNL(msg string) string {
	oldmsg := msg
	for {
		msg = strings.TrimPrefix(msg, "\n")
		if msg == oldmsg {
			break
		}
		oldmsg = msg
	}
	oldmsg = msg
	for {
		msg = strings.TrimSuffix(msg, "\n")
		if msg == oldmsg {
			break
		}
		oldmsg = msg
	}
	return msg
}

func CChainNotReady(err error) bool {
	if strings.HasPrefix(err.Error(), "404 Not Found") {
		return true
	}
	if strings.HasPrefix(err.Error(), "503 Service Unavailable") {
		return true
	}
	if strings.HasSuffix(err.Error(), "connect: connection refused") {
		return true
	}
	if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
		return true
	}
	return false
}

// runProcessor starts the processing loop for the backend and closes it when
// finished
func (p *ProducerCChain) runProcessor() error {
	id := p.ID()

	if p.runningControl.IsStopped() {
		p.sc.Log.Info("Not starting worker for cchain because we're stopping")
		return nil
	}

	p.sc.Log.Info("Starting worker for cchain")
	defer p.sc.Log.Info("Exiting worker for cchain")

	var pc *producerCChainContainer

	wgpc := &sync.WaitGroup{}
	wgpcmsgchan := &sync.WaitGroup{}

	t := time.NewTicker(30 * time.Second)
	tdoneCh := make(chan struct{})

	defer func() {
		t.Stop()
		close(tdoneCh)
		if pc != nil {
			pc.runningControl.Close()
			wgpc.Wait()
			close(pc.msgChanDone)
			wgpcmsgchan.Wait()
			close(pc.msgChan)

			err := pc.Close()
			if err != nil {
				p.sc.Log.Warn("Stopping worker for cchain %w", err)
			}
		}
	}()
	var err error
	pc, err = newContainerC(p.sc, p.conf)
	if err != nil {
		return err
	}

	pblockp1 := big.NewInt(0).Add(pc.block, big.NewInt(1))
	if pc.blockCount.Cmp(pblockp1) < 0 {
		conns1, err := p.sc.DatabaseOnly()
		if err != nil {
			return err
		}
		wgpc.Add(1)
		go pc.catchupBlock(conns1, pblockp1, wgpc)
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
		wgpcmsgchan.Add(1)
		go p.blockProcessor(pc, cl, conns1, wgpcmsgchan)
	}

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := pc.ProcessNextMessage()
			if pc.catchupErrs.GetValue() != nil {
				err = pc.catchupErrs.GetValue().(error)
				if !CChainNotReady(err) {
					failures++
					p.Failure()
					p.sc.Log.Error("Catchup error: %v", err)
				} else {
					p.sc.Log.Warn("%s", TrimNL(err.Error()))
				}
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
				if CChainNotReady(err) {
					p.sc.Log.Warn("%s", TrimNL(err.Error()))
					return nil
				}

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
				if p.runningControl.IsStopped() || pc.runningControl.IsStopped() {
					return
				}
			case <-tdoneCh:
				return
			}
		}
	}()

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

type localBlockObject struct {
	blockContainer *cblock.BlockContainer
	time           time.Time
}

func (p *ProducerCChain) processWork(conns *services.Connections, localBlock *localBlockObject) error {
	cblk, err := cblock.New(localBlock.blockContainer.Block)
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
		CreatedAt:     localBlock.time,
	}
	err = txPool.ComputeID()
	if err != nil {
		return err
	}
	err = p.updateTxPool(conns, txPool)
	if err != nil {
		return err
	}

	for _, txTranactionTraces := range localBlock.blockContainer.Traces {
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
			CreatedAt:     localBlock.time,
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

	for _, log := range localBlock.blockContainer.Logs {
		logBits, err := json.Marshal(log)
		if err != nil {
			return err
		}

		id, err := ids.ToID(hashing.ComputeHash256(logBits))
		if err != nil {
			return err
		}

		txPool := &services.TxPool{
			NetworkID:     p.conf.NetworkID,
			ChainID:       p.conf.CchainID,
			MsgKey:        id.String(),
			Serialization: logBits,
			Processed:     0,
			Topic:         p.topicLogs,
			CreatedAt:     localBlock.time,
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

type blockWorkContainer struct {
	errs        *avalancheGoUtils.AtomicInterface
	blockNumber *big.Int
}

func (p *ProducerCChain) blockProcessor(pc *producerCChainContainer, client *cblock.Client, conns *services.Connections, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		_ = conns.Close()
		client.Close()
	}()

	for {
		select {
		case <-pc.msgChanDone:
			return
		case blockWork := <-pc.msgChan:
			atomic.AddInt64(&pc.msgChanSz, -1)
			if blockWork.errs.GetValue() != nil {
				continue
			}

			blContainer, err := client.ReadBlock(blockWork.blockNumber, rpcTimeout)
			if err != nil {
				blockWork.errs.SetValue(err)
				continue
			}

			localBlockObject := &localBlockObject{blockContainer: blContainer, time: time.Now().UTC()}
			err = p.processWork(conns, localBlockObject)
			if err != nil {
				blockWork.errs.SetValue(err)
				continue
			}

			_ = metrics.Prometheus.CounterInc(p.metricProcessedCountKey)
			_ = metrics.Prometheus.CounterInc(services.MetricProduceProcessedCountKey)

			err = p.updateBlock(conns, blockWork.blockNumber, localBlockObject.time)
			if err != nil {
				blockWork.errs.SetValue(err)
				continue
			}
		}
	}
}
