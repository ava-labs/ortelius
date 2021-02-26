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
	"time"

	"github.com/ava-labs/avalanchego/ids"
	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/avalanchego/utils/wrappers"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/coreth/core/types"

	"github.com/ava-labs/avalanchego/utils/hashing"

	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/coreth"

	"github.com/ava-labs/coreth/ethclient"
	"github.com/ava-labs/coreth/rpc"
	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

const (
	rpcTimeout        = time.Minute
	kafkaWriteTimeout = 10 * time.Second
	dbReadTimeout     = 10 * time.Second
	dbWriteTimeout    = time.Minute

	readRPCTimeout = 500 * time.Millisecond

	blocksToQueue = 25
)

type ProducerCChain struct {
	id string
	sc *services.Control

	// metrics
	metricProcessedCountKey string
	metricSuccessCountKey   string
	metricFailureCountKey   string

	rpcClient *rpc.Client
	ethClient *ethclient.Client
	conns     *services.Connections
	block     *big.Int
	writer    *kafka.Writer
	conf      cfg.Config

	// Concurrency control
	quitCh     chan struct{}
	doneCh     chan struct{}
	topic      string
	topicTrc   string
	blockCount *big.Int

	ethClientLock sync.Mutex
}

func NewProducerCChain() utils.ListenCloserFactory {
	return func(sc *services.Control, conf cfg.Config, _ int, _ int) utils.ListenCloser {
		topicName := fmt.Sprintf("%d-%s-cchain", conf.NetworkID, conf.CchainID)
		topicTrcName := fmt.Sprintf("%d-%s-cchain-trc", conf.NetworkID, conf.CchainID)

		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:      conf.Brokers,
			Topic:        topicName,
			Balancer:     &kafka.LeastBytes{},
			BatchBytes:   ConsumerMaxBytesDefault,
			BatchSize:    defaultBufferedWriterSize,
			WriteTimeout: defaultWriteTimeout,
			RequiredAcks: int(kafka.RequireAll),
		})

		p := &ProducerCChain{
			topic:                   topicName,
			topicTrc:                topicTrcName,
			conf:                    conf,
			sc:                      sc,
			metricProcessedCountKey: fmt.Sprintf("produce_records_processed_%s_cchain", conf.CchainID),
			metricSuccessCountKey:   fmt.Sprintf("produce_records_success_%s_cchain", conf.CchainID),
			metricFailureCountKey:   fmt.Sprintf("produce_records_failure_%s_cchain", conf.CchainID),
			id:                      fmt.Sprintf("producer %d %s cchain", conf.NetworkID, conf.CchainID),
			writer:                  writer,
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

// Close shuts down the producer
func (p *ProducerCChain) Close() error {
	close(p.quitCh)
	<-p.doneCh
	return nil
}

func (p *ProducerCChain) ID() string {
	return p.id
}

type TracerParam struct {
	Tracer string `json:"tracer"`
}

func (p *ProducerCChain) readBlockFromRPC(blockNumber *big.Int) (*types.Block, []*cblock.TransactionTrace, error) {
	p.ethClientLock.Lock()
	defer p.ethClientLock.Unlock()

	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()

	bl, err := p.ethClient.BlockByNumber(ctx, blockNumber)
	if err == coreth.NotFound {
		return nil, nil, ErrNoMessage
	}
	if err != nil {
		return nil, nil, err
	}

	txTraces := make([]*cblock.TransactionTrace, 0, len(bl.Transactions()))
	for _, tx := range bl.Transactions() {
		txh := tx.Hash().Hex()
		if !strings.HasPrefix(txh, "0x") {
			txh = "0x" + txh
		}
		var results []interface{}
		err = p.rpcClient.CallContext(ctx, &results, "debug_traceTransaction", txh, TracerParam{Tracer: utils.Tracer})
		if err != nil {
			return nil, nil, err
		}
		for ipos, result := range results {
			traceBits, err := json.Marshal(result)
			if err != nil {
				return nil, nil, err
			}
			txTraces = append(txTraces,
				&cblock.TransactionTrace{
					Hash:  txh,
					Idx:   uint32(ipos),
					Trace: traceBits,
				},
			)
		}
	}

	return bl, txTraces, nil
}

func (p *ProducerCChain) writeMessagesToKafka(messages ...kafka.Message) error {
	ctx, cancelCTX := context.WithTimeout(context.Background(), kafkaWriteTimeout)
	defer cancelCTX()

	return p.writer.WriteMessages(ctx, messages...)
}

func (p *ProducerCChain) updateTxPool(txPool *services.TxPool) error {
	sess := p.conns.DB().NewSessionForEventReceiver(p.conns.StreamDBDedup().NewJob("update-tx-pool"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.InsertTxPool(ctx, sess, txPool)
}

func (p *ProducerCChain) updateBlock(blockNumber *big.Int, updateTime time.Time) error {
	sess := p.conns.DB().NewSessionForEventReceiver(p.conns.StreamDBDedup().NewJob("update-block"))

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	cvmBlocks := &services.CvmBlocks{
		Block:     blockNumber.String(),
		CreatedAt: updateTime,
	}
	return p.sc.Persist.InsertCvmBlocks(ctx, sess, cvmBlocks)
}

func (p *ProducerCChain) fetchLatest() (uint64, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()
	return p.ethClient.BlockNumber(ctx)
}

type localBlockObject struct {
	block  *types.Block
	time   time.Time
	traces []*cblock.TransactionTrace
}

func (p *ProducerCChain) ProcessNextMessage() error {
	current := big.NewInt(0).Set(p.block)

	localBlocks := make([]*localBlockObject, 0, blocksToQueue)

	consumeBlock := func() error {
		if len(localBlocks) == 0 {
			return nil
		}

		defer func() {
			localBlocks = make([]*localBlockObject, 0, blocksToQueue)
		}()

		if p.sc.IsDBPoll {
			for _, bl := range localBlocks {
				wp := &WorkPacketCChain{localBlock: bl}
				err := p.processWork(wp)
				if err != nil {
					return err
				}
			}
		} else {
			var kafkaMessages []kafka.Message

			for _, bl := range localBlocks {
				cblk, err := cblock.New(bl.block)
				if err != nil {
					return err
				}
				if cblk == nil {
					return fmt.Errorf("invalid block")
				}
				// wipe before re-encoding
				cblk.Txs = nil
				block, err := json.Marshal(cblk)
				if err != nil {
					return err
				}

				kafkaMessages = append(kafkaMessages,
					kafka.Message{Value: block, Key: hashing.ComputeHash256(block)},
				)
			}

			err := p.writeMessagesToKafka(kafkaMessages...)
			if err != nil {
				return err
			}
		}

		for _, bl := range localBlocks {
			err := p.updateBlock(bl.block.Number(), time.Now().UTC())
			if err != nil {
				return err
			}
			p.block.Set(bl.block.Number())
		}

		return nil
	}

	defer func() {
		err := consumeBlock()
		if err != nil {
			p.sc.Log.Warn("consume block error %v", err)
		}
	}()

	for !p.isStopping() {
		lblock, err := p.fetchLatest()
		if err != nil {
			time.Sleep(readRPCTimeout)
			return err
		}

		lblocknext := big.NewInt(0).SetUint64(lblock)
		if lblocknext.Cmp(current) <= 0 {
			time.Sleep(readRPCTimeout)
			return ErrNoMessage
		}

		for !p.isStopping() && lblocknext.Cmp(current) > 0 {
			ncurrent := big.NewInt(0).Add(current, big.NewInt(1))
			bl, traces, err := p.readBlockFromRPC(ncurrent)
			if err != nil {
				time.Sleep(readRPCTimeout)
				return err
			}
			_ = metrics.Prometheus.CounterInc(p.metricProcessedCountKey)
			_ = metrics.Prometheus.CounterInc(services.MetricProduceProcessedCountKey)

			localBlocks = append(localBlocks, &localBlockObject{block: bl, traces: traces, time: time.Now().UTC()})
			if len(localBlocks) > blocksToQueue {
				err = consumeBlock()
				if err != nil {
					return err
				}
			}

			current = big.NewInt(0).Add(current, big.NewInt(1))
		}

		err = consumeBlock()
		if err != nil {
			return err
		}
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

func (p *ProducerCChain) getBlock() error {
	var err error

	job := p.conns.Stream().NewJob("get-block")
	sess := p.conns.DB().NewSessionForEventReceiver(job)

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
	cblock, ok := big.NewInt(0).SetString(maxBlock.BlockCount, 10)
	if !ok {
		return fmt.Errorf("invalid block %s", maxBlock.BlockCount)
	}
	p.block = mblock
	p.blockCount = cblock
	p.sc.Log.Info("starting processing block %s cnt %s", p.block.String(), p.blockCount.String())
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

func (p *ProducerCChain) init() error {
	conns, err := p.sc.DatabaseOnly()
	if err != nil {
		return err
	}

	p.conns = conns

	err = p.getBlock()
	if err != nil {
		return err
	}

	rc, err := rpc.Dial(p.conf.Producer.CChainRPC)
	if err != nil {
		return err
	}
	p.rpcClient = rc
	p.ethClient = ethclient.NewClient(rc)

	return nil
}

func (p *ProducerCChain) processorClose() error {
	p.sc.Log.Info("close %s", p.id)
	if p.rpcClient != nil {
		p.rpcClient.Close()
	}
	errs := wrappers.Errs{}
	if p.conns != nil {
		errs.Add(p.conns.Close())
	}
	return errs.Err
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

	defer func() {
		err := p.processorClose()
		if err != nil {
			p.sc.Log.Warn("Stopping worker for cchain %w", err)
		}
	}()
	err := p.init()
	if err != nil {
		return err
	}

	pblockp1 := big.NewInt(0).Add(p.block, big.NewInt(1))
	if p.blockCount.Cmp(pblockp1) < 0 {
		go p.catchupBlock(pblockp1)
	}

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := p.ProcessNextMessage()

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

func (p *ProducerCChain) processWork(wp *WorkPacketCChain) error {
	cblk, err := cblock.New(wp.localBlock.block)
	if err != nil {
		return err
	}
	if cblk == nil {
		return fmt.Errorf("cblock is nil")
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
	err = p.updateTxPool(txPool)
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
		err = p.updateTxPool(txPool)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ProducerCChain) catchupBlock(catchupBlock *big.Int) {
	conns, err := p.sc.DatabaseOnly()
	if err != nil {
		p.sc.Log.Warn("catchupBock %v", err)
		return
	}
	defer func() {
		_ = conns.Close()
	}()

	sess := p.conns.DB().NewSessionForEventReceiver(p.conns.StreamDBDedup().NewJob("catchup-block"))

	ctx := context.Background()

	startBlock := big.NewInt(0)
	endBlock := big.NewInt(0)
	for endBlock.Cmp(catchupBlock) < 0 {
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
			p.sc.Log.Warn("catchupBock %v", err)
			return
		}
		blockMap := make(map[string]struct{})
		for _, bl := range cvmBlocks {
			blockMap[bl.Block] = struct{}{}
		}
		for startBlock.Cmp(endBlock) < 0 {
			if _, ok := blockMap[startBlock.String()]; !ok {
				p.sc.Log.Info("refill %v", startBlock.String())
				bl, traces, err := p.readBlockFromRPC(startBlock)
				if err != nil {
					p.sc.Log.Warn("catchupBock %v", err)
					return
				}

				localBlockObject := &localBlockObject{block: bl, traces: traces, time: time.Now().UTC()}

				wp := &WorkPacketCChain{localBlock: localBlockObject}
				err = p.processWork(wp)
				if err != nil {
					p.sc.Log.Warn("catchupBock %v", err)
					return
				}

				err = p.updateBlock(startBlock, time.Now().UTC())
				if err != nil {
					p.sc.Log.Warn("catchupBock %v", err)
					return
				}
			}
			startBlock = big.NewInt(0).Add(startBlock, big.NewInt(1))
		}
	}

	p.sc.Log.Info("catchup complete")
}
