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

	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"

	"github.com/ava-labs/avalanchego/ids"

	cblock "github.com/ava-labs/ortelius/models"

	"github.com/ava-labs/avalanchego/utils/wrappers"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/ortelius/services/db"

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
	rpcTimeout        = 10 * time.Second
	kafkaWriteTimeout = 10 * time.Second
	dbReadTimeout     = 10 * time.Second
	dbWriteTimeout    = 10 * time.Second

	readRPCTimeout = 500 * time.Millisecond

	blocksToQueue = 25

	defaultWorkerCChainSize = 4
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
	quitCh chan struct{}
	doneCh chan struct{}
	topic  string

	worker utils.Worker
}

func NewProducerCChain() utils.ListenCloserFactory {
	return func(sc *services.Control, conf cfg.Config, _ int, _ int) utils.ListenCloser {
		topicName := fmt.Sprintf("%d-%s-cchain", conf.NetworkID, conf.CchainID)

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

		accumfunc := func(part int, v interface{}) {
			p.processWork(part, v)
		}

		p.worker = utils.NewWorker(defaultWorkerCChainSize, defaultBufferedWriterMsgQueueSize, accumfunc)

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

func (p *ProducerCChain) readBlockFromRPC(blockNumber *big.Int) (*types.Block, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()

	bl, err := p.ethClient.BlockByNumber(ctx, blockNumber)
	if err == coreth.NotFound {
		return nil, ErrNoMessage
	}
	if err != nil {
		return nil, err
	}

	return bl, nil
}

func (p *ProducerCChain) writeMessagesToKafka(messages ...kafka.Message) error {
	ctx, cancelCTX := context.WithTimeout(context.Background(), kafkaWriteTimeout)
	defer cancelCTX()

	return p.writer.WriteMessages(ctx, messages...)
}

func (p *ProducerCChain) updateTxPool(txPool *services.TxPool) error {
	dbRunner, err := p.conns.DB().NewSession("updateTxPool", dbWriteTimeout)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	return p.sc.Persist.InsertTxPool(ctx, dbRunner, txPool)
}

func (p *ProducerCChain) updateBlock(blockNumber *big.Int, updateTime time.Time) error {
	dbRunner, err := p.conns.DB().NewSession("updateBlock", dbWriteTimeout)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	_, err = dbRunner.ExecContext(ctx,
		"insert into cvm_blocks (block,created_at) values ("+blockNumber.String()+",?)",
		updateTime)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return err
	}
	return nil
}

func (p *ProducerCChain) fetchLatest() (uint64, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()
	return p.ethClient.BlockNumber(ctx)
}

type localBlockObject struct {
	block       *types.Block
	blockNumber *big.Int
	time        time.Time
}

func (p *ProducerCChain) ProcessNextMessage() error {
	current := big.NewInt(0).Set(p.block)

	localBlocks := make([]*localBlockObject, 0, blocksToQueue)

	consumeBlock := func() error {
		if len(localBlocks) == 0 {
			return nil
		}

		if p.sc.IsDBPoll {
			errs := avlancheGoUtils.AtomicInterface{}

			for _, bl := range localBlocks {
				p.worker.Enque(&WorkPacketCChain{bl: bl, errs: &errs})

				current.Set(bl.blockNumber)
				if current.Uint64()%1000 == 0 {
					p.sc.Log.Info("current block %s", current.String())
				}
			}

			for p.worker.JobCnt() > 0 && !p.worker.IsFinished() {
				time.Sleep(time.Millisecond)
			}
			if errs.GetValue() != nil {
				return errs.GetValue().(error)
			}

			p.block = big.NewInt(0).Add(current, big.NewInt(1))

			return nil
		}

		var blockNumberUpdates []*big.Int

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

			kafkaMessage := kafka.Message{Value: block, Key: hashing.ComputeHash256(block)}
			kafkaMessages = append(kafkaMessages, kafkaMessage)

			blockNumberUpdates = append(blockNumberUpdates, bl.blockNumber)
		}

		localBlocks = make([]*localBlockObject, 0, blocksToQueue)

		err := p.writeMessagesToKafka(kafkaMessages...)
		if err != nil {
			return err
		}

		for _, blockNumber := range blockNumberUpdates {
			err := p.updateBlock(blockNumber, time.Now().UTC())
			if err != nil {
				return err
			}

			p.block.Set(blockNumber)
			if p.block.Uint64()%1000 == 0 {
				p.sc.Log.Info("current block %s", p.block.String())
			}
		}
		p.block = big.NewInt(0).Add(p.block, big.NewInt(1))

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
		if lblocknext.Cmp(p.block) <= 0 {
			time.Sleep(readRPCTimeout)
			return ErrNoMessage
		}

		for !p.isStopping() && lblocknext.Cmp(p.block) > 0 {
			bl, err := p.readBlockFromRPC(current)
			if err != nil {
				time.Sleep(readRPCTimeout)
				return err
			}
			_ = metrics.Prometheus.CounterInc(p.metricProcessedCountKey)
			_ = metrics.Prometheus.CounterInc(services.MetricProduceProcessedCountKey)

			ncurrent := big.NewInt(0).Set(current)
			localBlocks = append(localBlocks, &localBlockObject{block: bl, blockNumber: ncurrent, time: time.Now().UTC()})
			if len(localBlocks) > blocksToQueue {
				err = consumeBlock()
				if err != nil {
					return err
				}
			}

			current = current.Add(current, big.NewInt(1))
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

	dbRunner, err := p.conns.DB().NewSession("getBlock", dbReadTimeout)
	if err != nil {
		return err
	}
	ctx, cancelCtx := context.WithTimeout(context.Background(), dbReadTimeout)
	defer cancelCtx()

	var block string
	_, err = dbRunner.Select("cast(case when max(block) is null then 0 else max(block) end as char) as block").
		From("cvm_blocks").
		LoadContext(ctx, &block)
	if err != nil {
		return err
	}

	n, ok := big.NewInt(0).SetString(block, 10)
	if !ok {
		return fmt.Errorf("invalid block %s", block)
	}
	p.block = n
	if p.block.String() != "0" {
		p.block = p.block.Add(p.block, big.NewInt(1))
	}
	p.sc.Log.Info("starting processing block %s", p.block.String())
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
	bl   *localBlockObject
	errs *avlancheGoUtils.AtomicInterface
}

func (p *ProducerCChain) processWork(_ int, workPacketI interface{}) {
	wp, ok := workPacketI.(*WorkPacketCChain)
	if !ok {
		return
	}

	cblk, err := cblock.New(wp.bl.block)
	if err != nil {
		wp.errs.SetValue(err)
		return
	}
	if cblk == nil {
		return
	}
	// wipe before re-encoding
	cblk.Txs = nil
	block, err := json.Marshal(cblk)
	if err != nil {
		wp.errs.SetValue(err)
		return
	}

	key := hashing.ComputeHash256(block)

	id, err := ids.ToID(key)
	if err != nil {
		wp.errs.SetValue(err)
		return
	}

	txPool := &services.TxPool{
		NetworkID:     p.conf.NetworkID,
		ChainID:       p.conf.CchainID,
		MsgKey:        id.String(),
		Serialization: block,
		Processed:     0,
		Topic:         p.topic,
		CreatedAt:     time.Now(),
	}
	err = txPool.ComputeID()
	if err != nil {
		wp.errs.SetValue(err)
		return
	}
	err = p.updateTxPool(txPool)
	if err != nil {
		wp.errs.SetValue(err)
		return
	}

	err = p.updateBlock(wp.bl.blockNumber, time.Now().UTC())
	if err != nil {
		wp.errs.SetValue(err)
		return
	}
}
