// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

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

	"github.com/ava-labs/avalanchego/utils/logging"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/metrics"
)

const (
	rpcTimeout        = 10 * time.Second
	kafkaWriteTimeout = 10 * time.Second
	dbReadTimeout     = 10 * time.Second
	dbWriteTimeout    = 10 * time.Second

	notFoundSleep  = 1 * time.Second
	readRPCTimeout = 500 * time.Millisecond

	blocksToQueue = 25
)

type ProducerCChain struct {
	id  string
	log logging.Logger

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
}

func NewProducerCChain() utils.ListenCloserFactory {
	return func(conf cfg.Config) utils.ListenCloser {
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
			conf:                    conf,
			log:                     conf.Log,
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
		time.Sleep(notFoundSleep)
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

func (p *ProducerCChain) ProcessNextMessage() error {
	current := new(big.Int)
	current.Set(p.block)

	type localBlockObject struct {
		block       *types.Block
		blockNumber *big.Int
		time        time.Time
	}

	var localBlocks []*localBlockObject

	consumeBlock := func() error {
		if len(localBlocks) == 0 {
			return nil
		}

		var blockNumberUpdates []*big.Int

		var kafkaMessages []kafka.Message

		for _, bl := range localBlocks {
			block, err := cblock.Marshal(bl.block)
			if err != nil {
				return err
			}

			kafkaMessage := kafka.Message{Value: block, Key: hashing.ComputeHash256(block)}
			kafkaMessages = append(kafkaMessages, kafkaMessage)

			blockNumberUpdates = append(blockNumberUpdates, bl.blockNumber)
		}

		localBlocks = nil

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
		}
		p.block = p.block.Add(p.block, big.NewInt(1))

		return nil
	}

	for {
		bl, err := p.readBlockFromRPC(current)
		if err != nil {
			err2 := consumeBlock()
			if err2 != nil {
				time.Sleep(readRPCTimeout)
				return err2
			}
			time.Sleep(readRPCTimeout)
			return err
		}

		ncurrent := new(big.Int)
		ncurrent.Set(current)
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

func (p *ProducerCChain) Failure() {
	err := metrics.Prometheus.CounterInc(p.metricFailureCountKey)
	if err != nil {
		p.log.Error("prometheus.CounterInc %s", err)
	}
}

func (p *ProducerCChain) Success() {
	err := metrics.Prometheus.CounterInc(p.metricSuccessCountKey)
	if err != nil {
		p.log.Error("prometheus.CounterInc %s", err)
	}
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

	n := new(big.Int)
	n, ok := n.SetString(block, 10)
	if !ok {
		return fmt.Errorf("invalid block %s", block)
	}
	p.block = n
	p.log.Info("starting processing block %s", p.block.String())
	return nil
}

func (p *ProducerCChain) Listen() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		p.log.Info("Started worker manager for cchain")
		defer p.log.Info("Exiting worker manager for cchain")
		defer wg.Done()

		// Keep running the worker until we're asked to stop
		var err error
		for !p.isStopping() {
			err = p.runProcessor()

			// If there was an error we want to log it, and iff we are not stopping
			// we want to add a retry delay.
			if err != nil {
				p.log.Error("Error running worker: %s", err.Error())
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
	p.log.Info("All workers stopped")
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
	conns, err := services.NewConnectionsFromConfig(p.conf.Services, false)
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
		p.log.Info("Not starting worker for cchain because we're stopping")
		return nil
	}

	p.log.Info("Starting worker for cchain")
	defer p.log.Info("Exiting worker for cchain")

	err := p.init()
	if err != nil {
		return err
	}
	defer func() {
		err := p.processorClose()
		if err != nil {
			p.log.Warn("Stopping worker for cchain %w", err)
		}
	}()

	// Create a closure that processes the next message from the backend
	var (
		successes          int
		failures           int
		nomsg              int
		processNextMessage = func() error {
			err := p.ProcessNextMessage()
			if err == nil {
				successes++
				p.Success()
				return nil
			}

			switch err {
			// This error is expected when the upstream service isn't producing
			case context.DeadlineExceeded:
				nomsg++
				p.log.Debug("context deadline exceeded")
				return nil

			case ErrNoMessage:
				nomsg++
				p.log.Debug("no message")
				return nil

			case io.EOF:
				p.log.Error("EOF")
				return io.EOF

			default:
				if strings.HasPrefix(err.Error(), "404 Not Found") {
					p.log.Warn("%s", err.Error())
					return nil
				}
				if strings.HasPrefix(err.Error(), "503 Service Unavailable") {
					p.log.Warn("%s", err.Error())
					return nil
				}
				if strings.HasSuffix(err.Error(), "connect: connection refused") {
					p.log.Warn("%s", err.Error())
					return nil
				}

				p.Failure()
				p.log.Error("Unknown error: %s", err.Error())
			}

			failures++
			return nil
		}
	)

	id := p.ID()

	// Log run statistics periodically until asked to stop
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			p.log.Info("IProcessor %s successes=%d failures=%d nomsg=%d", id, successes, failures, nomsg)
			if p.isStopping() {
				return
			}
		}
	}()

	// Process messages until asked to stop
	for !p.isStopping() {
		err := processNextMessage()
		if err == io.EOF && !p.isStopping() {
			return err
		}
	}

	return nil
}
