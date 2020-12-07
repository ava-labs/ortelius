// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

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

	notFoundSleep = 1 * time.Second
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
		topicName := fmt.Sprintf("%d-cchain", conf.NetworkID)

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
			metricProcessedCountKey: "produce_records_processed_cchain",
			metricSuccessCountKey:   "produce_records_success_cchain",
			metricFailureCountKey:   "produce_records_failure_cchain",
			id:                      fmt.Sprintf("producer %d cchain", conf.NetworkID),
			writer:                  writer,

			quitCh: make(chan struct{}),
			doneCh: make(chan struct{}),
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

func (p *ProducerCChain) readBlockFromRPC() (*types.Block, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()

	bl, err := p.ethClient.BlockByNumber(ctx, p.block)
	if err == coreth.NotFound {
		time.Sleep(notFoundSleep)
		return nil, ErrNoMessage
	}
	if err != nil {
		return nil, err
	}

	return bl, nil
}

func (p *ProducerCChain) writeBlockToKafka(block []byte) error {
	ctx, cancelCTX := context.WithTimeout(context.Background(), kafkaWriteTimeout)
	defer cancelCTX()

	kmessage := kafka.Message{}
	kmessage.Value = block
	// compute hash before processing.
	kmessage.Key = hashing.ComputeHash256(kmessage.Value)
	if err := p.writer.WriteMessages(ctx, kmessage); err != nil {
		return err
	}

	return nil
}

func (p *ProducerCChain) updateBlock(block []byte, receivedAt time.Time) error {
	dbRunner, err := p.conns.DB().NewSession("updateBlock", dbWriteTimeout)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), dbWriteTimeout)
	defer cancelCtx()

	_, err = dbRunner.ExecContext(ctx,
		"insert into cvm_block (block,canonical_serialization,created_at,received_at) values ("+p.block.String()+",?,?,?)",
		block,
		time.Now(),
		receivedAt)
	if err != nil && !db.ErrIsDuplicateEntryError(err) {
		return err
	}
	if cfg.PerformUpdates {
		_, err = dbRunner.ExecContext(ctx,
			"update cvm_block set canonical_serialization=?,received_at=? where block="+p.block.String(),
			block,
			receivedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ProducerCChain) ProcessNextMessage() error {
	bl, err := p.readBlockFromRPC()
	if err != nil {
		return err
	}

	var block []byte = bl.ExtraData()
	if len(block) != 0 {
		err = p.writeBlockToKafka(block)
		if err != nil {
			return err
		}
	}

	if block == nil || len(block) > 64000 {
		block = []byte("")
	}

	err = p.updateBlock(block, bl.ReceivedAt)
	if err != nil {
		return err
	}

	p.block = p.block.Add(p.block, big.NewInt(1))
	return nil
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
		From("cvm_block").
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
