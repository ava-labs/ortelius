package avm

import (
	"encoding/json"
	"fmt"

	"nanomsg.org/go/mangos/v2/protocol"

	"github.com/ava-labs/gecko/database"
	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/secp256k1fx"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
)

// AVM produces for the AVM, taking messages from the IPC socket and writing
// formatting txs to Kafka
type AVM struct {
	avm            *avm.VM
	log            logging.Logger
	sock           protocol.Socket
	filter         Filter
	topicPartition kafka.TopicPartition
	producer       *kafka.Producer
}

// Initialize the producer using the configs passed as an argument
func (p *AVM) Initialize(log logging.Logger, conf *cfg.Config, sock protocol.Socket) error {
	p.log = log
	p.sock = sock

	// Create filter
	p.filter = Filter{}
	if err := p.filter.Initialize(conf.Filter); err != nil {
		return err
	}

	// Create the AVM for the configured chain
	var err error
	if p.avm, err = p.newAVM(conf.ChainID, conf.NetworkID); err != nil {
		return err
	}

	// Set the Kafka config and start the producer
	chainIDStr := conf.ChainID.String()
	p.topicPartition = kafka.TopicPartition{
		Topic:     &chainIDStr,
		Partition: kafka.PartitionAny,
	}
	if p.producer, err = kafka.NewProducer(&conf.Kafka); err != nil {
		return err
	}

	log.Info("Initialized producer with chainID=%s and filter=%s", conf.ChainID, conf.Filter)

	return nil
}

// Close shuts down the producer
func (p *AVM) Close() error {
	p.producer.Close()
	return nil
}

// ProcessNextMessage takes in a message from the IPC socket and writes it to
// Kafka
func (p *AVM) ProcessNextMessage() error {
	txBytes, err := p.sock.Recv()
	if err != nil {
		return err
	}
	return p.writeTx(txBytes)
}

// Events returns delivery events channel
func (p *AVM) Events() chan kafka.Event {
	return p.producer.Events()
}

// writeTx takes in a serialized tx and writes it to Kafka
func (p *AVM) writeTx(msg []byte) error {
	if p.filter.Filter(msg) {
		return nil // filter returned true, so we filter it
	}

	message, txID, err := p.makeMessage(msg)
	if err != nil {
		return err
	}

	p.log.Info("Writing tx message: %s", txID.String())
	return p.producer.Produce(message, nil)
}

// makeMessage takes in a raw tx and builds a Kafka message for it
func (p *AVM) makeMessage(msg []byte) (*kafka.Message, ids.ID, error) {
	var (
		err  error
		data []byte
		tx   snowstorm.Tx
	)

	if tx, err = p.avm.ParseTx(msg); err != nil {
		return nil, ids.Empty, err
	}

	if data, err = json.Marshal(tx); err != nil {
		return nil, ids.Empty, err
	}

	return &kafka.Message{
		Value:          data,
		Key:            tx.ID().Bytes(),
		TopicPartition: p.topicPartition,
	}, tx.ID(), nil
}

// newAVM creates an AVM instance that we can use to parse txs
func (p *AVM) newAVM(chainID ids.ID, networkID uint32) (*avm.VM, error) {
	genesisTX := genesis.VMGenesis(networkID, avm.ID)

	fxIDs := genesisTX.FxIDs
	fxs := make([]*common.Fx, 0, len(fxIDs))
	for _, fxID := range fxIDs {
		switch {
		case fxID.Equals(secp256k1fx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		default:
			return nil, fmt.Errorf("Unknown FxID: %s", secp256k1fx.ID)
		}
	}

	ctx := &snow.Context{
		NetworkID: networkID,
		ChainID:   chainID,
		Log:       logging.NoLog{},
	}

	// Initialize an AVM to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err := vm.Initialize(ctx, &nodb.Database{}, genesisTX.GenesisData, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, err
	}
	return vm, nil
}
