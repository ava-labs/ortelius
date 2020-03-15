package avm

import (
	"encoding/json"
	"fmt"

	"nanomsg.org/go/mangos/v2/protocol"

	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/platformvm"
	"github.com/ava-labs/gecko/vms/secp256k1fx"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
)

// AVM produces for the AVM
type AVM struct {
	log       logging.Logger
	sock      protocol.Socket
	producer  *kafka.Producer
	filter    Filter
	topic     string
	chainID   ids.ID
	genesisTx *platformvm.CreateChainTx
	networkID uint32
	vmID      ids.ID
	ctx       *snow.Context
	avm       *avm.VM
}

// Initialize the producer using the configs passed as an argument
func (p *AVM) Initialize(log logging.Logger, kafkaConf kafka.ConfigMap, sock protocol.Socket) error {
	var err error
	p.log = log
	p.sock = sock
	p.topic = cfg.Viper.GetString("chainID")
	log.Info("chainID: %s", cfg.Viper.GetString("chainID"))
	if p.chainID, err = ids.FromString(p.topic); err != nil {
		return err
	}
	filter := cfg.Viper.GetStringMap("filter")
	p.filter = Filter{}
	log.Info("filter %s", cfg.Viper.GetStringMap("filter"))
	if err := p.filter.Initialize(filter); err != nil {
		return err
	}
	p.networkID = cfg.Viper.GetUint32("networkID")
	p.vmID = avm.ID
	p.genesisTx = genesis.VMGenesis(p.networkID, p.vmID)
	p.ctx = &snow.Context{
		NetworkID: p.networkID,
		ChainID:   p.chainID,
		Log:       logging.NoLog{},
	}
	p.avm = &avm.VM{}
	echan := make(chan common.Message, 1)
	fxids := p.genesisTx.FxIDs
	fxs := []*common.Fx{}
	for _, fxID := range fxids {
		switch {
		case fxID.Equals(secp256k1fx.ID):
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		default:
			return fmt.Errorf("Unknown FxID: %s", secp256k1fx.ID)
		}
	}
	p.log.Info("%+v", fxs)
	p.avm.Initialize(p.ctx, &nodb.Database{}, p.genesisTx.Bytes(), echan, fxs)

	if p.producer, err = kafka.NewProducer(&kafkaConf); err != nil {
		return err
	}

	return nil
}

// Close shuts down the producer
func (p *AVM) Close() error {
	p.producer.Close()
	return nil
}

// Accept takes in a message from the IPC socket and writes it to Kafka
func (p *AVM) Accept() error {
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
		Value: data,
		Key:   tx.ID().Bytes(),
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
	}, tx.ID(), nil
}
