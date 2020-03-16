package avm

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ava-labs/gecko/snow/consensus/snowstorm"

	"github.com/ava-labs/gecko/database/nodb"
	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/engine/common"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/gecko/vms/avm"
	"github.com/ava-labs/gecko/vms/platformvm"
	"github.com/ava-labs/gecko/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/utils"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// AVM produces for the AVM
type AVM struct {
	log       logging.Logger
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
func (p *AVM) Initialize(log logging.Logger) error {
	var err error
	p.log = log
	p.topic = cfg.Viper.GetString("chainID")
	log.Info("chainid %s", cfg.Viper.GetString("chainID"))
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
	kconf := cfg.Viper.Sub("kafka")
	kafkaConf := kafka.ConfigMap{}
	kc := kconf.AllSettings()
	for k, v := range kc {
		kafkaConf[k] = v
	}
	if p.producer, err = kafka.NewProducer(&kafkaConf); err != nil {
		return err
	}

	return nil
}

// Close shuts down the producer
func (p *AVM) Close() {
	p.producer.Close()
}

// Events returns delivery events channel
func (p *AVM) Events() chan kafka.Event {
	return p.producer.Events()
}

func (p *AVM) makeMessage(msg []byte) (*kafka.Message, error) {
	var data []byte
	var err error
	p.log.Info(hex.EncodeToString(msg))
	topicPartition := kafka.TopicPartition{
		Topic:     &p.topic,
		Partition: kafka.PartitionAny,
	}
	var tx snowstorm.Tx
	if tx, err = p.avm.ParseTx(msg); err != nil {
		return nil, err
	}

	if data, err = json.Marshal(tx); err != nil {
		return nil, err
	}

	message := utils.Message{
		Raw:  msg,
		Key:  tx.ID().Bytes(),
		Data: data,
	}

	km := &kafka.Message{
		TopicPartition: topicPartition,
		Key:            message.Key,
		Value:          data,
	}

	return km, nil
}

// Produce produces for the topic as an AVM tx
func (p *AVM) Produce(msg []byte) error {
	var message *kafka.Message
	var err error
	if p.filter.Filter(msg) {
		return nil // filter returned true, so we filter it
	}

	if message, err = p.makeMessage(msg); err != nil {
		return err
	}

	return p.producer.Produce(message, nil)
}
