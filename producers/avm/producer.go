package avm

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// AVM produces for the AVM
type AVM struct {
	producer *kafka.Producer
	filter   Filter
	topic    string
}

// Initialize the producer with the topic and filter values
func (p *AVM) Initialize(topic string, filter string) error {
	p.topic = topic
	if err := p.filter.Initialize(filter); err != nil {
		return err
	}
	var err error
	p.producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"client.id":          "avm",
		"enable.idempotence": true,
		"bootstrap.servers":  "b-2.kafkaclusterdev.uhplt2.c7.kafka.us-east-1.amazonaws.com:9092,b-3.kafkaclusterdev.uhplt2.c7.kafka.us-east-1.amazonaws.com:9092,b-1.kafkaclusterdev.uhplt2.c7.kafka.us-east-1.amazonaws.com:9092",
	})
	if err != nil {
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

// Produce produces for the topic as an AVM tx
func (p *AVM) Produce(msg []byte) error {
	if p.filter.Filter(msg) {
		return nil // filter returned true, so we filter it
	}
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value: msg,
	}, nil)
}
