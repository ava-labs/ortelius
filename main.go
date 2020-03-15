package main

import (
	"os"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client"
	"github.com/ava-labs/ortelius/cmd"
)

func main() {
	cmd.Execute()

	// Not enough arguments, show help and quit
	if len(os.Args) <= 1 {
		cmd.RootCmd.Help()
		os.Exit(1)
	}

	kafkaConf := kafka.ConfigMap{}
	for k, v := range cfg.Viper.GetStringMap("kafka") {
		kafkaConf[k] = v
	}

	// Start client
	client := client.New(kafkaConf)
	if err := client.Listen(); err != nil {
		os.Exit(1)
	}
}
