package main

import (
	"os"

	"github.com/ava-labs/ortelius/client"
	"github.com/ava-labs/ortelius/cmd"
	"github.com/ava-labs/ortelius/utils"
)

func main() {
	// Execute root command and obtain the config object to use
	conf, err := cmd.Execute()
	if err != nil {
		utils.Die("Could not execute root command: %s", err.Error())
	}

	// Start client
	client := client.New(conf)
	if err := client.Listen(); err != nil {
		os.Exit(1)
	}
}
