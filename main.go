package main

import (
	"os"

	"github.com/ava-labs/avash/cfg"
	"github.com/ava-labs/avash/cmd"
	"github.com/ava-labs/ortelius/client"
)

func main() {
	cmd.Execute()
	cfg.InitConfig()
	client := client.Client{}
	if len(os.Args) > 1 {
		client.Initialize()
		client.Listen(os.Args[1])
		os.Exit(0)
	}
	cmd.RootCmd.Help()
	os.Exit(1)
}
