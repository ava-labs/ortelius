package main

import (
	"os"

	"github.com/ava-labs/ortelius/client"
	"github.com/ava-labs/ortelius/cmd"
)

func main() {
	cmd.Execute()
	client := client.Client{}
	if len(os.Args) > 1 {
		client.Initialize()
		client.Listen()
		os.Exit(0)
	}
	cmd.RootCmd.Help()
	os.Exit(1)
}
