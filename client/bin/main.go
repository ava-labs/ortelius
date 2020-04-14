// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"errors"
	"log"

	"github.com/spf13/cobra"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client"
)

func main() {
	// Execute root command and obtain the config object to use
	conf, err := Execute()
	if err != nil {
		log.Fatalln("Could not execute root command:", err.Error())
	}

	// Start client
	client, err := client.New(conf)
	if err != nil {
		log.Fatalln("Could not create client:", err.Error())
	}
	if err := client.Listen(); err != nil {
		log.Fatalln("Listen error:", err.Error())
	}
}

// Execute runs the root command for ortelius
func Execute() (conf *cfg.ClientConfig, err error) {
	var confErr error
	rootCmd := &cobra.Command{
		Use:   "ortelius [context] [configuration file]\nex: ortelius producer /home/ccusce/kafka/avm.json",
		Short: "A producer/consumer launcher for the explorer backend.",
		Long:  "A producer/consumer launcher for the explorer backend.",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 2 {
				if err = cmd.Help(); err != nil {
					return
				}
				confErr = errors.New("Invalid number of arguments.")
				return
			}
			context := args[0]
			confFile := args[1]
			conf, confErr = cfg.NewClientConfig(context, confFile)
		},
	}
	if err = rootCmd.Execute(); err != nil {
		return nil, err
	}
	return conf, confErr
}
