// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"os"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/client"
	"github.com/ava-labs/ortelius/utils"
	"github.com/spf13/cobra"
)

func main() {
	// Execute root command and obtain the config object to use
	conf, err := Execute()
	if err != nil {
		utils.Die("Could not execute root command: %s", err.Error())
	}

	// Start client
	client := client.New(conf)
	if err := client.Listen(); err != nil {
		os.Exit(1)
	}
}

// Execute runs the root command for ortelius
func Execute() (conf *cfg.ClientConfig, confErr error) {
	rootCmd := &cobra.Command{
		Use:   "ortelius [context] [configuration file]\nex: ortelius producer /home/ccusce/kafka/avm.json",
		Short: "A producer/consumer launcher for the explorer backend.",
		Long:  "A producer/consumer launcher for the explorer backend.",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 2 {
				if err := cmd.Help(); err != nil {
					utils.Die("Help generation failed.")
				}
				utils.Die("Invalid number of arguments.")
			}
			context := args[0]
			confFile := args[1]
			conf, confErr = cfg.NewClientConfig(context, confFile)
		},
	}
	if err := rootCmd.Execute(); err != nil {
		utils.Die("Unable to launch: %s", err.Error())
	}
	return conf, confErr
}
