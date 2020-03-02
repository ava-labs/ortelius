package cmd

import (
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/utils"
	"github.com/spf13/cobra"
)

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:   "ortelius [context] [configuration file]\nex: ortelius producer /home/ccusce/kafka/avm.json",
	Short: "A producer/consumer launcher for the explorer backend.",
	Long:  "A producer/consumer launcher for the explorer backend.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Help()
			utils.Die("Invalid number of arguments.")
		}
		context := args[0]
		confFile := args[1]
		cfg.InitConfig(context, confFile)
	},
}

// Execute runs the root command for ortelius
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		utils.Die("Unable to launch: %s", err.Error())
	}
}
