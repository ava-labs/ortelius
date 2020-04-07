// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/utils"
	"github.com/spf13/cobra"
)

func main() {
	// Execute root command and obtain the config object to use
	conf, err := Execute()
	if err != nil {
		log.Fatalln("Could not execute root command:", err.Error())
	}

	// Create and start http server
	server, err := api.NewServer(conf)
	if err != nil {
		log.Fatalln("Could not create server:", err.Error())
	}

	go func() {
		if err := server.Listen(); err != nil {
			log.Fatalln("Server listen error:", err.Error())
		}
	}()

	// Wait for exit signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sigCh

	// Stop server
	if err := server.Shutdown(); err != nil {
		log.Fatalln("Server shutdown error:", err.Error())
	}
}

// Execute runs the root command for ortelius
func Execute() (conf cfg.APIConfig, confErr error) {
	rootCmd := &cobra.Command{
		Use:   "ortelius [configuration file]\nex: ortelius /var/lib/ortelius/api/config.json",
		Short: "An API for the explorer backend.",
		Long:  "An API explorer backend.",
		Run: func(cmd *cobra.Command, args []string) {
			confFile := ""
			if len(args) > 0 {
				confFile = args[0]
			}
			conf, confErr = cfg.NewAPIConfig(confFile)
		},
	}
	if err := rootCmd.Execute(); err != nil {
		utils.Die("Unable to launch: %s", err.Error())
	}
	return conf, confErr
}
