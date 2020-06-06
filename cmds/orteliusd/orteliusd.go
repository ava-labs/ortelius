// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"

	// Register service plugins
	_ "github.com/ava-labs/ortelius/services/avm_index"
)

const (
	rootCmdUse  = "orteliusd [command]\nex: orteliusd api"
	rootCmdDesc = "Daemons for Ortelius."

	apiCmdUse  = "api"
	apiCmdDesc = "Runs the API daemon"

	streamConsumerCmdUse  = "stream-consumer"
	streamConsumerCmdDesc = "Runs the stream consumer daemon"

	streamProducerCmdUse  = "stream-producer"
	streamProducerCmdDesc = "Runs the stream producer daemon"
)

// listenCloser listens for messages until it's asked to close
type listenCloser interface {
	Listen() error
	Close() error
}

func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// Execute runs the root command for ortelius
func execute() error {
	var (
		runErr     error
		configFile string
		rootCmd    = &cobra.Command{
			Use:   rootCmdUse,
			Short: rootCmdDesc,
			Long:  rootCmdDesc,
		}
	)

	// Add flags
	rootCmd.
		PersistentFlags().
		StringVarP(&configFile, "config", "c", "config.json", "Config file location")

	// Add commands
	rootCmd.AddCommand(&cobra.Command{
		Use:   apiCmdUse,
		Short: apiCmdDesc,
		Long:  apiCmdDesc,
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, args []string) {

			var config cfg.APIConfig
			var lc listenCloser
			if config, runErr = cfg.NewAPIConfig(configFile); runErr != nil {
				return
			}
			if lc, runErr = api.NewServer(config); runErr != nil {
				return
			}
			runListenCloser(lc)
		},
	})


   	rootCmd.AddCommand(&cobra.Command{
		Use:   streamConsumerCmdUse,
		Short: streamConsumerCmdDesc,
		Long:  streamConsumerCmdDesc,
		Run:   streamProcessorCmdRunFn(configFile, &runErr, stream.NewConsumer),
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   streamProducerCmdUse,
		Short: streamProducerCmdDesc,
		Long:  streamProducerCmdDesc,
		Run:   streamProcessorCmdRunFn(configFile, &runErr, stream.NewProducer),
	})

	if err := rootCmd.Execute(); err != nil {
		return err
	}

	return runErr
}

// runListenCloser runs the listenCloser until signaled to stop
func runListenCloser(lc listenCloser) {
	// Start listening in the background
	go func() {
		if err := lc.Listen(); err != nil {
			log.Fatalln("Daemon listen error:", err.Error())
		}
	}()

	// Wait for exit signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sigCh

	// Stop server
	if err := lc.Close(); err != nil {
		log.Fatalln("Daemon shutdown error:", err.Error())
	}
}

func streamProcessorCmdRunFn(configFile string, runErr *error, factory streamProcessorFactory) func(_ *cobra.Command, _ []string) {
	return func(_ *cobra.Command, _ []string) {
		config, err := cfg.NewClientConfig("", configFile)

		if err != nil {
			*runErr = err
			return
		}
		runListenCloser(newStreamProcessorManager(config, factory))
	}
}
