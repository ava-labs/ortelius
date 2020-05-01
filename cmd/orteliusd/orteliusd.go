// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"
)

const (
	rootCmdUse  = "orteliusd [comamnd] [configuration file]\nex: ortelius producer config.json"
	rootCmdDesc = "Processor daemons for Ortelius."

	apiCmdUse         = "api"
	apiCmdDescription = "Runs the API damon"

	streamConsumerCmdUse  = "stream-consumer"
	streamConsumerCmdDesc = "Runs the stream consumer daemon"

	streamProducerCmdUse  = "stream-producer"
	streamProducerCmdDesc = "Runs the stream producer daemon"
)

func main() {
	// Execute root command and obtain the listenCloser to use
	lc, err := execute()
	if err != nil {
		log.Fatalln(err)
	}

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

type listenCloser interface {
	Listen() error
	Close() error
}

// Execute runs the root command for ortelius
func execute() (listenCloser, error) {
	rootCmd := &cobra.Command{Use: rootCmdUse, Short: rootCmdDesc, Long: rootCmdDesc}

	var err error
	var lc listenCloser
	rootCmd.AddCommand(&cobra.Command{
		Use:   apiCmdUse,
		Short: apiCmdDescription,
		Long:  apiCmdDescription,
		Run: func(_ *cobra.Command, args []string) {
			var config cfg.APIConfig
			config, err = cfg.NewAPIConfig(append(args, "")[0])
			if err != nil {
				return
			}
			lc, err = api.NewServer(config)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   streamConsumerCmdUse,
		Short: streamConsumerCmdDesc,
		Long:  streamConsumerCmdDesc,
		Run: func(_ *cobra.Command, args []string) {
			lc, err = runStreamProcessorCmd(stream.NewConsumer, args)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   streamProducerCmdUse,
		Short: streamProducerCmdDesc,
		Long:  streamProducerCmdDesc,
		Run: func(_ *cobra.Command, args []string) {
			lc, err = runStreamProcessorCmd(stream.NewProducer, args)
		},
	})

	if runErr := rootCmd.Execute(); runErr != nil {
		return nil, runErr
	}
	return lc, err
}

func runStreamProcessorCmd(factory streamProcessorFactory, args []string) (listenCloser, error) {
	config, err := cfg.NewClientConfig("", append(args, "")[0])
	if err != nil {
		return nil, err
	}

	return newStreamProcessorManager(config, factory), nil
}
