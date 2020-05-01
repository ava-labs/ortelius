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
	rootCmdUse  = "orteliusd [comamnd] [configuration file]\nex: ortelius producer config.json"
	rootCmdDesc = "Processor daemons for Ortelius."

	apiCmdUse         = "api"
	apiCmdDescription = "Runs the API damon"

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
		cmdErr   error
		runLCCmd = func(lc listenCloser, err error) {
			if err != nil {
				cmdErr = err
			}
			runListenCloser(lc)
		}

		rootCmd = &cobra.Command{Use: rootCmdUse, Short: rootCmdDesc, Long: rootCmdDesc}
	)

	rootCmd.AddCommand(&cobra.Command{
		Use:   apiCmdUse,
		Short: apiCmdDescription,
		Long:  apiCmdDescription,
		Run: func(_ *cobra.Command, args []string) {
			config, err := cfg.NewAPIConfig(append(args, "")[0])
			if err != nil {
				cmdErr = err
				return
			}
			lc, err := api.NewServer(config)
			runLCCmd(lc, err)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   streamConsumerCmdUse,
		Short: streamConsumerCmdDesc,
		Long:  streamConsumerCmdDesc,
		Run: func(_ *cobra.Command, args []string) {
			lc, err := getStreamProcessor(stream.NewConsumer, args)
			runLCCmd(lc, err)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   streamProducerCmdUse,
		Short: streamProducerCmdDesc,
		Long:  streamProducerCmdDesc,
		Run: func(_ *cobra.Command, args []string) {
			lc, err := getStreamProcessor(stream.NewProducer, args)
			runLCCmd(lc, err)
		},
	})

	if err := rootCmd.Execute(); err != nil {
		return err
	}
	return cmdErr
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

// getStreamProcessor gets a StreamProcessor as a listenCloser based on the cmd
// arguments
func getStreamProcessor(factory streamProcessorFactory, args []string) (listenCloser, error) {
	config, err := cfg.NewClientConfig("", append(args, "")[0])
	if err != nil {
		return nil, err
	}

	return newStreamProcessorManager(config, factory), nil
}
