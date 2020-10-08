// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"
	"github.com/ava-labs/ortelius/stream/consumers"

	// Register service plugins
	_ "github.com/ava-labs/ortelius/services/indexes/avm"
	_ "github.com/ava-labs/ortelius/services/indexes/pvm"
)

const (
	// rootCmdUse  = "orteliusd [command]\nex: orteliusd api"
	rootCmdUse  = "orteliusd [command]"
	rootCmdDesc = "Daemons for Ortelius."

	apiCmdUse  = "api"
	apiCmdDesc = "Runs the API daemon"

	streamCmdUse  = "stream"
	streamCmdDesc = "Runs stream commands"

	streamProducerCmdUse  = "producer"
	streamProducerCmdDesc = "Runs the stream producer daemon"

	streamIndexerCmdUse  = "indexer"
	streamIndexerCmdDesc = "Runs the stream indexer daemon"

	streamExportCmdUse  = "export"
	streamExportCmdDesc = "Exports the stream to disk"

	envCmdUse  = "env"
	envCmdDesc = "Displays information about the Ortelius environment"
)

// listenCloser listens for messages until it's asked to close
type listenCloser interface {
	Listen() error
	Close() error
}

func main() {
	if err := execute(); err != nil {
		log.Fatalln("Failed to run:", err.Error())
	}
}

// Execute runs the root command for ortelius
func execute() error {
	var (
		runErr     error
		config     = &cfg.Config{}
		configFile = func() *string { s := ""; return &s }()
		cmd        = &cobra.Command{Use: rootCmdUse, Short: rootCmdDesc, Long: rootCmdDesc,
			PersistentPreRun: func(cmd *cobra.Command, args []string) {
				c, err := cfg.NewFromFile(*configFile)
				if err != nil {
					log.Fatalln("Failed to read config file", *configFile, ":", err.Error())
				}
				*config = *c
			},
		}
	)

	// Add flags and commands
	cmd.PersistentFlags().StringVarP(configFile, "config", "c", "config.json", "")
	cmd.AddCommand(
		createStreamCmds(config, &runErr),
		createAPICmds(config, &runErr),
		createEnvCmds(config, &runErr))

	// Execute the command and return the runErr to the caller
	if err := cmd.Execute(); err != nil {
		return err
	}
	return runErr
}

func createAPICmds(config *cfg.Config, runErr *error) *cobra.Command {
	return &cobra.Command{
		Use:   apiCmdUse,
		Short: apiCmdDesc,
		Long:  apiCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			var lc listenCloser
			lc, err := api.NewServer(*config)
			if err != nil {
				*runErr = err
				return
			}
			runListenCloser(lc)
		},
	}
}

func createStreamCmds(config *cfg.Config, runErr *error) *cobra.Command {
	streamCmd := &cobra.Command{
		Use:   streamCmdUse,
		Short: streamCmdDesc,
		Long:  streamCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
			os.Exit(0)
		},
	}

	// Create export sub command command
	exportCmd := &cobra.Command{
		Use:   streamExportCmdUse,
		Short: streamExportCmdDesc,
		Long:  streamExportCmdDesc,
	}
	exportFile := ""
	exportChainID := ""
	exportCmd.Flags().StringVarP(&exportFile, "export-path", "", "/tmp/ortelius_exports", "")
	exportCmd.Flags().StringVarP(&exportChainID, "export-chain-id", "", "11111111111111111111111111111111LpoYY", "")
	exportCmd.Run = func(_ *cobra.Command, _ []string) {
		count, err := consumers.ExportToDisk(config, exportFile, exportChainID)
		if err != nil {
			log.Printf("Error exporting: %s\n", err.Error())
		}
		log.Printf("Export finished. Exported %d records.\n", count)
	}

	// Add sub commands
	streamCmd.AddCommand(exportCmd, &cobra.Command{
		Use:   streamProducerCmdUse,
		Short: streamProducerCmdDesc,
		Long:  streamProducerCmdDesc,
		Run:   runStreamProcessorManagers(config, runErr, stream.NewConsensusProducerProcessor, stream.NewDecisionsProducerProcessor),
	}, &cobra.Command{
		Use:   streamIndexerCmdUse,
		Short: streamIndexerCmdDesc,
		Long:  streamIndexerCmdDesc,
		Run:   runStreamProcessorManagers(config, runErr, consumers.NewIndexerFactory()),
	})

	return streamCmd
}

func createEnvCmds(config *cfg.Config, runErr *error) *cobra.Command {
	return &cobra.Command{
		Use:   envCmdUse,
		Short: envCmdDesc,
		Long:  envCmdDesc,
		Run: func(_ *cobra.Command, _ []string) {
			configBytes, err := json.MarshalIndent(config, "", "    ")
			if err != nil {
				*runErr = err
				return
			}

			fmt.Println(string(configBytes))
		},
	}
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

// runStreamProcessorManagers returns a cobra command that instantiates and runs
// a set of stream process managers
func runStreamProcessorManagers(config *cfg.Config, runErr *error, factories ...stream.ProcessorFactory) func(_ *cobra.Command, _ []string) {
	return func(_ *cobra.Command, _ []string) {
		wg := &sync.WaitGroup{}
		wg.Add(len(factories))

		for _, factory := range factories {
			go func(factory stream.ProcessorFactory) {
				defer wg.Done()

				// Create and start processor manager
				pm, err := stream.NewProcessorManager(*config, factory)
				if err != nil {
					*runErr = err
					return
				}
				runListenCloser(pm)
			}(factory)
		}

		wg.Wait()
	}
}
