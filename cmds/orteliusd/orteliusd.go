// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ava-labs/ortelius/export"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"

	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"

	"github.com/ava-labs/ortelius/utils"

	"github.com/ava-labs/ortelius/replay"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/spf13/cobra"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"
	"github.com/ava-labs/ortelius/stream/consumers"

	// Register service plugins
	_ "github.com/ava-labs/ortelius/services/indexes/avm"
	_ "github.com/ava-labs/ortelius/services/indexes/pvm"

	oreliusRpc "github.com/ava-labs/ortelius/rpc"
)

const (
	// rootCmdUse  = "orteliusd [command]\nex: orteliusd api"
	rootCmdUse  = "orteliusd [command]"
	rootCmdDesc = "Daemons for Ortelius."

	apiCmdUse  = "api"
	apiCmdDesc = "Runs the API daemon"

	streamCmdUse  = "stream"
	streamCmdDesc = "Runs stream commands"

	streamReplayCmdUse  = "replay"
	streamReplayCmdDesc = "Runs the replay"

	streamReplayExportCmdUse  = "replayExport"
	streamReplayExportCmdDesc = "Runs the replay export"

	streamProducerCmdUse  = "producer"
	streamProducerCmdDesc = "Runs the stream producer daemon"

	streamIndexerCmdUse  = "indexer"
	streamIndexerCmdDesc = "Runs the stream indexer daemon"

	streamExportCmdUse  = "export"
	streamExportCmdDesc = "Exports the stream to disk"

	envCmdUse  = "env"
	envCmdDesc = "Displays information about the Ortelius environment"

	defaultReplayQueueSize    = int(2000)
	defaultReplayQueueThreads = int(4)
)

func main() {
	if err := execute(); err != nil {
		log.Fatalln("Failed to run:", err.Error())
	}
}

// Execute runs the root command for ortelius
func execute() error {
	rand.Seed(time.Now().UnixNano())

	var (
		runErr             error
		config             = &cfg.Config{}
		serviceControl     = &services.Control{}
		configFile         = func() *string { s := ""; return &s }()
		replayqueuesize    = func() *int { i := defaultReplayQueueSize; return &i }()
		replayqueuethreads = func() *int { i := defaultReplayQueueThreads; return &i }()
		cmd                = &cobra.Command{Use: rootCmdUse, Short: rootCmdDesc, Long: rootCmdDesc,
			PersistentPreRun: func(cmd *cobra.Command, args []string) {
				c, err := cfg.NewFromFile(*configFile)
				if err != nil {
					log.Fatalln("Failed to read config file", *configFile, ":", err.Error())
				}
				alog, err := logging.New(c.Logging)
				if err != nil {
					log.Fatalln("Failed to create log", c.Logging.Directory, ":", err.Error())
				}

				models.SetBech32HRP(c.NetworkID)

				serviceControl.Log = alog
				serviceControl.Services = c.Services
				serviceControl.Persist = services.NewPersist()
				serviceControl.Features = c.Features
				err = serviceControl.Init(c.NetworkID)
				if err != nil {
					log.Fatalln("Failed to create service control", ":", err.Error())
				}

				*config = *c

				if config.MetricsListenAddr != "" {
					sm := http.NewServeMux()
					sm.Handle("/metrics", promhttp.Handler())
					go func() {
						err = http.ListenAndServe(config.MetricsListenAddr, sm)
						if err != nil {
							log.Fatalln("Failed to start metrics listener", err.Error())
						}
					}()
					alog.Info("Starting metrics handler on %s", config.MetricsListenAddr)
				}
				if config.AdminListenAddr != "" {
					rpcServer := rpc.NewServer()
					codec := json2.NewCodec()
					rpcServer.RegisterCodec(codec, "application/json")
					rpcServer.RegisterCodec(codec, "application/json;charset=UTF-8")
					api := oreliusRpc.NewAPI(alog)
					if err := rpcServer.RegisterService(api, "api"); err != nil {
						log.Fatalln("Failed to start admin listener", err.Error())
					}
					sm := http.NewServeMux()
					sm.Handle("/api", rpcServer)
					go func() {
						err = http.ListenAndServe(config.AdminListenAddr, sm)
						if err != nil {
							log.Fatalln("Failed to start metrics listener", err.Error())
						}
					}()
				}
			},
		}
	)

	// Add flags and commands
	cmd.PersistentFlags().StringVarP(configFile, "config", "c", "config.json", "config file")
	cmd.PersistentFlags().IntVarP(replayqueuesize, "replayqueuesize", "", defaultReplayQueueSize, fmt.Sprintf("replay queue size default %d", defaultReplayQueueSize))
	cmd.PersistentFlags().IntVarP(replayqueuethreads, "replayqueuethreads", "", defaultReplayQueueThreads, fmt.Sprintf("replay queue size threads default %d", defaultReplayQueueThreads))

	cmd.AddCommand(
		createReplayExportCmds(serviceControl, config, &runErr, replayqueuesize, replayqueuethreads),
		createReplayCmds(serviceControl, config, &runErr, replayqueuesize, replayqueuethreads),
		createStreamCmds(serviceControl, config, &runErr),
		createAPICmds(serviceControl, config, &runErr),
		createEnvCmds(config, &runErr))

	// Execute the command and return the runErr to the caller
	if err := cmd.Execute(); err != nil {
		return err
	}

	serviceControl.BalanceAccumulatorManager.Close()

	return runErr
}

func createAPICmds(sc *services.Control, config *cfg.Config, runErr *error) *cobra.Command {
	return &cobra.Command{
		Use:   apiCmdUse,
		Short: apiCmdDesc,
		Long:  apiCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			lc, err := api.NewServer(sc, *config)
			if err != nil {
				*runErr = err
				return
			}
			runListenCloser(lc)
		},
	}
}

func createReplayCmds(sc *services.Control, config *cfg.Config, runErr *error, replayqueuesize *int, replayqueuethreads *int) *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   streamReplayCmdUse,
		Short: streamReplayCmdDesc,
		Long:  streamReplayCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			if sc.IsDBPoll {
				replay := replay.NewDB(sc, config, *replayqueuesize, *replayqueuethreads)
				err := replay.Start()
				if err != nil {
					*runErr = err
				}
			} else {
				replay := replay.New(sc, config, *replayqueuesize, *replayqueuethreads)
				err := replay.Start()
				if err != nil {
					*runErr = err
				}
			}
		},
	}

	return replayCmd
}

func createReplayExportCmds(sc *services.Control, config *cfg.Config, runErr *error, replayqueuesize *int, replayqueuethreads *int) *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   streamReplayExportCmdUse,
		Short: streamReplayExportCmdDesc,
		Long:  streamReplayExportCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			replay := export.New(sc, config, *replayqueuesize, *replayqueuethreads)
			err := replay.Start()
			if err != nil {
				*runErr = err
			}
		},
	}

	return replayCmd
}

type ProcessorFactoryControl struct {
	Factory   stream.ProcessorFactory
	Instances int
}
type ListenCloserFactoryControl struct {
	Factory   utils.ListenCloserFactory
	Instances int
}

func createStreamCmds(sc *services.Control, config *cfg.Config, runErr *error) *cobra.Command {
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
		Run: runStreamProcessorManagers(
			false,
			sc,
			config,
			runErr,
			[]ProcessorFactoryControl{
				{Factory: stream.NewConsensusProducerProcessor, Instances: 1},
				{Factory: stream.NewDecisionsProducerProcessor, Instances: 1},
			},
			producerFactories(config),
			nil,
		),
	}, &cobra.Command{
		Use:   streamIndexerCmdUse,
		Short: streamIndexerCmdDesc,
		Long:  streamIndexerCmdDesc,
		Run: runStreamProcessorManagers(
			true,
			sc,
			config,
			runErr,
			[]ProcessorFactoryControl{
				{Factory: consumers.Indexer, Instances: 2},
				{Factory: consumers.IndexerConsensus, Instances: 2},
			},
			indexerFactories(config),
			[]consumers.ConsumerFactory{
				consumers.IndexerConsumer,
			},
		),
	})

	return streamCmd
}

func indexerFactories(_ *cfg.Config) []ListenCloserFactoryControl {
	var factories []ListenCloserFactoryControl
	factories = append(
		factories,
		ListenCloserFactoryControl{Factory: consumers.IndexerCChain(), Instances: 2},
	)
	return factories
}

func producerFactories(_ *cfg.Config) []ListenCloserFactoryControl {
	var factories []ListenCloserFactoryControl
	factories = append(factories, ListenCloserFactoryControl{Factory: stream.NewProducerCChain(), Instances: 1})
	return factories
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

// runListenCloser runs the ListenCloser until signaled to stop
func runListenCloser(lc utils.ListenCloser) {
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
func runStreamProcessorManagers(
	indexer bool,
	sc *services.Control,
	config *cfg.Config,
	runError *error,
	factories []ProcessorFactoryControl,
	listenCloseFactories []ListenCloserFactoryControl,
	consumerFactories []consumers.ConsumerFactory,
) func(_ *cobra.Command, _ []string) {
	return func(_ *cobra.Command, _ []string) {
		if indexer {
			err := sc.BalanceAccumulatorManager.Start()
			if err != nil {
				sc.BalanceAccumulatorManager.Close()
				*runError = err
				return
			}
			err = consumers.Bootstrap(sc, config.NetworkID, config.Chains, consumerFactories)
			if err != nil {
				*runError = err
				return
			}

			// start the accumulator at startup
			sc.BalanceAccumulatorManager.Run(sc)
		}

		wg := &sync.WaitGroup{}

		for _, factory := range factories {
			for instpos := 0; instpos < factory.Instances; instpos++ {
				wg.Add(1)
				go func(factory stream.ProcessorFactory, idx int, maxidx int) {
					defer wg.Done()

					// Create and start processor manager
					pm := stream.NewProcessorManager(sc, *config, factory, idx, maxidx)
					runListenCloser(pm)
				}(factory.Factory, instpos, factory.Instances)
			}
		}

		for _, listenCloseFactory := range listenCloseFactories {
			for instpos := 0; instpos < listenCloseFactory.Instances; instpos++ {
				wg.Add(1)
				go func(listenCloserFactory utils.ListenCloserFactory, idx int, maxidx int) {
					defer wg.Done()

					lc := listenCloserFactory(sc, *config, idx, maxidx)
					runListenCloser(lc)
				}(listenCloseFactory.Factory, instpos, listenCloseFactory.Instances)
			}
		}

		wg.Wait()
	}
}
