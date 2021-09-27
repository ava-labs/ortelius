// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

const (
	keysNetworkID    = "networkID"
	keysLogDirectory = "logDirectory"
	keysFeatures     = "features"

	keysChains       = "chains"
	keysChainsID     = "id"
	keysChainsVMType = "vmtype"

	keysServices = "services"

	keysServicesAPIListenAddr     = "listenAddr"
	keysServicesAdminListenAddr   = "adminListenAddr"
	keysServicesMetricsListenAddr = "metricsListenAddr"

	keysServicesDB       = "db"
	keysServicesDBDriver = "driver"
	keysServicesDBDSN    = "dsn"
	keysServicesDBRODSN  = "ro_dsn"

	keysStreamProducerAvalanchego  = "avalanchego"
	keysStreamProducerNodeInstance = "nodeInstance"

	keysStreamProducerCchainID = "cchainID"
)
