// (c) 2020, Ava Labs, Inc. All rights reserved.
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

	keysServicesRedis         = "redis"
	keysServicesRedisAddr     = "addr"
	keysServicesRedisPassword = "password"
	keysServicesRedisDB       = "db"

	keysStream = "stream"

	keysStreamKafka        = "kafka"
	keysStreamKafkaBrokers = "brokers"

	keysStreamProducer        = "producer"
	keysStreamProducerIPCRoot = "ipcRoot"

	keysStreamProducerCchainRPC = "cchainRpc"
	keysStreamProducerCchainID  = "cchainID"

	keysStreamConsumer          = "consumer"
	keysStreamConsumerStartTime = "startTime"
)
