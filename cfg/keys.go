// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

const (
	keysNetworkID    = "networkID"
	keysLogDirectory = "logDirectory"

	keysChains       = "chains"
	keysChainsID     = "id"
	keysChainsAlias  = "alias"
	keysChainsVMType = "vmtype"

	keysServices = "services"

	keysServicesAPI           = "api"
	keysServicesAPIListenAddr = "listenAddr"

	keysServicesDB       = "db"
	keysServicesDBDriver = "driver"
	keysServicesDBDSN    = "dsn"
	keysServicesDBTXDB   = "txDB"

	keysServicesRedis         = "redis"
	keysServicesRedisAddr     = "addr"
	keysServicesRedisPassword = "password"
	keysServicesRedisDB       = "db"

	keysStream = "stream"

	keysStreamKafka        = "kafka"
	keysStreamKafkaBrokers = "brokers"

	keysStreamProducer        = "producer"
	keysStreamProducerIPCRoot = "ipcRoot"

	keysStreamConsumer          = "consumer"
	keysStreamConsumerGroupName = "groupName"
	keysStreamConsumerStartTime = "startTime"

	keysStreamFilter    = "filter"
	keysStreamFilterMin = "min"
	keysStreamFilterMax = "max"
)
