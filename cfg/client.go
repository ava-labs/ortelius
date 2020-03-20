// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ClientConfig manages configuration data for the client app
type ClientConfig struct {
	Context string

	ChainID   ids.ID
	DataType  string
	IPCURL    string
	NetworkID uint32
	Filter    map[string]interface{}

	Logging logging.Config
	Kafka   kafka.ConfigMap
	Redis   redis.Options
}

// NewClientConfig returns a *ClientConfig populated with data from the given file
func NewClientConfig(context string, file string) (*ClientConfig, error) {
	// Parse config file with viper and set defaults
	v, err := getConfigViper(file)
	if err != nil {
		return nil, err
	}
	setClientDefaults(v)

	// Parse chainID string
	chainID, err := ids.FromString(v.GetString("chainID"))
	if err != nil {
		return nil, err
	}

	// Collect config data into a ClientConfig object
	return &ClientConfig{
		Context: context,

		ChainID:   chainID,
		DataType:  v.GetString("dataType"),
		IPCURL:    v.GetString("ipcURL"),
		NetworkID: v.GetUint32("networkID"),
		Filter:    v.GetStringMap("filter"),

		Kafka: getKafkaConf(v.GetStringMap("kafka")),

		Logging: getLogConf(v.GetString("logDirectory")),
		Redis:   getRedisConfig(v),
	}, nil
}

func setClientDefaults(v *viper.Viper) {
	v.SetDefault("ipcURL", "ipc:///tmp/GJABrZ9A6UQFpwjPU8MDxDd8vuyRoDVeDAXc694wJ5t3zEkhU.ipc")
	v.SetDefault("chainID", "GJABrZ9A6UQFpwjPU8MDxDd8vuyRoDVeDAXc694wJ5t3zEkhU")
	v.SetDefault("dataType", "avm")
	v.SetDefault("networkID", 12345)
	v.SetDefault("logDirectory", "/var/log/ortelius")
	v.SetDefault("filter", map[string]interface{}{
		"max": 1073741824,
		"min": 2147483648,
	})
	v.SetDefault("kafka", map[string]interface{}{
		"client.id":          "avm",
		"enable.idempotence": true,
		"bootstrap.servers":  "kafka:9092",
	})
	v.SetDefault("redis", defaultRedisConf)
}
