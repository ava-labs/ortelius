// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/go-redis/redis"
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
	// Parse config file with viper
	v, err := getConfigViper(file)
	if err != nil {
		return nil, err
	}

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
		Redis:   getRedisConfig(v.Sub("redis")),
	}, nil
}
