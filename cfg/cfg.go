/*
Copyright © 2019 AVA Labs <collin@avalabs.org>
*/

package cfg

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Config manages configuration data read from a file or the environment
type Config struct {
	Context string

	ChainID   ids.ID
	DataType  string
	IPCURL    string
	LogDir    string
	NetworkID uint32
	Filter    map[string]interface{}

	Logging logging.Config
	Kafka   kafka.ConfigMap
	Redis   redis.Options
}

// NewConfig returns a *Config populated with data from the given file
func NewConfig(context string, file string) (*Config, error) {
	// Parse config file with viper
	v := viper.New()
	v.SetConfigFile(file)
	v.SetConfigType("json")
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	// Create logging config
	logConf, err := logging.DefaultConfig()
	if err != nil {
		return nil, err
	}
	logConf.Directory = v.GetString("logDirectory")

	// Parse Kafka config into a kafka.ConfigMap
	kafkaConf := kafka.ConfigMap{}
	for k, v := range v.GetStringMap("kafka") {
		kafkaConf[k] = v
	}

	// Parse chainID string
	chainID, err := ids.FromString(v.GetString("chainID"))
	if err != nil {
		return nil, err
	}

	// Collect config data into a Config object
	redisConf := v.Sub("redis")
	return &Config{
		Context: context,

		ChainID:   chainID,
		DataType:  v.GetString("dataType"),
		IPCURL:    v.GetString("ipcURL"),
		NetworkID: v.GetUint32("networkID"),
		Filter:    v.GetStringMap("filter"),

		Logging: logConf,
		Kafka:   kafkaConf,
		Redis: redis.Options{
			Addr:     redisConf.GetString("addr"),
			Password: redisConf.GetString("password"),
			DB:       redisConf.GetInt("db"),
		},
	}, nil
}
