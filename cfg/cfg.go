// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"errors"
	"time"
)

const appName = "ortelius"

var (
	ErrChainsConfigMustBeStringMap = errors.New("Chain config must a string map")
	ErrChainsConfigIDEmpty         = errors.New("Chain config ID is empty")
	ErrChainsConfigAliasEmpty      = errors.New("Chain config alias is empty")
	ErrChainsConfigVMEmpty         = errors.New("Chain config vm type is empty")
	ErrChainsConfigIDNotString     = errors.New("Chain config ID is not a string")
	ErrChainsConfigAliasNotString  = errors.New("Chain config alias is not a string")
	ErrChainsConfigVMNotString     = errors.New("Chain config vm type is not a string")
)

type Config struct {
	NetworkID    uint32 `json:"networkID"`
	LogDirectory string `json:"logDirectory"`
	Chains       `json:"chains"`
	Stream       `json:"stream"`
	Services     `json:"services"`
}

type Chain struct {
	ID     string `json:"id"`
	Alias  string `json:"alias"`
	VMType string `json:"vmType"`
}

type Chains map[string]Chain

type Services struct {
	API    `json:"api"`
	*DB    `json:"db"`
	*Redis `json:"redis"`
}

type API struct {
	ListenAddr string `json:"listenAddr"`
}

type DB struct {
	DSN    string `json:"dsn"`
	Driver string `json:"driver"`
	TXDB   bool   `json:"txDB"`
}

type Redis struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type Stream struct {
	Kafka    `json:"kafka"`
	Filter   `json:"filter"`
	Producer Producer `json:"producer"`
	Consumer Consumer `json:"consumer"`
}

type Kafka struct {
	Brokers []string `json:"brokers"`
}

type Filter struct {
	Min uint32 `json:"min"`
	Max uint32 `json:"max"`
}

type Producer struct {
	IPCRoot string `json:"ipcRoot"`
}

type Consumer struct {
	StartTime time.Time `json:"startTime"`
	GroupName string    `json:"groupName"`
}

// NewFromFile creates a new *Config with the defaults replaced by the config  in
// the file at the given path
func NewFromFile(filePath string) (*Config, error) {
	v, err := newViperFromFile(filePath)
	if err != nil {
		return nil, err
	}

	// Get sub vipers for all objects with parents
	servicesViper := newSubViper(v, keysServices)
	servicesAPIViper := newSubViper(servicesViper, keysServicesAPI)
	servicesDBViper := newSubViper(servicesViper, keysServicesDB)
	servicesRedisViper := newSubViper(servicesViper, keysServicesRedis)

	streamViper := newSubViper(v, keysStream)
	streamKafkaViper := newSubViper(streamViper, keysStreamKafka)
	streamFilterViper := newSubViper(streamViper, keysStreamFilter)
	streamProducerViper := newSubViper(streamViper, keysStreamProducer)
	streamConsumerViper := newSubViper(streamViper, keysStreamConsumer)

	// Get chains config
	chains, err := newChainsConfig(v)
	if err != nil {
		return nil, err
	}

	// Put it all together
	return &Config{
		NetworkID:    v.GetUint32(keysNetworkID),
		LogDirectory: v.GetString(keysLogDirectory),
		Chains:       chains,
		Services: Services{
			API: API{
				ListenAddr: servicesAPIViper.GetString(keysServicesAPIListenAddr),
			},
			DB: &DB{
				Driver: servicesDBViper.GetString(keysServicesDBDriver),
				DSN:    servicesDBViper.GetString(keysServicesDBDSN),
				TXDB:   servicesDBViper.GetBool(keysServicesDBTXDB),
			},
			Redis: &Redis{
				Addr:     servicesRedisViper.GetString(keysServicesRedisAddr),
				Password: servicesRedisViper.GetString(keysServicesRedisPassword),
				DB:       servicesRedisViper.GetInt(keysServicesRedisDB),
			},
		},
		Stream: Stream{
			Kafka: Kafka{
				Brokers: streamKafkaViper.GetStringSlice(keysStreamKafkaBrokers),
			},
			Filter: Filter{
				Min: streamFilterViper.GetUint32(keysStreamFilterMin),
				Max: streamFilterViper.GetUint32(keysStreamFilterMax),
			},
			Producer: Producer{
				IPCRoot: streamProducerViper.GetString(keysStreamProducerIPCRoot),
			},
			Consumer: Consumer{
				StartTime: streamConsumerViper.GetTime(keysStreamConsumerStartTime),
				GroupName: streamConsumerViper.GetString(keysStreamConsumerGroupName),
			},
		},
	}, nil
}
