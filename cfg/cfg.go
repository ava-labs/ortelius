// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"errors"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"
)

const appName = "ortelius"

var (
	ErrChainsConfigMustBeStringMap = errors.New("Chain config must a string map")
	ErrChainsConfigIDEmpty         = errors.New("Chain config ID is empty")
	ErrChainsConfigVMEmpty         = errors.New("Chain config vm type is empty")
	ErrChainsConfigIDNotString     = errors.New("Chain config ID is not a string")
	ErrChainsConfigAliasNotString  = errors.New("Chain config alias is not a string")
	ErrChainsConfigVMNotString     = errors.New("Chain config vm type is not a string")
)

type Config struct {
	NetworkID         uint32 `json:"networkID"`
	Chains            `json:"chains"`
	Stream            `json:"stream"`
	Services          `json:"services"`
	MetricsListenAddr string `json:"metricsListenAddr"`
	AdminListenAddr   string `json:"adminListenAddr"`
	Features          map[string]struct{}
}

type Chain struct {
	ID     string `json:"id"`
	VMType string `json:"vmType"`
}

type Chains map[string]Chain

type Services struct {
	Logging logging.Config `json:"logging"`
	API     `json:"api"`
	*DB     `json:"db"`
	*Redis  `json:"redis"`
}

type API struct {
	ListenAddr string `json:"listenAddr"`
}

type DB struct {
	DSN    string `json:"dsn"`
	RODSN  string `json:"rodsn"`
	Driver string `json:"driver"`
}

type Redis struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type Stream struct {
	Kafka    `json:"kafka"`
	Producer Producer `json:"producer"`
	Consumer Consumer `json:"consumer"`
	CchainID string   `json:"cchainId"`
}

type Kafka struct {
	Brokers []string `json:"brokers"`
}

type Filter struct {
	Min uint32 `json:"min"`
	Max uint32 `json:"max"`
}

type Producer struct {
	IPCRoot   string `json:"ipcRoot"`
	CChainRPC string `json:"cChainRpc"`
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
	servicesDBViper := newSubViper(servicesViper, keysServicesDB)
	servicesRedisViper := newSubViper(servicesViper, keysServicesRedis)

	streamViper := newSubViper(v, keysStream)
	streamKafkaViper := newSubViper(streamViper, keysStreamKafka)
	streamProducerViper := newSubViper(streamViper, keysStreamProducer)
	streamConsumerViper := newSubViper(streamViper, keysStreamConsumer)

	// Get chains config
	chains, err := newChainsConfig(v)
	if err != nil {
		return nil, err
	}

	// Build logging config
	loggingConf, err := logging.DefaultConfig()
	if err != nil {
		return nil, err
	}
	loggingConf.Directory = v.GetString(keysLogDirectory)

	dbdsn := servicesDBViper.GetString(keysServicesDBDSN)
	dbrodsn := dbdsn
	if servicesDBViper.Get(keysServicesDBRODSN) != nil {
		dbrodsn = servicesDBViper.GetString(keysServicesDBRODSN)
	}

	features := v.GetStringSlice(keysFeatures)
	featuresMap := make(map[string]struct{})
	for _, feature := range features {
		featurec := strings.TrimSpace(strings.ToLower(feature))
		if featurec == "" {
			continue
		}
		featuresMap[featurec] = struct{}{}
	}
	// Put it all together
	return &Config{
		NetworkID:         v.GetUint32(keysNetworkID),
		Features:          featuresMap,
		Chains:            chains,
		MetricsListenAddr: v.GetString(keysServicesMetricsListenAddr),
		AdminListenAddr:   v.GetString(keysServicesAdminListenAddr),
		Services: Services{
			Logging: loggingConf,
			API: API{
				ListenAddr: v.GetString(keysServicesAPIListenAddr),
			},
			DB: &DB{
				Driver: servicesDBViper.GetString(keysServicesDBDriver),
				DSN:    dbdsn,
				RODSN:  dbrodsn,
			},
			Redis: &Redis{
				Addr:     servicesRedisViper.GetString(keysServicesRedisAddr),
				Password: servicesRedisViper.GetString(keysServicesRedisPassword),
				DB:       servicesRedisViper.GetInt(keysServicesRedisDB),
			},
		},
		Stream: Stream{
			CchainID: streamViper.GetString(keysStreamProducerCchainID),
			Kafka: Kafka{
				Brokers: streamKafkaViper.GetStringSlice(keysStreamKafkaBrokers),
			},
			Producer: Producer{
				IPCRoot:   streamProducerViper.GetString(keysStreamProducerIPCRoot),
				CChainRPC: streamProducerViper.GetString(keysStreamProducerCchainRPC),
			},
			Consumer: Consumer{
				StartTime: streamConsumerViper.GetTime(keysStreamConsumerStartTime),
				GroupName: streamConsumerViper.GetString(keysStreamConsumerGroupName),
			},
		},
	}, nil
}
