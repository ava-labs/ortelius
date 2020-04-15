// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"errors"

	"github.com/ava-labs/gecko/ids"
	"github.com/spf13/viper"
)

const (
	configKeysNetworkID = "networkID"
	configKeysIPCRoot   = "ipcRoot"

	configKeysFilter    = "filter"
	configKeysFilterMin = "min"
	configKeysFilterMax = "max"

	configKeysChains       = "chains"
	configKeysChainsID     = "id"
	configKeysChainsAlias  = "alias"
	configKeysChainsVMType = "vmType"

	configKeysKafka          = "kafka"
	configKeysKafkaBrokers   = "brokers"
	configKeysKafkaGroupName = "GroupName"
)

type ChainConfig struct {
	Alias  string
	VMType string
}

type KafkaConfig struct {
	Brokers   []string
	GroupName string
}

type FilterConfig struct {
	Min uint32
	Max uint32
}

// ClientConfig manages configuration data for the client app
type ClientConfig struct {
	Context string

	NetworkID uint32
	IPCRoot   string

	Chains map[ids.ID]ChainConfig

	ServiceConfig
	KafkaConfig
	FilterConfig
}

// NewClientConfig returns a *ClientConfig populated with data from the given file
func NewClientConfig(context string, file string) (*ClientConfig, error) {
	// Parse config file with viper and set defaults
	v, err := getConfigViper(file, map[string]interface{}{
		configKeysNetworkID:    12345,
		configKeysKafkaBrokers: "127.0.0.1:9092",
		configKeysChains: map[string]map[string]interface{}{
			"4R5p2RXDGLqaifZE4hHWH9owe34pfoBULn1DrQTWivjg8o4aH": {
				configKeysChainsID:     "4R5p2RXDGLqaifZE4hHWH9owe34pfoBULn1DrQTWivjg8o4aH",
				configKeysChainsAlias:  "X",
				configKeysChainsVMType: "avm",
			},
			"11111111111111111111111111111111LpoYY": {
				configKeysChainsID:     "11111111111111111111111111111111LpoYY",
				configKeysChainsAlias:  "P",
				configKeysChainsVMType: "pvm",
			},
		},
		configKeysFilter: map[string]interface{}{
			"max": 1073741824,
			"min": 2147483648,
		},
	})
	if err != nil {
		return nil, err
	}

	// Parse chains config
	chainsConf := v.GetStringMap(configKeysChains)
	chains := make(map[ids.ID]ChainConfig, len(chainsConf))
	for _, chainConf := range chainsConf {
		confMap, ok := chainConf.(map[string]interface{})
		if !ok {
			return nil, errors.New("Chain config must a string map")
		}

		idStr, ok := confMap[configKeysChainsID].(string)
		if !ok {
			return nil, errors.New("Chain config must a string map")
		}

		id := ids.Empty
		if idStr != "11111111111111111111111111111111LpoYY" {
			id, err = ids.FromString(idStr)
			if err != nil {
				return nil, err
			}
		}

		alias, ok := confMap[configKeysChainsAlias].(string)
		if !ok {
			return nil, errors.New("Chain config must a string map")
		}

		vmType, ok := confMap[configKeysChainsAlias].(string)
		if !ok {
			return nil, errors.New("Chain config must a string map")
		}

		chains[id] = ChainConfig{
			Alias:  alias,
			VMType: vmType,
		}
	}

	// Parse services config
	serviceConf, err := getServiceConfig(v)
	if err != nil {
		return nil, err
	}

	// Collect config data into a ClientConfig object
	return &ClientConfig{
		Context:       context,
		Chains:        chains,
		ServiceConfig: serviceConf,
		NetworkID:     v.GetUint32(configKeysNetworkID),
		IPCRoot:       v.GetString(configKeysIPCRoot),
		KafkaConfig:   getKafkaConf(getSubViper(v, configKeysKafka)),
		FilterConfig:  getFilterConf(getSubViper(v, configKeysFilter)),
	}, nil
}

func getFilterConf(v *viper.Viper) FilterConfig {
	return FilterConfig{
		Min: v.GetUint32(configKeysFilterMin),
		Max: v.GetUint32(configKeysFilterMax),
	}
}

func getKafkaConf(v *viper.Viper) KafkaConfig {
	return KafkaConfig{
		Brokers:   v.GetStringSlice(configKeysKafkaBrokers),
		GroupName: v.GetString(configKeysKafkaGroupName),
	}
}
