// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"bytes"
	"log"

	"github.com/spf13/viper"
)

func newViper() *viper.Viper {
	v := viper.NewWithOptions(viper.KeyDelimiter("_"))
	v.SetEnvPrefix(appName)
	v.AutomaticEnv()
	v.SetConfigType("json")

	// Add defaults
	if err := v.ReadConfig(bytes.NewBufferString(defaultJSON)); err != nil {
		log.Printf("Failed to read default config: %s", err.Error())
	}

	return v
}

func newViperFromFile(filePath string) (*viper.Viper, error) {
	// Get default viper
	v := newViper()

	// Read config from file
	v.SetConfigFile(filePath)
	if err := v.MergeInConfig(); err != nil {
		return nil, err
	}

	return v, nil
}

func newSubViper(v *viper.Viper, name string) *viper.Viper {
	if v == nil {
		return newViper()
	}

	v = v.Sub(name)
	if v == nil {
		return newViper()
	}

	v.SetEnvPrefix(appName + "_" + name)
	v.AutomaticEnv()
	return v
}

func newChainsConfig(v *viper.Viper) (Chains, error) {
	chainsConf := v.GetStringMap(keysChains)
	chains := make(Chains, len(chainsConf))
	for _, chainConf := range chainsConf {
		confMap, ok := chainConf.(map[string]interface{})
		if !ok {
			return nil, ErrChainsConfigMustBeStringMap
		}

		// Check for empty values
		switch {
		case confMap[keysChainsID] == nil:
			return nil, ErrChainsConfigIDEmpty
		case confMap[keysChainsVMType] == nil:
			return nil, ErrChainsConfigVMEmpty
		}

		// Convert to proper types
		id, ok := confMap[keysChainsID].(string)
		if !ok {
			return nil, ErrChainsConfigIDNotString
		}

		vmType, ok := confMap[keysChainsVMType].(string)
		if !ok {
			return nil, ErrChainsConfigVMNotString
		}

		chains[id] = Chain{ID: id, VMType: vmType}
	}
	return chains, nil
}
