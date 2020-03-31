// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"errors"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

const (
	appName = "ortelius"

	configKeysNetworkID   = "networkID"
	configKeyLogDirectory = "logDirectory"

	configKeysChains       = "chains"
	configKeysChainsID     = "id"
	configKeysChainsAlias  = "alias"
	configKeysChainsVMType = "vmtype"

	configKeysRedis = "redis"
	configKeysDB    = "db"
)

var (
	ErrChainsConfigMustBeStringMap = errors.New("Chain config must a string map")

	defaultCommon = map[string]interface{}{
		configKeysNetworkID:   12345,
		configKeyLogDirectory: "/tmp/ortelius/logs",
		configKeysRedis: map[string]interface{}{
			"addr":     "127.0.0.1:6379",
			"database": 0,
			"password": "",
		},
		configKeysDB: map[string]interface{}{
			"driver": "mysql",
			"dsn":    "root:password@tcp(127.0.0.1:3306)/ortelius_dev",
			"txDB":   false,
		},
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
	}
)

type Common struct {
	NetworkID uint32
	ChainsConfig
	ServiceConfig
}

type ChainConfig struct {
	Alias  string
	VMType string
}

type ChainsConfig map[ids.ID]ChainConfig

type ServiceConfig struct {
	Redis   *redis.Options
	DB      *DBConfig
	Logging logging.Config
}

type DBConfig struct {
	DSN    string
	Driver string
	TXDB   bool
}

func getConfigViper(file string, defaults map[string]interface{}) (*viper.Viper, error) {
	v := viper.NewWithOptions(viper.KeyDelimiter("_"))

	v.SetEnvPrefix(appName)
	v.AutomaticEnv()

	for key, val := range defaultCommon {
		v.SetDefault(key, val)
	}
	for key, val := range defaults {
		v.SetDefault(key, val)
	}

	if file != "" {
		v.SetConfigFile(file)
		v.SetConfigType("json")
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	return v, nil
}

func getSubViper(v *viper.Viper, name string) *viper.Viper {
	if v == nil {
		return nil
	}

	v = v.Sub(name)
	if v == nil {
		return nil
	}

	v.SetEnvPrefix(appName + "_" + name)
	v.AutomaticEnv()
	return v
}

func getCommonConfig(v *viper.Viper) (Common, error) {
	var err error
	c := Common{
		NetworkID:     v.GetUint32(configKeysNetworkID),
		ServiceConfig: getServiceConfig(v),
	}
	c.ChainsConfig, err = getChainsConfig(v)
	if err != nil {
		return c, err
	}

	return c, nil
}

func getLogConfig(v *viper.Viper) logging.Config {
	// We ignore the error because it's related to creating the default directory
	// but we are going to override it anyways
	logConf, _ := logging.DefaultConfig()
	logConf.Directory = v.GetString(configKeyLogDirectory)
	return logConf
}

func getChainsConfig(v *viper.Viper) (ChainsConfig, error) {
	chainsConf := v.GetStringMap(configKeysChains)
	chains := make(ChainsConfig, len(chainsConf))
	for _, chainConf := range chainsConf {
		confMap, ok := chainConf.(map[string]interface{})
		if !ok {
			return nil, ErrChainsConfigMustBeStringMap
		}

		idStr, ok := confMap[configKeysChainsID].(string)
		if !ok {
			return nil, ErrChainsConfigMustBeStringMap
		}

		id, err := ids.FromString(idStr)
		if err != nil {
			return nil, err
		}

		alias, ok := confMap[configKeysChainsAlias].(string)
		if !ok {
			return nil, ErrChainsConfigMustBeStringMap
		}

		vmType, ok := confMap[configKeysChainsVMType].(string)
		if !ok {
			return nil, ErrChainsConfigMustBeStringMap
		}

		chains[id] = ChainConfig{
			Alias:  alias,
			VMType: vmType,
		}
	}
	return chains, nil
}

func getServiceConfig(v *viper.Viper) ServiceConfig {
	return ServiceConfig{
		Redis:   getRedisConfig(getSubViper(v, configKeysRedis)),
		DB:      getDBConfig(getSubViper(v, configKeysDB)),
		Logging: getLogConfig(v),
	}
}

func getRedisConfig(v *viper.Viper) *redis.Options {
	if v == nil {
		return nil
	}
	opts := &redis.Options{}
	opts.Addr = v.GetString("addr")
	opts.Password = v.GetString("password")
	opts.DB = v.GetInt("db")
	return opts
}

func getDBConfig(v *viper.Viper) *DBConfig {
	if v == nil {
		return nil
	}

	return &DBConfig{
		Driver: v.GetString("driver"),
		DSN:    v.GetString("dsn"),
		TXDB:   v.GetBool("txDB"),
	}
}
