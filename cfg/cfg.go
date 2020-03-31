// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

const (
	appName = "ortelius"

	configKeyLogDirectory  = "logDirectory"
	configKeysRedis        = "redis"
	configKeysDB           = "db"
	configKeysChainAliases = "chainAliases"
)

var (
	requiredChainAliases = []string{"x"}

	defaultCommon = map[string]interface{}{
		configKeyLogDirectory: "/tmp/ortelius/logs",
		configKeysRedis: map[string]interface{}{
			"addr":     "127.0.0.1:6379",
			"database": 0,
			"password": "",
		},
		configKeysDB: map[string]interface{}{
			"driver": "mysql",
			"dsn":    "root:password@tcp(127.0.0.1:3306)/ortelius_dev",
		},
		configKeysChainAliases: map[string]interface{}{
			"x": "i8KtK2KwLi1o7WaVBbEKpRLPtAEYayfoptqAFYxfQgrus1g6m",
		},
	}
)

type DBConfig struct {
	DSN    string
	Driver string
}

type ServiceConfig struct {
	ChainAliasConfig
	Redis   *redis.Options
	DB      *DBConfig
	Logging logging.Config
}

type ChainAliasConfig map[string]ids.ID

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

func getLogConf(v *viper.Viper) logging.Config {
	// We ignore the error because it's related to creating the default directory
	// but we are going to override it anyways
	logConf, _ := logging.DefaultConfig()
	logConf.Directory = v.GetString(configKeyLogDirectory)
	return logConf
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
	}
}

func getChainAliasConfig(v *viper.Viper) (conf ChainAliasConfig, err error) {
	if v == nil {
		return nil, nil
	}

	conf = ChainAliasConfig{}
	for _, alias := range requiredChainAliases {
		chainIDStr := v.GetString(alias)
		conf[alias], err = ids.FromString(chainIDStr)
		if err != nil {
			return nil, err
		}
	}
	return conf, nil
}

func getServiceConfig(v *viper.Viper) (ServiceConfig, error) {
	chainAliases, err := getChainAliasConfig(getSubViper(v, configKeysChainAliases))
	if err != nil {
		return ServiceConfig{}, err
	}
	return ServiceConfig{
		Redis:            getRedisConfig(getSubViper(v, configKeysRedis)),
		DB:               getDBConfig(getSubViper(v, configKeysDB)),
		Logging:          getLogConf(v),
		ChainAliasConfig: chainAliases,
	}, nil
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
