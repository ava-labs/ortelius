// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

// APIConfig manages configuration data for the API app
type APIConfig struct {
	ListenAddr string
	Logging    logging.Config
	Redis      redis.Options
}

// NewAPIConfig returns a *APIConfig populated with data from the given file
func NewAPIConfig(file string) (APIConfig, error) {
	// Parse config file with viper and set the defaults
	v, err := getConfigViper(file)
	if err != nil {
		return APIConfig{}, err
	}
	setAPIDefaults(v)

	// Collect config data into a APIConfig object
	return APIConfig{
		ListenAddr: v.GetString("listenAddr"),
		Logging:    getLogConf(v.GetString("logDirectory")),
		Redis:      getRedisConfig(v),
	}, nil
}

func setAPIDefaults(v *viper.Viper) {
	v.SetDefault("listenAddr", ":8080")
	v.SetDefault("logDirectory", defaultLogDirectory)
	v.SetDefault("redis", defaultRedisConf)
}
