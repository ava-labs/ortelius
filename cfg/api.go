// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

const (
	configKeysListenAddr = "listenAddr"
)

// APIConfig manages configuration data for the API app
type APIConfig struct {
	ServiceConfig
	ListenAddr string
}

// NewAPIConfig returns a *APIConfig populated with data from the given file
func NewAPIConfig(file string) (APIConfig, error) {
	// Parse config file with viper and set the defaults
	v, err := getConfigViper(file, map[string]interface{}{
		configKeysListenAddr: ":8080",
	})
	if err != nil {
		return APIConfig{}, err
	}

	serviceConf, err := getServiceConfig(v)
	if err != nil {
		return APIConfig{}, err
	}

	return APIConfig{
		ListenAddr:    v.GetString(configKeysListenAddr),
		ServiceConfig: serviceConf,
	}, nil
}
