// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"errors"
	"strings"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/version"
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
	Services          `json:"services"`
	MetricsListenAddr string `json:"metricsListenAddr"`
	AdminListenAddr   string `json:"adminListenAddr"`
	Features          map[string]struct{}
	CchainID          string `json:"cchainId"`
	AvalancheGO       string `json:"avalanchego"`
	NodeInstance      string `json:"nodeInstance"`
	AP5Activation     uint64
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
}

type API struct {
	ListenAddr string `json:"listenAddr"`
}

type DB struct {
	DSN    string `json:"dsn"`
	RODSN  string `json:"rodsn"`
	Driver string `json:"driver"`
}

type Filter struct {
	Min uint32 `json:"min"`
	Max uint32 `json:"max"`
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

	networkID := v.GetUint32(keysNetworkID)
	ap5Activation := version.GetApricotPhase5Time(networkID).Unix()

	// Put it all together
	return &Config{
		NetworkID:         networkID,
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
		},
		CchainID:      v.GetString(keysStreamProducerCchainID),
		AvalancheGO:   v.GetString(keysStreamProducerAvalanchego),
		NodeInstance:  v.GetString(keysStreamProducerNodeInstance),
		AP5Activation: uint64(ap5Activation),
	}, nil
}
