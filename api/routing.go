// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"errors"
	"sync"

	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

var (
	ErrUnregisteredVM = errors.New("no Router is registered for this VM")

	routerFactoriesMu = sync.Mutex{}
	routerFactories   = map[string]registeredRouter{}
)

type registeredRouter struct {
	RouterFactory
	ctx interface{}
}

type RouterFactory func(RouterParams) error
type RouterParams struct {
	Router      *web.Router
	Connections *services.Connections

	NetworkID     uint32
	ChainConfig   cfg.Chain
	ServiceConfig cfg.Services
}

func RegisterRouter(name string, factory RouterFactory, ctx interface{}) {
	routerFactoriesMu.Lock()
	defer routerFactoriesMu.Unlock()
	routerFactories[name] = registeredRouter{factory, ctx}
}

func routerFactoryForVM(name string) (*registeredRouter, error) {
	routerFactoriesMu.Lock()
	defer routerFactoriesMu.Unlock()

	if f, ok := routerFactories[name]; ok {
		return &f, nil
	}
	return nil, ErrUnregisteredVM
}

func newRouter(conf cfg.Config) (*web.Router, error) {
	connections, err := services.NewConnectionsFromConfig(conf.Services)
	if err != nil {
		return nil, err
	}

	baseParams := RouterParams{
		Connections:   connections,
		ServiceConfig: conf.Services,
		NetworkID:     conf.NetworkID,
	}

	// Create a root Router that does the work common to all requests and provides
	// chain-agnostic endpoints
	router, err := newRootRouter(baseParams, conf.Chains)
	if err != nil {
		return nil, err
	}

	// Instantiate a Router for each chain
	for chainID, chainConfig := range conf.Chains {
		// Copy params and set specifics for this chain
		params := baseParams
		params.ChainConfig = chainConfig

		// Get the registered Router factory for this VM
		vmRouterFactory, err := routerFactoryForVM(chainConfig.VMType)
		if err != nil {
			return nil, err
		}

		// Create a helper to instantiate a Router at a given path
		createRouterAtPath := func(path string) error {
			params.Router = router.Subrouter(vmRouterFactory.ctx, "/"+path)
			return vmRouterFactory.RouterFactory(params)
		}

		// Create a Router for the chainID and one for the alias if an alias exists
		if err = createRouterAtPath(chainID); err != nil {
			return nil, err
		}
		if chainConfig.Alias != "" {
			if err = createRouterAtPath(chainConfig.Alias); err != nil {
				return nil, err
			}
		}
	}

	return router, nil
}
