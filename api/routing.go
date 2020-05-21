// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"errors"
	"sync"

	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
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

type RouterFactory func(RouterFactoryParams) error
type RouterFactoryParams struct {
	Router        *web.Router
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
	// Create a root Router that does the work common to all requests and provides
	// chain-agnostic endpoints
	router, err := newRootRouter(conf.Chains)
	if err != nil {
		return nil, err
	}

	// Instantiate a Router for each chain
	for chainID, chainConfig := range conf.Chains {
		// Get the registered Router factory for this VM
		vmRouterFactory, err := routerFactoryForVM(chainConfig.VMType)
		if err != nil {
			return nil, err
		}

		// Create a helper to instantiate a Router at a given path
		createRouterAtPath := func(path string) error {
			return vmRouterFactory.RouterFactory(RouterFactoryParams{
				Router:        router.Subrouter(vmRouterFactory.ctx, "/"+path),
				NetworkID:     conf.NetworkID,
				ChainConfig:   chainConfig,
				ServiceConfig: conf.Services,
			})
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
