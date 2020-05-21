// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"github.com/ava-labs/gecko/vms/platformvm"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/models"
)

func init() {
	api.RegisterRouter(VMName, NewAPIRouter, APIContext{})
}

type Index struct {
	networkID uint32
	chainID   string
	db        *DB
}

func New(conf cfg.Services, networkID uint32, chainID string) (*Index, error) {
	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		return nil, err
	}
	return newForConnections(conns, networkID, chainID), nil
}

func newForConnections(conns *services.Connections, networkID uint32, chainID string) *Index {
	db := NewDBIndex(conns.Stream(), conns.DB(), networkID, chainID, platformvm.Codec)
	return &Index{networkID, chainID, db}
}

func (i *Index) Name() string { return "pvm-index" }

func (i *Index) GetChainInfo(alias string, networkID uint32) (*models.ChainInfo, error) {
	return &models.ChainInfo{
		ID:        models.StringID(i.chainID),
		Alias:     alias,
		NetworkID: networkID,
		VM:        VMName,
	}, nil
}

func (i *Index) Consume(ingestable services.Consumable) error         { return i.db.Consume(ingestable) }
func (i *Index) Bootstrap() error                                     { return i.db.Bootstrap() }
func (i *Index) ListBlocks(p ListBlocksParams) (*BlockList, error)    { return i.db.ListBlocks(p) }
func (i *Index) ListSubnets(p ListSubnetsParams) (*SubnetList, error) { return i.db.ListSubnets(p) }
func (i *Index) ListChains(p ListChainsParams) (*ChainList, error)    { return i.db.ListChains(p) }
func (i *Index) ListValidators(p ListValidatorsParams) (*ValidatorList, error) {
	return i.db.ListValidators(p)
}
