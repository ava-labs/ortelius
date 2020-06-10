// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"

	"github.com/ava-labs/gecko/ids"
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

func (i *Index) Consume(ctx context.Context, ingestable services.Consumable) error {
	return i.db.Consume(ctx, ingestable)
}
func (i *Index) Bootstrap(ctx context.Context) error { return i.db.Bootstrap(ctx) }
func (i *Index) ListBlocks(ctx context.Context, p ListBlocksParams) (*BlockList, error) {
	return i.db.ListBlocks(ctx, p)
}
func (i *Index) ListSubnets(ctx context.Context, p ListSubnetsParams) (*SubnetList, error) {
	return i.db.ListSubnets(ctx, p)
}
func (i *Index) ListChains(ctx context.Context, p ListChainsParams) (*ChainList, error) {
	return i.db.ListChains(ctx, p)
}
func (i *Index) ListValidators(ctx context.Context, p ListValidatorsParams) (*ValidatorList, error) {
	return i.db.ListValidators(ctx, p)
}

func (i *Index) GetBlock(ctx context.Context, id ids.ID) (*Block, error) {
	list, err := i.db.ListBlocks(ctx, ListBlocksParams{ID: &id})
	if err != nil || len(list.Blocks) == 0 {
		return nil, err
	}
	return list.Blocks[0], nil
}

func (i *Index) GetSubnet(ctx context.Context, id ids.ID) (*Subnet, error) {
	list, err := i.db.ListSubnets(ctx, ListSubnetsParams{ID: &id})
	if err != nil || len(list.Subnets) == 0 {
		return nil, err
	}
	return list.Subnets[0], nil
}

func (i *Index) GetChain(ctx context.Context, id ids.ID) (*Chain, error) {
	list, err := i.db.ListChains(ctx, ListChainsParams{ID: &id})
	if err != nil || len(list.Chains) == 0 {
		return nil, err
	}
	return list.Chains[0], nil
}

func (i *Index) GetValidator(ctx context.Context, id ids.ID) (*Validator, error) {
	list, err := i.ListValidators(ctx, ListValidatorsParams{ID: &id})
	if err != nil || len(list.Validators) == 0 {
		return nil, err
	}
	return list.Validators[0], nil
}
