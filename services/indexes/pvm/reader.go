// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

type Reader struct {
	conns *services.Connections
}

func NewReader(conns *services.Connections) *Reader {
	return &Reader{conns: conns}
}

func (r *Reader) ListBlocks(ctx context.Context, params params.ListBlocksParams) (*models.BlockList, error) {
	blocks := []*models.Block{}

	_, err := params.Apply(r.conns.DB().NewSession("list_blocks").
		Select("id", "type", "parent_id", "chain_id", "created_at").
		From("pvm_blocks")).
		LoadContext(ctx, &blocks)

	if err != nil {
		return nil, err
	}
	return &models.BlockList{Blocks: blocks}, nil
}

func (r *Reader) ListSubnets(ctx context.Context, params params.ListSubnetsParams) (*models.SubnetList, error) {
	subnets := []*models.Subnet{}
	_, err := params.Apply(r.conns.DB().NewSession("list_subnets").
		Select("id", "network_id", "threshold", "created_at").
		From("pvm_subnets")).
		LoadContext(ctx, &subnets)
	if err != nil {
		return nil, err
	}

	if err = r.loadControlKeys(ctx, subnets); err != nil {
		return nil, err
	}

	return &models.SubnetList{Subnets: subnets}, nil
}

func (r *Reader) ListValidators(ctx context.Context, params params.ListValidatorsParams) (*models.ValidatorList, error) {
	validators := []*models.Validator{}

	_, err := params.Apply(r.conns.DB().NewSession("list_blocks").
		Select("transaction_id", "node_id", "weight", "start_time", "end_time", "destination", "shares", "subnet_id").
		From("pvm_validators")).
		LoadContext(ctx, &validators)

	if err != nil {
		return nil, err
	}
	return &models.ValidatorList{Validators: validators}, nil
}

func (r *Reader) ListChains(ctx context.Context, params params.ListChainsParams) (*models.ChainList, error) {
	chains := []*models.Chain{}

	_, err := params.Apply(r.conns.DB().NewSession("list_chains").
		Select("id", "network_id", "subnet_id", "name", "vm_id", "genesis_data", "created_at").
		From("pvm_chains")).
		LoadContext(ctx, &chains)
	if err != nil {
		return nil, err
	}

	if err = r.loadFXIDs(ctx, chains); err != nil {
		return nil, err
	}
	if err = r.loadControlSignatures(ctx, chains); err != nil {
		return nil, err
	}

	return &models.ChainList{Chains: chains}, nil
}

func (r *Reader) GetBlock(ctx context.Context, id ids.ID) (*models.Block, error) {
	list, err := r.ListBlocks(ctx, params.ListBlocksParams{ID: &id})
	if err != nil || len(list.Blocks) == 0 {
		return nil, err
	}
	return list.Blocks[0], nil
}

func (r *Reader) GetSubnet(ctx context.Context, id ids.ID) (*models.Subnet, error) {
	list, err := r.ListSubnets(ctx, params.ListSubnetsParams{ID: &id})
	if err != nil || len(list.Subnets) == 0 {
		return nil, err
	}
	return list.Subnets[0], nil
}

func (r *Reader) GetChain(ctx context.Context, id ids.ID) (*models.Chain, error) {
	list, err := r.ListChains(ctx, params.ListChainsParams{ID: &id})
	if err != nil || len(list.Chains) == 0 {
		return nil, err
	}
	return list.Chains[0], nil
}

func (r *Reader) GetValidator(ctx context.Context, id ids.ID) (*models.Validator, error) {
	list, err := r.ListValidators(ctx, params.ListValidatorsParams{ID: &id})
	if err != nil || len(list.Validators) == 0 {
		return nil, err
	}
	return list.Validators[0], nil
}

func (r *Reader) loadControlKeys(ctx context.Context, subnets []*models.Subnet) error {
	if len(subnets) < 1 {
		return nil
	}

	subnetMap := make(map[models.StringID]*models.Subnet, len(subnets))
	ids := make([]models.StringID, len(subnets))
	for i, s := range subnets {
		ids[i] = s.ID
		subnetMap[s.ID] = s
		s.ControlKeys = []models.ControlKey{}
	}

	keys := []struct {
		SubnetID models.StringID
		Key      models.ControlKey
	}{}
	_, err := r.conns.DB().NewSession("load_control_keys").
		Select("subnet_id", "address", "public_key").
		From("pvm_subnet_control_keys").
		Where("pvm_subnet_control_keys.subnet_id IN ?", ids).
		LoadContext(ctx, &keys)
	if err != nil {
		return err
	}
	for _, key := range keys {
		s, ok := subnetMap[key.SubnetID]
		if ok {
			s.ControlKeys = append(s.ControlKeys, key.Key)
		}
	}

	return nil
}

func (r *Reader) loadControlSignatures(ctx context.Context, chains []*models.Chain) error {
	if len(chains) < 1 {
		return nil
	}

	chainMap := make(map[models.StringID]*models.Chain, len(chains))
	ids := make([]models.StringID, len(chains))
	for i, c := range chains {
		ids[i] = c.ID
		chainMap[c.ID] = c
		c.ControlSignatures = []models.ControlSignature{}
	}

	sigs := []struct {
		ChainID   models.StringID
		Signature models.ControlSignature
	}{}
	_, err := r.conns.DB().NewSession("load_control_signatures").
		Select("chain_id", "signature").
		From("pvm_chains_control_signatures").
		Where("pvm_chains_control_signatures.chain_id IN ?", ids).
		LoadContext(ctx, &sigs)
	if err != nil {
		return err
	}
	for _, sig := range sigs {
		s, ok := chainMap[sig.ChainID]
		if ok {
			s.ControlSignatures = append(s.ControlSignatures, sig.Signature)
		}
	}

	return nil
}

func (r *Reader) loadFXIDs(ctx context.Context, chains []*models.Chain) error {
	if len(chains) < 1 {
		return nil
	}

	chainMap := make(map[models.StringID]*models.Chain, len(chains))
	ids := make([]models.StringID, len(chains))
	for i, c := range chains {
		ids[i] = c.ID
		chainMap[c.ID] = c
		c.FxIDs = []models.StringID{}
	}

	fxIDs := []struct {
		ChainID models.StringID
		FXID    models.StringID `r:"fx_id"`
	}{}
	_, err := r.conns.DB().NewSession("load_control_signatures").
		Select("chain_id", "fx_id").
		From("pvm_chains_fx_ids").
		Where("pvm_chains_fx_ids.chain_id IN ?", ids).
		LoadContext(ctx, &fxIDs)
	if err != nil {
		return err
	}
	for _, fxID := range fxIDs {
		s, ok := chainMap[fxID.ChainID]
		if ok {
			s.FxIDs = append(s.FxIDs, fxID.FXID)
		}
	}

	return nil
}
