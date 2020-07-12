// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"

	"github.com/ava-labs/ortelius/services/models"
)

func (db *DB) ListBlocks(ctx context.Context, params ListBlocksParams) (*BlockList, error) {
	blocks := []*Block{}

	_, err := params.Apply(db.newSession("list_blocks").
		Select("id", "type", "parent_id", "chain_id", "created_at").
		From("pvm_blocks")).
		LoadContext(ctx, &blocks)

	if err != nil {
		return nil, err
	}
	return &BlockList{Blocks: blocks}, nil
}

func (db *DB) ListSubnets(ctx context.Context, params ListSubnetsParams) (*SubnetList, error) {
	subnets := []*Subnet{}
	_, err := params.Apply(db.newSession("list_subnets").
		Select("id", "network_id", "threshold", "created_at").
		From("pvm_subnets")).
		LoadContext(ctx, &subnets)
	if err != nil {
		return nil, err
	}

	if err = db.loadControlKeys(ctx, subnets); err != nil {
		return nil, err
	}

	return &SubnetList{Subnets: subnets}, nil
}

func (db *DB) ListValidators(ctx context.Context, params ListValidatorsParams) (*ValidatorList, error) {
	validators := []*Validator{}

	_, err := params.Apply(db.newSession("list_blocks").
		Select("transaction_id", "node_id", "weight", "start_time", "end_time", "destination", "shares", "subnet_id").
		From("pvm_validators")).
		LoadContext(ctx, &validators)

	if err != nil {
		return nil, err
	}
	return &ValidatorList{Validators: validators}, nil
}

func (db *DB) ListChains(ctx context.Context, params ListChainsParams) (*ChainList, error) {
	chains := []*Chain{}

	_, err := params.Apply(db.newSession("list_chains").
		Select("id", "network_id", "subnet_id", "name", "vm_id", "genesis_data", "created_at").
		From("pvm_chains")).
		LoadContext(ctx, &chains)
	if err != nil {
		return nil, err
	}

	if err = db.loadFXIDs(ctx, chains); err != nil {
		return nil, err
	}
	if err = db.loadControlSignatures(ctx, chains); err != nil {
		return nil, err
	}

	return &ChainList{Chains: chains}, nil
}

func (db *DB) loadControlKeys(ctx context.Context, subnets []*Subnet) error {
	if len(subnets) < 1 {
		return nil
	}

	subnetMap := make(map[models.StringID]*Subnet, len(subnets))
	ids := make([]models.StringID, len(subnets))
	for i, s := range subnets {
		ids[i] = s.ID
		subnetMap[s.ID] = s
		s.ControlKeys = []ControlKey{}
	}

	keys := []struct {
		SubnetID models.StringID
		Key      ControlKey
	}{}
	_, err := db.newSession("load_control_keys").
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

func (db *DB) loadControlSignatures(ctx context.Context, chains []*Chain) error {
	if len(chains) < 1 {
		return nil
	}

	chainMap := make(map[models.StringID]*Chain, len(chains))
	ids := make([]models.StringID, len(chains))
	for i, c := range chains {
		ids[i] = c.ID
		chainMap[c.ID] = c
		c.ControlSignatures = []ControlSignature{}
	}

	sigs := []struct {
		ChainID   models.StringID
		Signature ControlSignature
	}{}
	_, err := db.newSession("load_control_signatures").
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

func (db *DB) loadFXIDs(ctx context.Context, chains []*Chain) error {
	if len(chains) < 1 {
		return nil
	}

	chainMap := make(map[models.StringID]*Chain, len(chains))
	ids := make([]models.StringID, len(chains))
	for i, c := range chains {
		ids[i] = c.ID
		chainMap[c.ID] = c
		c.FxIDs = []models.StringID{}
	}

	fxIDs := []struct {
		ChainID models.StringID
		FXID    models.StringID `db:"fx_id"`
	}{}
	_, err := db.newSession("load_control_signatures").
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
