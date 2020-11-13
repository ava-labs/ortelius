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

func (r *Reader) ListBlocks(ctx context.Context, params *params.ListBlocksParams) (*models.BlockList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_blocks", services.RequestTimeout)
	if err != nil {
		return nil, err
	}

	blocks := []*models.Block{}

	_, err = params.Apply(dbRunner.
		Select("id", "type", "parent_id", "chain_id", "created_at").
		From("pvm_blocks")).
		LoadContext(ctx, &blocks)

	if err != nil {
		return nil, err
	}
	return &models.BlockList{Blocks: blocks}, nil
}

func (r *Reader) GetBlock(ctx context.Context, id ids.ID) (*models.Block, error) {
	list, err := r.ListBlocks(ctx, &params.ListBlocksParams{ListParams: params.ListParams{ID: &id}})
	if err != nil || len(list.Blocks) == 0 {
		return nil, err
	}
	return list.Blocks[0], nil
}
