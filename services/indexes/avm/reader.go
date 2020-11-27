// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

type Reader struct {
	conns      *services.Connections
	avaxReader *avax.Reader
}

func NewReader(conns *services.Connections) *Reader {
	return &Reader{
		conns:      conns,
		avaxReader: avax.NewReader(conns),
	}
}

func (r *Reader) ListAssets(ctx context.Context, p *params.ListAssetsParams) (*models.AssetList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_assets", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	assets := make([]*models.Asset, 0, 1)
	_, err = p.Apply(dbRunner.
		Select("id", "chain_id", "name", "symbol", "alias", "denomination", "current_supply", "created_at",
			"case when assets_variablecap.id is null then 0 else 1 end as variablecap").
		From("avm_assets")).
		LeftJoin("assets_variablecap", "avm_assets.id = assets_variablecap").
		LoadContext(ctx, &assets)
	if err != nil {
		return nil, err
	}

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(assets)))
		if len(assets) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			err := p.Apply(dbRunner.
				Select("COUNT(avm_assets.id)").
				From("avm_assets")).
				LoadOneContext(ctx, &count)
			if err != nil {
				return nil, err
			}
		}
	}

	// Add all the addition information we might want
	if err = r.dressAssets(ctx, assets, p); err != nil {
		return nil, err
	}

	return &models.AssetList{ListMetadata: models.ListMetadata{Count: count}, Assets: assets}, nil
}

func (r *Reader) GetAsset(ctx context.Context, p *params.ListAssetsParams, idStrOrAlias string) (*models.Asset, error) {
	id, err := ids.FromString(idStrOrAlias)
	if err == nil {
		p.ListParams.ID = &id
	} else {
		p.Alias = idStrOrAlias
	}
	p.ListParams.DisableCounting = true

	assetList, err := r.ListAssets(ctx, p)
	if err != nil {
		return nil, err
	}
	if len(assetList.Assets) > 0 {
		return assetList.Assets[0], nil
	}
	return nil, err
}

func (r *Reader) dressAssets(ctx context.Context, assets []*models.Asset, p *params.ListAssetsParams) error {
	if len(assets) == 0 {
		return nil
	}

	tnow := time.Now().UTC()
	tnow = tnow.Truncate(1 * time.Minute)

	// Create a list of ids for querying, and a map for accumulating results later
	assetIDs := make([]models.StringID, len(assets))
	for i, asset := range assets {
		assetIDs[i] = asset.ID

		if len(p.EnableAggregate) == 0 {
			continue
		}

		id, err := ids.FromString(string(asset.ID))
		if err != nil {
			r.conns.Logger().Warn("asset to id convert failed %s", err)
			continue
		}

		asset.Aggregates = make(map[string]*models.Aggregates)

		for _, intervalName := range p.EnableAggregate {
			aparams := params.AggregateParams{
				ListParams:   p.ListParams,
				AssetID:      &id,
				IntervalSize: params.IntervalNames[intervalName],
				Version:      1,
			}
			hm, err := r.avaxReader.Aggregate(ctx, &aparams)
			if err != nil {
				r.conns.Logger().Warn("aggregate query failed %s", err)
				return err
			}
			asset.Aggregates[intervalName] = &hm.Aggregates
		}
	}

	return nil
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
