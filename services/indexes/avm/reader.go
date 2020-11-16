// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"
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
	dbRunner, err := r.conns.DB().NewSession("list_assets", services.RequestTimeout)
	if err != nil {
		return nil, err
	}

	assets := make([]*models.Asset, 0, 1)
	_, err = p.Apply(dbRunner.
		Select("id", "chain_id", "name", "symbol", "alias", "denomination", "current_supply", "created_at").
		From("avm_assets")).
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
	if err = r.dressAssets(ctx, dbRunner, assets, p.EnableAggregate); err != nil {
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

	assetList, err := r.ListAssets(ctx, p)
	if err != nil {
		return nil, err
	}
	if len(assetList.Assets) > 0 {
		return assetList.Assets[0], nil
	}
	return nil, err
}

func (r *Reader) dressAssets(ctx context.Context, dbRunner dbr.SessionRunner, assets []*models.Asset, aggregate bool) error {
	if len(assets) == 0 {
		return nil
	}

	tnow := time.Now().UTC()
	tnow = tnow.Truncate(1 * time.Minute)

	// Create a list of ids for querying, and a map for accumulating results later
	assetIDs := make([]models.StringID, len(assets))
	for i, asset := range assets {
		assetIDs[i] = asset.ID

		if !aggregate {
			continue
		}

		id, err := ids.FromString(string(asset.ID))
		if err != nil {
			r.conns.Logger().Warn("asset to id convert failed %s", err)
			continue
		}

		asset.Aggregates = make(map[string]*models.Aggregates)

		for intervalName := range params.IntervalNames {
			if intervalName == "all" {
				continue
			}
			aparams := params.AggregateParams{
				ListParams: params.ListParams{
					StartTime: tnow.Add(-1 * params.IntervalNames[intervalName]),
					EndTime:   tnow,
				},
				AssetID:      &id,
				IntervalSize: params.IntervalNames[intervalName],
				Version:      1,
			}
			hm, err := r.avaxReader.Aggregate(ctx, &aparams)
			if err != nil {
				r.conns.Logger().Warn("aggregate query failed %s", err)
				continue
			}
			asset.Aggregates[intervalName] = &hm.Aggregates
		}
	}

	var rows []*struct {
		AssetID     models.StringID `json:"assetID"`
		VariableCap uint8           `json:"variableCap"`
	}

	mintOutputs := [2]models.OutputType{models.OutputTypesSECP2556K1Mint, models.OutputTypesNFTMint}
	_, err := dbRunner.Select("avm_outputs.asset_id", "CASE WHEN count(avm_outputs.asset_id) > 0 THEN 1 ELSE 0 END AS variable_cap").
		From("avm_outputs").
		Where("avm_outputs.output_type IN ? and avm_outputs.asset_id in ?", mintOutputs[:], assetIDs).
		GroupBy("avm_outputs.asset_id").
		Having("count(avm_outputs.asset_id) > 0").
		LoadContext(ctx, &rows)
	if err != nil {
		return err
	}

	assetMap := make(map[models.StringID]uint8)
	for pos, row := range rows {
		assetMap[row.AssetID] = rows[pos].VariableCap
	}

	for _, asset := range assets {
		if variableCap, ok := assetMap[asset.ID]; ok {
			asset.VariableCap = variableCap
		}
	}

	return nil
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
