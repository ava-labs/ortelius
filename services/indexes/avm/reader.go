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
	dbRunner, err := r.conns.DB().NewSession("list_assets", cfg.RequestTimeout)
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

	// Add all the addition information we might want
	if err = r.dressAssets(ctx, dbRunner, assets, p); err != nil {
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

func (r *Reader) dressAssets(ctx context.Context, dbRunner dbr.SessionRunner, assets []*models.Asset, p *params.ListAssetsParams) error {
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
			return err
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
				return err
			}
			asset.Aggregates[intervalName] = &hm.Aggregates
		}
	}

	var rows []*struct {
		AssetID    models.StringID   `json:"assetID"`
		OutputType models.OutputType `json:"outputType"`
		Cnt        uint8             `json:"cnt"`
	}

	mintOutputs := [3]models.OutputType{models.OutputTypesSECP2556K1Mint, models.OutputTypesNFTMint, models.OutputTypesNFTTransfer}
	_, err := dbRunner.Select("avm_outputs.asset_id", "avm_outputs.output_type", "case when count(*) > 0 then 1 else 0 end as cnt").
		From("avm_outputs").
		Where("avm_outputs.output_type IN ? and avm_outputs.asset_id in ?", mintOutputs[:], assetIDs).
		GroupBy("avm_outputs.asset_id", "avm_outputs.output_type").
		LoadContext(ctx, &rows)
	if err != nil {
		return err
	}

	assetMapVariableCap := make(map[models.StringID]uint64)
	assetMapNFT := make(map[models.StringID]uint64)
	for _, row := range rows {
		switch row.OutputType {
		case models.OutputTypesSECP2556K1Mint:
			assetMapVariableCap[row.AssetID] = 1
		case models.OutputTypesNFTMint:
			assetMapVariableCap[row.AssetID] = 1
			assetMapNFT[row.AssetID] = 1
		case models.OutputTypesNFTTransfer:
			assetMapNFT[row.AssetID] = 1
		}
	}

	for _, asset := range assets {
		if variableCap, ok := assetMapVariableCap[asset.ID]; ok {
			if variableCap != 0 {
				asset.VariableCap = 1
			}
		}
		if nft, ok := assetMapNFT[asset.ID]; ok {
			if nft != 0 {
				asset.Nft = 1
			}
		}
	}

	return nil
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
