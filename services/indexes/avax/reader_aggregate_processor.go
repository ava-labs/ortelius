package avax

import (
	"context"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

type ReaderAggregate struct {
	lock sync.RWMutex

	aggr  map[ids.ID]*models.AggregatesHistogram
	aggrt *time.Time
	aggrl []*models.AssetAggregate

	a1m   *models.AggregatesHistogram
	a1mt  *time.Time
	a1h   *models.AggregatesHistogram
	a1ht  *time.Time
	a24h  *models.AggregatesHistogram
	a24ht *time.Time
	a7d   *models.AggregatesHistogram
	a7dt  *time.Time
	a30d  *models.AggregatesHistogram
	a30dt *time.Time
}

func (r *Reader) CacheAggregatesByAsset() *models.CacheAssetAggregates {
	var res []*models.AssetAggregate
	var tm *time.Time
	r.readerAggregate.lock.RLock()
	res = append(res, r.readerAggregate.aggrl...)
	tm = r.readerAggregate.aggrt
	r.readerAggregate.lock.RUnlock()
	return &models.CacheAssetAggregates{Aggregates: res, Time: tm}
}

func (r *Reader) CacheAggregatesHistory(tag string) *models.CacheAggregatesHistory {
	var res *models.AggregatesHistogram
	var tm *time.Time
	r.readerAggregate.lock.RLock()
	switch tag {
	case "1m":
		res = r.readerAggregate.a1m
		tm = r.readerAggregate.a1mt
	case "1h":
		res = r.readerAggregate.a1h
		tm = r.readerAggregate.a1ht
	case "24h":
		res = r.readerAggregate.a24h
		tm = r.readerAggregate.a24ht
	case "7d":
		res = r.readerAggregate.a7d
		tm = r.readerAggregate.a7dt
	case "30d":
		res = r.readerAggregate.a30d
		tm = r.readerAggregate.a30dt
	}
	r.readerAggregate.lock.RUnlock()
	return &models.CacheAggregatesHistory{Aggregate: res, Time: tm}
}

func (r *Reader) aggregateProcessor() error {
	if !r.sc.IsAggregateCache {
		return nil
	}

	var connectionsaggr *services.Connections

	var connections1m *services.Connections
	var connections1h *services.Connections
	var connections24h *services.Connections
	var connections7d *services.Connections
	var connections30d *services.Connections
	var err error

	closeDBForError := func() {
		if connectionsaggr != nil {
			_ = connectionsaggr.Close()
		}

		if connections1m != nil {
			_ = connections1m.Close()
		}
		if connections1h != nil {
			_ = connections1h.Close()
		}
		if connections24h != nil {
			_ = connections24h.Close()
		}
		if connections7d != nil {
			_ = connections7d.Close()
		}
		if connections30d != nil {
			_ = connections30d.Close()
		}
	}

	connectionsaggr, err = r.sc.DatabaseRO()
	if err != nil {
		closeDBForError()
		return err
	}

	connections1m, err = r.sc.DatabaseRO()
	if err != nil {
		closeDBForError()
		return err
	}
	connections1h, err = r.sc.DatabaseRO()
	if err != nil {
		closeDBForError()
		return err
	}
	connections24h, err = r.sc.DatabaseRO()
	if err != nil {
		closeDBForError()
		return err
	}
	connections7d, err = r.sc.DatabaseRO()
	if err != nil {
		closeDBForError()
		return err
	}
	connections30d, err = r.sc.DatabaseRO()
	if err != nil {
		closeDBForError()
		return err
	}

	go r.aggregateProcessorAssetAggr(connectionsaggr)
	go r.aggregateProcessor1m(connections1h)
	go r.aggregateProcessor1h(connections1h)
	go r.aggregateProcessor24h(connections24h)
	go r.aggregateProcessor7d(connections7d)
	go r.aggregateProcessor30d(connections30d)
	return nil
}

func (r *Reader) aggregateProcessorAssetAggr(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	timeaggr := time.Now().Truncate(time.Minute)

	runAgg := func() {
		ctx := context.Background()

		job := conns.QuietStream().NewJob("aggr-asset-aggr")
		sess := conns.DB().NewSessionForEventReceiver(job)

		var assetsFound []string
		_, err := sess.Select(
			"asset_id",
			"count(distinct(transaction_id)) as tamt",
		).
			From("avm_outputs").
			Where("created_at > ? and asset_id <> ?",
				time.Now().UTC().Add(-24*time.Hour),
				r.sc.GenesisContainer.AvaxAssetID.String(),
			).
			GroupBy("asset_id").
			OrderDesc("tamt").
			LoadContext(ctx, &assetsFound)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}

		assets := append([]string{}, r.sc.GenesisContainer.AvaxAssetID.String())
		assets = append(assets, assetsFound...)

		aggrMap := make(map[ids.ID]*models.AggregatesHistogram)
		aggrList := make([]*models.AssetAggregate, 0, len(assets))
		for _, asset := range assets {
			p := &params.AggregateParams{}
			urlv := url.Values{}
			err = p.ForValues(1, urlv)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
			p.ListParams.EndTime = time.Now().Truncate(time.Minute)
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-24 * time.Hour)
			p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
			id, err := ids.FromString(asset)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
			p.AssetID = &id
			r.sc.Log.Info("aggregate %s %v-%v", id.String(), p.ListParams.StartTime.Format(time.RFC3339), p.ListParams.EndTime.Format(time.RFC3339))
			aggr, err := r.Aggregate(ctx, p, conns)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
			aggrMap[id] = aggr
			aggrList = append(aggrList, &models.AssetAggregate{Aggregate: aggr, Asset: id})
		}
		sort.Slice(aggrList, func(i, j int) bool {
			return aggrList[i].Aggregate.Aggregates.TransactionCount > aggrList[j].Aggregate.Aggregates.TransactionCount
		})
		tnow := time.Now()
		r.readerAggregate.lock.Lock()
		r.readerAggregate.aggrt = &tnow
		r.readerAggregate.aggr = aggrMap
		r.readerAggregate.aggrl = aggrList
		r.readerAggregate.lock.Unlock()
		timeaggr = timeaggr.Add(5 * time.Minute)
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(timeaggr) {
				runAgg()
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) processAggregate(conns *services.Connections, tag string, intervalSize string, deltaTime time.Duration) (*models.AggregatesHistogram, error) {
	ctx := context.Background()
	p := &params.AggregateParams{}
	urlv := url.Values{}
	urlv.Add(params.KeyIntervalSize, intervalSize)
	err := p.ForValues(1, urlv)
	if err != nil {
		r.sc.Log.Warn("Aggregate %v", err)
		return nil, err
	}
	p.ListParams.EndTime = time.Now().Truncate(time.Minute)
	p.ListParams.StartTime = p.ListParams.EndTime.Add(deltaTime)
	p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
	r.sc.Log.Info("aggregate %s interval %s %v->%v", tag, intervalSize, p.ListParams.StartTime.Format(time.RFC3339), p.ListParams.EndTime.Format(time.RFC3339))
	return r.Aggregate(ctx, p, conns)
}

func (r *Reader) aggregateProcessor1m(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	runAgg := func() {
		agg, err := r.processAggregate(conns, "1m", "1s", -time.Minute)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		tnow := time.Now()
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a1mt = &tnow
		r.readerAggregate.a1m = agg
		r.readerAggregate.lock.Unlock()
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			runAgg()
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) aggregateProcessor1h(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time1h := time.Now().Truncate(time.Minute)

	runAgg := func() {
		agg, err := r.processAggregate(conns, "1h", "5m", -time.Hour)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		tnow := time.Now()
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a1ht = &tnow
		r.readerAggregate.a1h = agg
		r.readerAggregate.lock.Unlock()
		time1h = time1h.Add(5 * time.Minute)
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1h) {
				runAgg()
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) aggregateProcessor24h(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time24h := time.Now().Truncate(time.Minute)

	runAgg := func() {
		agg, err := r.processAggregate(conns, "1d", "hour", -(24 * time.Hour))
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		tnow := time.Now()
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a24ht = &tnow
		r.readerAggregate.a24h = agg
		r.readerAggregate.lock.Unlock()
		time24h = time24h.Add(15 * time.Minute)
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time24h) {
				runAgg()
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) aggregateProcessor7d(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time7d := time.Now().Truncate(time.Minute)

	runAgg := func() {
		agg, err := r.processAggregate(conns, "7d", "day", -(7 * 24 * time.Hour))
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		tnow := time.Now()
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a7dt = &tnow
		r.readerAggregate.a7d = agg
		r.readerAggregate.lock.Unlock()
		time7d = time7d.Add(time.Hour)
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time7d) {
				runAgg()
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) aggregateProcessor30d(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time30d := time.Now().Truncate(time.Minute)

	runAgg := func() {
		agg, err := r.processAggregate(conns, "30d", "day", -(30 * 24 * time.Hour))
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		tnow := time.Now()
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a30dt = &tnow
		r.readerAggregate.a30d = agg
		r.readerAggregate.lock.Unlock()
		time30d = time30d.Add(4 * time.Hour)
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time30d) {
				runAgg()
			}
		case <-r.doneCh:
			return
		}
	}
}
