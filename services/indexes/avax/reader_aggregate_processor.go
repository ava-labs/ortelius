package avax

import (
	"context"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

type ReaderAggregate struct {
	lock sync.RWMutex

	assetm        map[ids.ID]*models.Asset
	assetl        []*models.Asset
	aggr          map[ids.ID]*models.AggregatesHistogram
	aggrl         []*models.AssetAggregate
	addressCountl []*models.ChainCounts
	txCountl      []*models.ChainCounts

	a1m  *models.AggregatesHistogram
	a1h  *models.AggregatesHistogram
	a24h *models.AggregatesHistogram
	a7d  *models.AggregatesHistogram
	a30d *models.AggregatesHistogram
}

func (r *Reader) CacheAddressCounts() []*models.ChainCounts {
	var res []*models.ChainCounts
	r.readerAggregate.lock.RLock()
	defer r.readerAggregate.lock.RUnlock()
	res = append(res, r.readerAggregate.addressCountl...)
	return res
}

func (r *Reader) CacheTxCounts() []*models.ChainCounts {
	var res []*models.ChainCounts
	r.readerAggregate.lock.RLock()
	defer r.readerAggregate.lock.RUnlock()
	res = append(res, r.readerAggregate.txCountl...)
	return res
}

func (r *Reader) CacheAssets() []*models.Asset {
	var res []*models.Asset
	r.readerAggregate.lock.RLock()
	defer r.readerAggregate.lock.RUnlock()
	res = append(res, r.readerAggregate.assetl...)
	return res
}

func (r *Reader) CacheAssetAggregates() []*models.AssetAggregate {
	var res []*models.AssetAggregate
	r.readerAggregate.lock.RLock()
	defer r.readerAggregate.lock.RUnlock()
	res = append(res, r.readerAggregate.aggrl...)
	return res
}

func (r *Reader) CacheAggregates(tag string) *models.AggregatesHistogram {
	var res *models.AggregatesHistogram
	r.readerAggregate.lock.RLock()
	defer r.readerAggregate.lock.RUnlock()
	switch tag {
	case "1m":
		res = r.readerAggregate.a1m
	case "1h":
		res = r.readerAggregate.a1h
	case "24h":
		res = r.readerAggregate.a24h
	case "7d":
		res = r.readerAggregate.a7d
	case "30d":
		res = r.readerAggregate.a30d
	}
	return res
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
	go r.aggregateProcessor1m(connections1m)
	go r.aggregateProcessor1h(connections1h)
	go r.aggregateProcessor24h(connections24h)
	go r.aggregateProcessor7d(connections7d)
	go r.aggregateProcessor30d(connections30d)
	return nil
}

func (r *Reader) addressCounts(ctx context.Context, sess *dbr.Session) {
	var addressCountl []*models.ChainCounts
	_, err := sess.Select(
		"chain_id",
		"cast(count(*) as char) as total",
	).
		From(services.TableAddressChain).
		GroupBy("chain_id").
		LoadContext(ctx, &addressCountl)
	if err != nil {
		r.sc.Log.Warn("Aggregate address counts %v", err)
		return
	}

	// counts for the chains only..
	var addressCountlpruned []*models.ChainCounts
	for _, aCount := range addressCountl {
		if _, ok := r.sc.Chains[string(aCount.ChainID)]; ok {
			addressCountlpruned = append(addressCountlpruned, aCount)
		}
	}

	r.readerAggregate.lock.Lock()
	r.readerAggregate.addressCountl = addressCountlpruned
	r.readerAggregate.lock.Unlock()
}

func (r *Reader) txCounts(ctx context.Context, sess *dbr.Session) {
	var txCountl []*models.ChainCounts
	_, err := sess.Select(
		"chain_id",
		"cast(count(*) as char) as total",
	).
		From(services.TableTransactions).
		GroupBy("chain_id").
		LoadContext(ctx, &txCountl)
	if err != nil {
		r.sc.Log.Warn("Aggregate tx counts %v", err)
		return
	}

	// counts for the chains only..
	var txCountlpruned []*models.ChainCounts
	for _, aCount := range txCountl {
		if _, ok := r.sc.Chains[string(aCount.ChainID)]; ok {
			txCountlpruned = append(txCountlpruned, aCount)
		}
	}

	r.readerAggregate.lock.Lock()
	r.readerAggregate.txCountl = txCountlpruned
	r.readerAggregate.lock.Unlock()
}

func (r *Reader) aggregateProcessorAssetAggr(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	runDuration := 24 * time.Hour

	ticker := time.NewTicker(time.Second)

	timeaggr := time.Now().Truncate(time.Minute).Truncate(5 * time.Minute)

	runAgg := func(runTm time.Time) {
		ctx := context.Background()

		sess := conns.DB().NewSessionForEventReceiver(conns.QuietStream().NewJob("aggr-asset-aggr"))

		r.addressCounts(ctx, sess)
		r.txCounts(ctx, sess)

		var assetsFound []string
		_, err := sess.Select(
			"asset_id",
			"count(distinct(transaction_id)) as tamt",
		).
			From("avm_outputs").
			Where("created_at > ? and asset_id <> ?",
				runTm.Add(-runDuration),
				r.sc.GenesisContainer.AvaxAssetID.String(),
			).
			GroupBy("asset_id").
			OrderDesc("tamt").
			LoadContext(ctx, &assetsFound)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}

		var addlAssetsFound []string
		_, err = sess.Select(
			"id",
		).
			From("avm_assets").
			OrderDesc("created_at").
			Limit(params.PaginationMaxLimit).
			LoadContext(ctx, &addlAssetsFound)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}

		assets := append([]string{}, r.sc.GenesisContainer.AvaxAssetID.String())
		assets = append(assets, assetsFound...)

		aggrMap := make(map[ids.ID]*models.AggregatesHistogram)
		assetMap := make(map[ids.ID]*models.Asset)
		aggrList := make([]*models.AssetAggregate, 0, len(assets))
		for _, asset := range assets {
			p := &params.AggregateParams{}
			urlv := url.Values{}
			err = p.ForValues(1, urlv)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
			p.ListParams.EndTime = runTm
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-runDuration)
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

			pa := &params.ListAssetsParams{ListParams: params.ListParams{DisableCounting: true, ID: &id}}
			lassets, err := r.ListAssets(ctx, pa, conns)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}

			aggrMap[id] = aggr
			for _, lasset := range lassets.Assets {
				assetMap[id] = lasset
			}
			aggrList = append(aggrList, &models.AssetAggregate{Aggregate: aggr, Asset: id})
		}

		for _, asset := range addlAssetsFound {
			id, err := ids.FromString(asset)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
			_, ok := assetMap[id]
			if ok {
				continue
			}

			pa := &params.ListAssetsParams{ListParams: params.ListParams{DisableCounting: true, ID: &id}}
			lassets, err := r.ListAssets(ctx, pa, conns)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
			for _, lasset := range lassets.Assets {
				assetMap[id] = lasset
			}
		}

		sort.Slice(aggrList, func(i, j int) bool {
			return aggrList[i].Aggregate.Aggregates.TransactionCount > aggrList[j].Aggregate.Aggregates.TransactionCount
		})

		assetl := make([]*models.Asset, 0, len(assetMap))
		for _, assetv := range assetMap {
			assetl = append(assetl, assetv)
		}

		r.readerAggregate.lock.Lock()
		r.readerAggregate.assetm = assetMap
		r.readerAggregate.assetl = assetl
		r.readerAggregate.aggr = aggrMap
		r.readerAggregate.aggrl = aggrList
		r.readerAggregate.lock.Unlock()

		timeaggr = timeaggr.Add(5 * time.Minute).Truncate(5 * time.Minute)
	}
	runAgg(timeaggr)
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(timeaggr) {
				runAgg(timeaggr)
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) processAggregate(conns *services.Connections, runTm time.Time, tag string, intervalSize string, deltaTime time.Duration) (*models.AggregatesHistogram, error) {
	ctx := context.Background()
	p := &params.AggregateParams{}
	urlv := url.Values{}
	urlv.Add(params.KeyIntervalSize, intervalSize)
	err := p.ForValues(1, urlv)
	if err != nil {
		r.sc.Log.Warn("Aggregate %v", err)
		return nil, err
	}
	p.ListParams.EndTime = runTm
	p.ListParams.StartTime = p.ListParams.EndTime.Add(deltaTime)
	p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
	r.sc.Log.Info("aggregate %s interval %s %v->%v", tag, intervalSize, p.ListParams.StartTime.Format(time.RFC3339), p.ListParams.EndTime.Format(time.RFC3339))
	return r.Aggregate(ctx, p, conns)
}

func (r *Reader) aggregateProcessor1m(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(time.Second)

	time1m := time.Now().Truncate(time.Minute)

	runAgg := func(runTm time.Time) {
		agg, err := r.processAggregate(conns, runTm, "1m", "1s", -time.Minute)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a1m = agg
		r.readerAggregate.lock.Unlock()
		time1m = time1m.Add(time.Minute).Truncate(time.Minute)
	}
	runAgg(time1m)
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1m) {
				runAgg(time1m)
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) aggregateProcessor1h(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(time.Second)

	time1h := time.Now().Truncate(time.Minute).Truncate(5 * time.Minute)

	runAgg := func(runtm time.Time) {
		agg, err := r.processAggregate(conns, runtm, "1h", "5m", -time.Hour)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a1h = agg
		r.readerAggregate.lock.Unlock()
		time1h = time1h.Add(5 * time.Minute).Truncate(5 * time.Minute)
	}
	runAgg(time1h)
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1h) {
				runAgg(time1h)
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

	ticker := time.NewTicker(time.Second)

	time24h := time.Now().Truncate(time.Minute).Truncate(15 * time.Minute)

	runAgg := func(runTm time.Time) {
		agg, err := r.processAggregate(conns, runTm, "24h", "hour", -(24 * time.Hour))
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a24h = agg
		r.readerAggregate.lock.Unlock()
		time24h = time24h.Add(15 * time.Minute).Truncate(15 * time.Minute)
	}
	runAgg(time24h)
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time24h) {
				runAgg(time24h)
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

	ticker := time.NewTicker(time.Second)

	time7d := time.Now().Truncate(time.Minute).Truncate(30 * time.Minute)

	runAgg := func(runTm time.Time) {
		agg, err := r.processAggregate(conns, runTm, "7d", "day", -(7 * 24 * time.Hour))
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a7d = agg
		r.readerAggregate.lock.Unlock()
		time7d = time7d.Add(30 * time.Minute).Truncate(30 * time.Minute)
	}
	runAgg(time7d)
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time7d) {
				runAgg(time7d)
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

	ticker := time.NewTicker(time.Second)

	time30d := time.Now().Truncate(time.Minute).Truncate(30 * time.Minute)

	runAgg := func(runTm time.Time) {
		agg, err := r.processAggregate(conns, runTm, "30d", "day", -(30 * 24 * time.Hour))
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		r.readerAggregate.lock.Lock()
		r.readerAggregate.a30d = agg
		r.readerAggregate.lock.Unlock()
		time30d = time30d.Add(30 * time.Minute).Truncate(30 * time.Minute)
	}
	runAgg(time30d)
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time30d) {
				runAgg(time30d)
			}
		case <-r.doneCh:
			return
		}
	}
}
