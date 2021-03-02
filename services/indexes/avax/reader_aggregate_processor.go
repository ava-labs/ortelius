package avax

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

type ReaderAggregate struct {
	lock sync.RWMutex

	a1h   *models.AggregatesHistogram
	a1ht  *time.Time
	a24h  *models.AggregatesHistogram
	a24ht *time.Time
	a7d   *models.AggregatesHistogram
	a7dt  *time.Time
	a30d  *models.AggregatesHistogram
	a30dt *time.Time
}

func (r *Reader) aggregateProcessor() error {
	if !r.sc.IsAggregateCache {
		return nil
	}

	var connectionsaggr *services.Connections

	var connections1h *services.Connections
	var connections24h *services.Connections
	var connections7d *services.Connections
	var connections30d *services.Connections
	var err error

	closeDBForError := func() {
		if connectionsaggr != nil {
			_ = connectionsaggr.Close()
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
		_, err := sess.Select("asset_id").
			Distinct().
			From("avm_outputs").
			Where("created_at > ? and asset_id <> ?",
				time.Now().UTC().Add(-24*time.Hour),
				r.sc.GenesisContainer.AvaxAssetID.String(),
			).
			LoadContext(ctx, &assetsFound)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}

		assets := append([]string{}, r.sc.GenesisContainer.AvaxAssetID.String())
		assets = append(assets, assetsFound...)

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
			r.sc.Log.Info("aggregate %s %v-%v", id.String(), p.ListParams.StartTime, p.ListParams.EndTime)
			_, err = r.Aggregate(ctx, p, conns)
			if err != nil {
				r.sc.Log.Warn("Aggregate %v", err)
				return
			}
		}
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

func (r *Reader) aggregateProcessor1h(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time1h := time.Now().Truncate(time.Minute)

	runAgg := func() {
		ctx := context.Background()
		p := &params.AggregateParams{}
		urlv := url.Values{}
		urlv.Add(params.KeyIntervalSize, "5m")
		err := p.ForValues(1, urlv)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		p.ListParams.EndTime = time.Now().Truncate(time.Minute)
		p.ListParams.StartTime = p.ListParams.EndTime.Add(-time.Hour)
		p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
		r.sc.Log.Info("aggregate 1h interval 5m %v->%v", p.ListParams.StartTime, p.ListParams.EndTime)
		agg, err := r.Aggregate(ctx, p, conns)
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
		ctx := context.Background()
		p := &params.AggregateParams{}
		urlv := url.Values{}
		urlv.Add(params.KeyIntervalSize, "hour")
		err := p.ForValues(1, urlv)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		p.ListParams.EndTime = time.Now().Truncate(time.Minute)
		p.ListParams.StartTime = p.ListParams.EndTime.Add(-(24 * time.Hour))
		p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
		r.sc.Log.Info("aggregate 1d interval 1h %v->%v", p.ListParams.StartTime, p.ListParams.EndTime)
		agg, err := r.Aggregate(ctx, p, conns)
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
		ctx := context.Background()
		p := &params.AggregateParams{}
		urlv := url.Values{}
		urlv.Add(params.KeyIntervalSize, "day")
		err := p.ForValues(1, urlv)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		p.ListParams.EndTime = time.Now().Truncate(time.Minute)
		p.ListParams.StartTime = p.ListParams.EndTime.Add(-(7 * 24 * time.Hour))
		p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
		r.sc.Log.Info("aggregate 1w interval 1d %v->%v", p.ListParams.StartTime, p.ListParams.EndTime)
		agg, err := r.Aggregate(ctx, p, conns)
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
		ctx := context.Background()
		p := &params.AggregateParams{}
		urlv := url.Values{}
		urlv.Add(params.KeyIntervalSize, "month")
		err := p.ForValues(1, urlv)
		if err != nil {
			r.sc.Log.Warn("Aggregate %v", err)
			return
		}
		p.ListParams.EndTime = time.Now().Truncate(time.Minute)
		p.ListParams.StartTime = p.ListParams.EndTime.Add(-(30 * 24 * time.Hour))
		p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
		r.sc.Log.Info("aggregate 1m interval 1d %v->%v", p.ListParams.StartTime, p.ListParams.EndTime)
		agg, err := r.Aggregate(ctx, p, conns)
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
