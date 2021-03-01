package avax

import (
	"context"
	"net/url"
	"time"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

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
		urlv.Add(params.KeyIntervalSize, "hour")
		err := p.ForValues(1, urlv)
		if err == nil {
			p.ListParams.EndTime = time.Now().Truncate(time.Minute)
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-time.Hour)
			p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
			_, _ = r.Aggregate(ctx, p, conns)
		}
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1h) {
				runAgg()
				time1h = time1h.Add(5 * time.Minute)
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
		urlv.Add(params.KeyIntervalSize, "day")
		err := p.ForValues(1, urlv)
		if err == nil {
			p.ListParams.EndTime = time.Now().Truncate(time.Minute)
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-(24 * time.Hour))
			p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
			_, _ = r.Aggregate(ctx, p, conns)
		}
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time24h) {
				runAgg()
				time24h = time24h.Add(10 * time.Minute)
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
		urlv.Add(params.KeyIntervalSize, "week")
		err := p.ForValues(1, urlv)
		if err == nil {
			p.ListParams.EndTime = time.Now().Truncate(time.Minute)
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-(7 * 24 * time.Hour))
			p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
			_, _ = r.Aggregate(ctx, p, conns)
		}
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time7d) {
				runAgg()
				time7d = time7d.Add(time.Hour)
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
		if err == nil {
			p.ListParams.EndTime = time.Now().Truncate(time.Minute)
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-(30 * 24 * time.Hour))
			p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
			_, _ = r.Aggregate(ctx, p, conns)
		}
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time30d) {
				runAgg()
				time30d = time30d.Add(4 * time.Hour)
			}
		case <-r.doneCh:
			return
		}
	}
}

func (r *Reader) aggregateProcessor1y(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time1y := time.Now().Truncate(time.Minute)

	runAgg := func() {
		ctx := context.Background()
		p := &params.AggregateParams{}
		urlv := url.Values{}
		urlv.Add(params.KeyIntervalSize, "year")
		err := p.ForValues(1, urlv)
		if err == nil {
			p.ListParams.EndTime = time.Now().Truncate(time.Minute)
			p.ListParams.StartTime = p.ListParams.EndTime.Add(-(365 * 24 * time.Hour))
			p.ChainIDs = append(p.ChainIDs, r.sc.GenesisContainer.XChainID.String())
			_, _ = r.Aggregate(ctx, p, conns)
		}
	}
	runAgg()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1y) {
				runAgg()
				time1y = time1y.Add(12 * time.Hour)
			}
		case <-r.doneCh:
			return
		}
	}
}
