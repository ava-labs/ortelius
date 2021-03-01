package avax

import (
	"context"
	"net/url"
	"time"

	"github.com/ava-labs/ortelius/services"
	params2 "github.com/ava-labs/ortelius/services/indexes/params"
)

func (r *Reader) aggregateProcessor1h(conns *services.Connections) {
	defer func() {
		_ = conns.Close()
	}()

	ticker := time.NewTicker(1 * time.Minute)

	time1h := time.Now().Truncate(time.Minute)

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1h) {
				params := &params2.AggregateParams{}
				urlv := url.Values{}
				urlv.Add(params2.KeyIntervalSize, "hour")
				params.ListParams.EndTime = time.Now().Truncate(time.Minute)
				params.ChainIDs = append(params.ChainIDs, r.sc.GenesisContainer.XChainID.String())
				err := params.ForValues(1, urlv)
				if err == nil {
					_, _ = r.Aggregate(ctx, params, conns)
				}
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

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time24h) {
				params := &params2.AggregateParams{}
				urlv := url.Values{}
				urlv.Add(params2.KeyIntervalSize, "day")
				params.ListParams.EndTime = time.Now().Truncate(time.Minute)
				params.ChainIDs = append(params.ChainIDs, r.sc.GenesisContainer.XChainID.String())
				err := params.ForValues(1, urlv)
				if err == nil {
					_, _ = r.Aggregate(ctx, params, conns)
				}
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

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time7d) {
				params := &params2.AggregateParams{}
				urlv := url.Values{}
				urlv.Add(params2.KeyIntervalSize, "week")
				params.ListParams.EndTime = time.Now().Truncate(time.Minute)
				params.ChainIDs = append(params.ChainIDs, r.sc.GenesisContainer.XChainID.String())
				err := params.ForValues(1, urlv)
				if err == nil {
					_, _ = r.Aggregate(ctx, params, conns)
				}
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

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time30d) {
				params := &params2.AggregateParams{}
				urlv := url.Values{}
				urlv.Add(params2.KeyIntervalSize, "month")
				params.ListParams.EndTime = time.Now().Truncate(time.Minute)
				params.ChainIDs = append(params.ChainIDs, r.sc.GenesisContainer.XChainID.String())
				err := params.ForValues(1, urlv)
				if err == nil {
					_, _ = r.Aggregate(ctx, params, conns)
				}
				time30d = time30d.Add(1 * time.Hour)
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

	ctx := context.Background()

	for {
		select {
		case <-ticker.C:
			tnow := time.Now()
			if tnow.After(time1y) {
				params := &params2.AggregateParams{}
				urlv := url.Values{}
				urlv.Add(params2.KeyIntervalSize, "year")
				params.ListParams.EndTime = time.Now().Truncate(time.Minute)
				params.ChainIDs = append(params.ChainIDs, r.sc.GenesisContainer.XChainID.String())
				err := params.ForValues(1, urlv)
				if err == nil {
					_, _ = r.Aggregate(ctx, params, conns)
				}
				time1y = time1y.Add(24 * time.Hour)
			}
		case <-r.doneCh:
			return
		}
	}
}
