// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

func TestAggregateTxfee(t *testing.T) {
	reader, closeFn := newTestIndex(t)
	defer closeFn()

	ctx := newTestContext()

	sess, _ := reader.conns.DB().NewSession("test_aggregate_tx_fee", cfg.RequestTimeout)
	_, _ = sess.DeleteFrom("aggregate_txfee").ExecContext(ctx)

	tnow := time.Now().UTC().Truncate(1 * time.Second).Add(-1 * time.Hour)

	_, _ = sess.InsertInto("aggregate_txfee").
		Pair("aggregate_ts", tnow).
		Pair("tx_fee", 10).
		ExecContext(ctx)
	_, _ = sess.InsertInto("aggregate_txfee").
		Pair("aggregate_ts", tnow.Add(-1*time.Hour)).
		Pair("tx_fee", 15).
		ExecContext(ctx)

	starttime := tnow.Add(-2 * time.Hour)
	endtime := tnow.Add(1 * time.Second)
	p := params.TxfeeAggregateParams{ListParams: params.ListParams{StartTime: starttime, EndTime: endtime}}
	agg, err := reader.TxfeeAggregate(ctx, &p)
	if err != nil {
		t.Error("error", err)
	}
	t.Error(fmt.Sprintf("%v", agg))
	if agg.TxfeeAggregates.Txfee != models.TokenAmount("25") {
		t.Error("aggregate tx invalid")
	}
	if agg.StartTime != starttime || agg.EndTime != endtime {
		t.Error("aggregate tx invalid")
	}

	p = params.TxfeeAggregateParams{ListParams: params.ListParams{StartTime: tnow.Add(-50 * time.Minute), EndTime: tnow.Add(1 * time.Second)}}
	agg, _ = reader.TxfeeAggregate(ctx, &p)

	if agg.TxfeeAggregates.Txfee != models.TokenAmount("10") {
		t.Error("aggregate tx invalid")
	}
}

func newTestIndex(t *testing.T) (*Reader, func()) {
	// Start test redis
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("Failed to create miniredis server:", err.Error())
	}

	logConf, err := logging.DefaultConfig()
	if err != nil {
		t.Fatal("Failed to create logging config:", err.Error())
	}

	conf := cfg.Services{
		Logging: logConf,
		DB: &cfg.DB{
			TXDB:   true,
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true",
		},
		Redis: &cfg.Redis{
			Addr: s.Addr(),
		},
	}

	conf.Log = logging.NoLog{}
	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	reader := NewReader(conns)
	return reader, func() {
		s.Close()
		_ = conns.Close()
	}
}

func newTestContext() context.Context {
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	time.AfterFunc(5*time.Second, cancelFn)
	return ctx
}
