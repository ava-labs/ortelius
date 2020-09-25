package models

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
)

type EventReceiverTest struct {
}

func (*EventReceiverTest) Event(eventName string)                          {}
func (*EventReceiverTest) EventKv(eventName string, kvs map[string]string) {}
func (*EventReceiverTest) EventErr(eventName string, err error) error      { return nil }
func (*EventReceiverTest) EventErrKv(eventName string, err error, kvs map[string]string) error {
	return nil
}
func (*EventReceiverTest) Timing(eventName string, nanoseconds int64)                          {}
func (*EventReceiverTest) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {}

func TestInsertUpdateAvmAssetAggregation(t *testing.T) {
	var eventReceiver EventReceiverTest

	c, err := dbr.Open("mysql", "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true", &eventReceiver)
	if err != nil {
		t.Errorf("open db %s", err.Error())
	}

	h := health.NewStream()

	co := services.NewConnections(h, c, nil)

	ctx := context.Background()

	job := co.Stream().NewJob("model_aggregation_test")
	sess := co.DB().NewSession(job)

	_, _ = sess.DeleteFrom("avm_asset_aggregation").ExecContext(ctx)
	_, _ = sess.DeleteFrom("avm_asset_address_counts").ExecContext(ctx)

	var avmAggregate AvmAggregate
	avmAggregate.AggregateTS = time.Now()
	avmAggregate.AssetId = "as1"
	avmAggregate.TransactionVolume = "1"
	avmAggregate.TransactionCount = 1
	avmAggregate.AddressCount = 1
	avmAggregate.AssetCount = 1
	avmAggregate.OutputCount = 1
	_, err = InsertAvmAssetAggregation(ctx, sess, avmAggregate)
	if err != nil {
		t.Errorf("insert failed %s", err.Error())
	}

	avmAggregateCounts, _ := SelectAvmAssetAggregations(ctx, sess)
	if len(avmAggregateCounts) != 1 {
		t.Errorf("not created")
	}

	for _, aggregateMapValue := range avmAggregateCounts {
		if aggregateMapValue.AssetId != "as1" &&
			aggregateMapValue.TransactionVolume != "1" &&
			aggregateMapValue.TransactionCount != 1 &&
			aggregateMapValue.AssetCount != 1 {
			t.Errorf("aggregate map invalid")
		}
	}

	avmAggregate.TransactionVolume = "2"
	avmAggregate.TransactionCount = 2
	avmAggregate.AddressCount = 2
	avmAggregate.AssetCount = 2
	avmAggregate.OutputCount = 2
	_, err = UpdateAvmAssetAggregation(ctx, sess, avmAggregate)
	if err != nil {
		t.Errorf("update failed %s", err.Error())
	}
	avmAggregateCounts, _ = SelectAvmAssetAggregations(ctx, sess)
	if len(avmAggregateCounts) != 1 {
		t.Errorf("not created")
	}

	for _, aggregateMapValue := range avmAggregateCounts {
		if aggregateMapValue.AssetId != "as1" &&
			aggregateMapValue.TransactionVolume != "2" &&
			aggregateMapValue.TransactionCount != 2 &&
			aggregateMapValue.AssetCount != 2 {
			t.Errorf("aggregate map invalid")
		}
	}
}

func TestInsertUpdateAvmAssetCount(t *testing.T) {
	var eventReceiver EventReceiverTest

	c, err := dbr.Open("mysql", "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true", &eventReceiver)
	if err != nil {
		t.Errorf("open db %s", err.Error())
	}

	h := health.NewStream()

	co := services.NewConnections(h, c, nil)

	ctx := context.Background()

	job := co.Stream().NewJob("model_aggregation_test")
	sess := co.DB().NewSession(job)

	sess.DeleteFrom("avm_asset_address_counts").ExecContext(ctx)

	var avmAggregate AvmAggregateCount
	avmAggregate.Address = "ad1"
	avmAggregate.AssetID = "as1"
	avmAggregate.TransactionCount = 1
	avmAggregate.TotalReceived = "1"
	avmAggregate.TotalSent = "1"
	avmAggregate.Balance = "1"
	avmAggregate.UtxoCount = 1

	_, err = InsertAvmAssetAggregationCount(ctx, sess, avmAggregate)
	if err != nil {
		t.Errorf("insert failed %s", err.Error())
	}

	avmAggregateCounts, _ := SelectAvmAssetAggregationCounts(ctx, sess)
	if len(avmAggregateCounts) != 1 {
		t.Errorf("not created")
	}

	for _, aggregateCountMapValue := range avmAggregateCounts {
		if aggregateCountMapValue.Address != "ad1" &&
			aggregateCountMapValue.AssetID != "as1" &&
			aggregateCountMapValue.TransactionCount != 1 &&
			aggregateCountMapValue.TotalSent != "1" &&
			aggregateCountMapValue.TotalReceived != "1" &&
			aggregateCountMapValue.Balance != "1" &&
			aggregateCountMapValue.UtxoCount != 1 {
			t.Errorf("insert invalid")
		}
	}

	avmAggregate.TransactionCount = 2
	avmAggregate.TotalReceived = "2"
	avmAggregate.TotalSent = "2"
	avmAggregate.Balance = "2"
	avmAggregate.UtxoCount = 2
	_, err = UpdateAvmAssetAggregationCount(ctx, sess, avmAggregate)
	if err != nil {
		t.Errorf("update failed %s", err.Error())
	}

	avmAggregateCounts, _ = SelectAvmAssetAggregationCounts(ctx, sess)
	if len(avmAggregateCounts) != 1 {
		t.Errorf("update")
	}

	for _, aggregateCountMapValue := range avmAggregateCounts {
		if aggregateCountMapValue.Address != "ad1" &&
			aggregateCountMapValue.AssetID != "as1" &&
			aggregateCountMapValue.TransactionCount != 2 &&
			aggregateCountMapValue.TotalSent != "2" &&
			aggregateCountMapValue.TotalReceived != "2" &&
			aggregateCountMapValue.Balance != "2" &&
			aggregateCountMapValue.UtxoCount != 2 {
			t.Errorf("update invalid")
		}
	}
}
