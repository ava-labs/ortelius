package stream

import (
	"context"
	"testing"
	"time"

	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/health"
)

func TestIntegration(t *testing.T) {
	var err error

	h := health.NewStream()

	c, _ := db.New(h, cfg.DB{Driver: "mysql", DSN: "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"})
	conf, _ := logging.DefaultConfig()
	log, _ := logging.New(conf)

	co := services.NewConnections(log, h, c, nil)

	// produce an expected timestamp to test..
	timenow := time.Now().Round(1 * time.Minute)
	timeProducerFunc := func() time.Time {
		return timenow
	}

	tasker := ProducerTasker{connections: co,
		timeStampProducer: timeProducerFunc,
	}

	ctx := context.Background()

	sess, _ := co.DB().NewSession("producertask", 5*time.Second)

	pastime := time.Now().Add(-5 * time.Hour).Round(1 * time.Minute).Add(1 * time.Second)

	aggregationTime := pastime.Truncate(timestampRollup)

	initData(ctx, sess, pastime, t)

	avmAggregate := models.AvmAggregate{}
	avmAggregate.AggregateTS = time.Now().Add(time.Duration(aggregateDeleteFrame.Milliseconds()+1) * time.Millisecond)
	avmAggregate.AssetID = "futureasset"
	_, _ = models.InsertAvmAssetAggregation(ctx, sess, avmAggregate)

	err = tasker.RefreshAggregates()
	if err != nil {
		t.Errorf("refresh failed %s", err.Error())
	}

	backupAggregationState, _ := models.SelectAvmAssetAggregationState(ctx, sess, models.StateBackupID)
	liveAggregationState, _ := models.SelectAvmAssetAggregationState(ctx, sess, models.StateLiveID)
	if liveAggregationState.ID != models.StateLiveID {
		t.Errorf("state live not created")
	}
	if !liveAggregationState.CreatedAt.Equal(timenow.Add(additionalHours)) {
		t.Errorf("state live createdat not reset to the future")
	}
	if backupAggregationState.ID != 0 {
		t.Errorf("state backup not removed")
	}

	count := 999999
	_, _ = sess.Select("count(*)").From("avm_asset_aggregation").
		Where("aggregate_ts < ?", time.Now().Add(aggregateDeleteFrame)).
		Load(&count)
	if count != 0 {
		t.Errorf("future avm_asset not removed")
	}

	avmAggregateModels, _ := models.SelectAvmAssetAggregations(ctx, sess)

	for _, aggregateMapValue := range avmAggregateModels {
		if !aggregateMapValue.AggregateTS.Equal(aggregationTime.UTC()) &&
			aggregateMapValue.AssetID != "testasset" &&
			aggregateMapValue.ChainID != "cid1" &&
			aggregateMapValue.TransactionVolume != "200" &&
			aggregateMapValue.TransactionCount != 1 &&
			aggregateMapValue.AssetCount != 2 {
			t.Errorf("aggregate map invalid")
		}
	}

	avmAggregateCounts, _ := models.SelectAvmAssetAggregationCounts(ctx, sess)
	if len(avmAggregateCounts) != 1 {
		t.Errorf("aggregate map count not created")
	}

	for _, aggregateCountMapValue := range avmAggregateCounts {
		if aggregateCountMapValue.Address != "id1" &&
			aggregateCountMapValue.AssetID != "testasset" &&
			aggregateCountMapValue.AssetID != "ch1" &&
			aggregateCountMapValue.TransactionCount != 1 &&
			aggregateCountMapValue.TotalSent != "0" &&
			aggregateCountMapValue.TotalReceived != "100" &&
			aggregateCountMapValue.Balance != "100" &&
			aggregateCountMapValue.UtxoCount != 1 {
			t.Errorf("aggregate map count invalid")
		}
	}

	aggregateTxFees, _ := models.SelectAggregateTxFee(ctx, sess)
	if len(aggregateTxFees) != 1 {
		t.Errorf("aggregate map count not created")
	}

	for _, aggregateTxFee := range aggregateTxFees {
		if !aggregateTxFee.AggregateTS.Equal(aggregationTime.UTC()) &&
			aggregateTxFee.TxFee != "1" {
			t.Errorf("aggregate map count invalid")
		}
	}
}

func initData(ctx context.Context, sess *dbr.Session, pastime time.Time, t *testing.T) {
	// cleanup for run.
	_, _ = models.DeleteAvmAssetAggregationState(ctx, sess, models.StateBackupID)
	_, _ = models.DeleteAvmAssetAggregationState(ctx, sess, models.StateLiveID)
	_, _ = sess.DeleteFrom("avm_asset_aggregation").ExecContext(ctx)
	_, _ = sess.DeleteFrom("avm_asset_address_counts").ExecContext(ctx)
	_, _ = sess.DeleteFrom("aggregate_txfee").ExecContext(ctx)
	_, _ = sess.DeleteFrom("avm_outputs").ExecContext(ctx)
	_, _ = sess.DeleteFrom("avm_output_addresses").ExecContext(ctx)
	_, _ = sess.DeleteFrom("avm_transactions").ExecContext(ctx)

	_, err := sess.InsertInto("avm_outputs").
		Pair("id", "id1").
		Pair("chain_id", "cid").
		Pair("output_index", 1).
		Pair("output_type", 1).
		Pair("locktime", 1).
		Pair("threshold", 1).
		Pair("created_at", pastime).
		Pair("asset_id", "testasset").
		Pair("amount", 100).
		Pair("transaction_id", 1).
		Exec()
	if err != nil {
		t.Error("insert avm_outputs", err)
	}
	_, err = sess.InsertInto("avm_outputs").
		Pair("id", "id2").
		Pair("chain_id", "cid").
		Pair("output_index", 2).
		Pair("output_type", 1).
		Pair("locktime", 1).
		Pair("threshold", 1).
		Pair("created_at", pastime).
		Pair("asset_id", "testasset").
		Pair("amount", 100).
		Pair("transaction_id", 1).
		Exec()
	if err != nil {
		t.Error("insert avm_outputs", err)
	}
	_, err = sess.InsertInto("avm_output_addresses").
		Pair("output_id", "id1").
		Pair("address", "addr1").
		Pair("created_at", pastime).
		Exec()
	if err != nil {
		t.Error("insert avm_output_addresses", err)
	}
	_, err = sess.InsertInto("avm_transactions").
		Pair("id", "id1").
		Pair("chain_id", "cid").
		Pair("type", "type").
		Pair("created_at", pastime).
		Exec()
	if err != nil {
		t.Error("insert avm_transactions", err)
	}
}

func TestHandleBackupState(t *testing.T) {
	h := health.NewStream()

	c, _ := db.New(h, cfg.DB{Driver: "mysql", DSN: "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"})
	conf, _ := logging.DefaultConfig()
	log, _ := logging.New(conf)

	co := services.NewConnections(log, h, c, nil)

	ctx := context.Background()

	sess, _ := co.DB().NewSession("producertasker", 5*time.Second)

	// cleanup for run.
	_, _ = models.DeleteAvmAssetAggregationState(ctx, sess, models.StateBackupID)
	_, _ = models.DeleteAvmAssetAggregationState(ctx, sess, models.StateLiveID)

	timeNow := time.Now().Round(1 * time.Minute)

	_, _ = models.InsertAvmAssetAggregationState(ctx, sess, models.AvmAssetAggregateState{
		ID:               models.StateBackupID,
		CreatedAt:        timeNow,
		CurrentCreatedAt: timeNow})

	state := models.AvmAssetAggregateState{
		ID:               models.StateLiveID,
		CreatedAt:        time.Unix(1, 0),
		CurrentCreatedAt: time.Unix(1, 0)}

	timenow := time.Now().Round(1 * time.Minute)
	timeProducerFunc := func() time.Time {
		return timenow
	}

	producerTask := ProducerTasker{connections: co,
		timeStampProducer: timeProducerFunc,
	}

	backupState, _ := producerTask.updateBackupState(ctx, sess, state)
	if backupState.ID != models.StateBackupID {
		t.Fatal("invalid state")
	}

	if !backupState.CurrentCreatedAt.Equal(state.CurrentCreatedAt) {
		t.Fatal("backup state current created not updated")
	}
}
