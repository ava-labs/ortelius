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
	tasker.initMetrics()

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
	if len(avmAggregateModels) != 1 {
		t.Errorf("aggregate map not created")
	}

	if !(avmAggregateModels[0].AggregateTS.Equal(aggregationTime.UTC()) &&
		avmAggregateModels[0].AssetID == "testasset" &&
		avmAggregateModels[0].ChainID == "cid" &&
		avmAggregateModels[0].TransactionVolume == "200" &&
		avmAggregateModels[0].TransactionCount == 1 &&
		avmAggregateModels[0].AssetCount == 1 &&
		avmAggregateModels[0].OutputCount == 2) {
		t.Error("aggregate map invalid", avmAggregateModels[0])
	}

	avmAggregateCounts, _ := models.SelectAvmAssetAggregationCounts(ctx, sess)
	if len(avmAggregateCounts) != 1 {
		t.Errorf("aggregate map count not created")
	}

	if !(avmAggregateCounts[0].Address == "addr1" &&
		avmAggregateCounts[0].AssetID == "testasset" &&
		avmAggregateCounts[0].ChainID == "cid" &&
		avmAggregateCounts[0].TransactionCount == 1 &&
		avmAggregateCounts[0].TotalReceived == "100" &&
		avmAggregateCounts[0].TotalSent == "0" &&
		avmAggregateCounts[0].Balance == "100" &&
		avmAggregateCounts[0].UtxoCount == 1) {
		t.Error("aggregate map count invalid", avmAggregateCounts[0])
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

func TestReplaceAvmAggregate(t *testing.T) {
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

	initData(ctx, sess, pastime, t)

	agg := models.AvmAggregate{
		AggregateTS:       pastime,
		AssetID:           "aid",
		TransactionVolume: "1",
	}

	err = tasker.replaceAvmAggregate(agg)
	if err != nil {
		t.Fatal("failed", err)
	}
	values, err := models.SelectAvmAssetAggregations(ctx, sess)
	if err != nil {
		t.Fatal("failed", err)
	}
	if len(values) != 1 {
		t.Fatal("failed", err)
	}
	if values[0].TransactionVolume != "1" {
		t.Fatal("failed", err)
	}

	agg.TransactionVolume = "2"
	err = tasker.replaceAvmAggregate(agg)
	if err != nil {
		t.Fatal("failed", err)
	}
	values, err = models.SelectAvmAssetAggregations(ctx, sess)
	if err != nil {
		t.Fatal("failed", err)
	}
	if len(values) != 1 {
		t.Fatal("failed", err)
	}
	if values[0].TransactionVolume != "2" {
		t.Fatal("failed", err)
	}
}

func TestReplaceAvmAggregateCount(t *testing.T) {
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

	initData(ctx, sess, pastime, t)

	agg := models.AvmAggregateCount{
		Address:          "addr1",
		AssetID:          "aid",
		TransactionCount: 1,
		TotalReceived:    "0",
		TotalSent:        "0",
		Balance:          "0",
	}

	err = tasker.replaceAvmAggregateCount(agg)
	if err != nil {
		t.Fatal("failed", err)
	}
	values, err := models.SelectAvmAssetAggregationCounts(ctx, sess)
	if err != nil {
		t.Fatal("failed", err)
	}
	if len(values) != 1 {
		t.Fatal("failed", err)
	}
	if values[0].TransactionCount != 1 {
		t.Fatal("failed", err)
	}

	agg.TransactionCount = 2
	err = tasker.replaceAvmAggregateCount(agg)
	if err != nil {
		t.Fatal("failed", err)
	}
	values, err = models.SelectAvmAssetAggregationCounts(ctx, sess)
	if err != nil {
		t.Fatal("failed", err)
	}
	if len(values) != 1 {
		t.Fatal("failed", err)
	}
	if values[0].TransactionCount != 2 {
		t.Fatal("failed", err)
	}
}
