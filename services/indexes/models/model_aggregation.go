package models

import (
	"context"
	"database/sql"
	"time"

	"github.com/gocraft/dbr/v2"
)

const (
	StateLiveID   = 1
	StateBackupID = 2
)

type AvmAggregate struct {
	AggregateTS       time.Time `json:"aggregateTS"`
	AssetID           string    `json:"assetId"`
	TransactionVolume string    `json:"transactionVolume"`
	TransactionCount  uint64    `json:"transactionCount"`
	AddressCount      uint64    `json:"addresCount"`
	AssetCount        uint64    `json:"assetCount"`
	OutputCount       uint64    `json:"outputCount"`
}

type AvmAggregateCount struct {
	Address          string
	AssetID          string
	TransactionCount uint64
	TotalReceived    string
	TotalSent        string
	Balance          string
	UtxoCount        uint64
}

type AggregateTxFee struct {
	AggregateTS time.Time `json:"aggregateTS"`
	TxFee       string    `json:"txFee"`
}

type AvmAssetAggregateState struct {
	ID               uint64    `json:"id"`
	CreatedAt        time.Time `json:"createdAt"`
	CurrentCreatedAt time.Time `json:"currentCreatedAt"`
}

func PurgeOldAvmAssetAggregation(ctx context.Context, sess *dbr.Session, time time.Time) (sql.Result, error) {
	return sess.
		DeleteFrom("avm_asset_aggregation").
		Where("aggregate_ts < ?", time).
		ExecContext(ctx)
}

func SelectAvmAssetAggregations(ctx context.Context, sess *dbr.Session) ([]*AvmAggregate, error) {
	avmAggregates := make([]*AvmAggregate, 0, 2)

	_, err := sess.Select("aggregate_ts", "asset_id", "transaction_volume", "transaction_count", "address_count", "asset_count", "output_count").
		From("avm_asset_aggregation").
		LoadContext(ctx, &avmAggregates)

	return avmAggregates, err
}

func UpdateAvmAssetAggregation(ctx context.Context, sess *dbr.Session, avmAggregate AvmAggregate) (sql.Result, error) {
	return sess.ExecContext(ctx, "update avm_asset_aggregation "+
		"set "+
		" transaction_volume="+avmAggregate.TransactionVolume+","+
		" transaction_count=?,"+
		" address_count=?,"+
		" asset_count=?,"+
		" output_count=? "+
		"where aggregate_ts = ? AND asset_id = ?",
		avmAggregate.TransactionCount,
		avmAggregate.AddressCount,
		avmAggregate.OutputCount,
		avmAggregate.AssetCount,
		avmAggregate.AggregateTS,
		avmAggregate.AssetID)
}

func InsertAvmAssetAggregation(ctx context.Context, sess *dbr.Session, avmAggregate AvmAggregate) (sql.Result, error) {
	return sess.ExecContext(ctx, "insert into avm_asset_aggregation "+
		"(aggregate_ts,asset_id,transaction_volume,transaction_count,address_count,asset_count,output_count) "+
		"values (?,?,"+avmAggregate.TransactionVolume+",?,?,?,?)",
		avmAggregate.AggregateTS,
		avmAggregate.AssetID,
		avmAggregate.TransactionCount,
		avmAggregate.AddressCount,
		avmAggregate.AssetCount,
		avmAggregate.OutputCount)
}

func SelectAvmAssetAggregationCounts(ctx context.Context, sess *dbr.Session) ([]*AvmAggregateCount, error) {
	avmAggregateCounts := make([]*AvmAggregateCount, 0, 2)

	_, err := sess.Select("address", "asset_id", "transaction_count", "total_received", "total_sent", "balance", "utxo_count").
		From("avm_asset_address_counts").
		LoadContext(ctx, &avmAggregateCounts)

	return avmAggregateCounts, err
}

func UpdateAvmAssetAggregationCount(ctx context.Context, sess *dbr.Session, avmAggregate AvmAggregateCount) (sql.Result, error) {
	return sess.ExecContext(ctx, "update avm_asset_address_counts "+
		"set "+
		" transaction_count=?,"+
		" total_received="+avmAggregate.TotalReceived+","+
		" total_sent="+avmAggregate.TotalSent+","+
		" balance="+avmAggregate.Balance+","+
		" utxo_count=? "+
		"where address = ? AND asset_id = ?",
		avmAggregate.TransactionCount,
		avmAggregate.UtxoCount,
		avmAggregate.Address,
		avmAggregate.AssetID)
}

func InsertAvmAssetAggregationCount(ctx context.Context, sess *dbr.Session, avmAggregate AvmAggregateCount) (sql.Result, error) {
	return sess.ExecContext(ctx, "insert into avm_asset_address_counts "+
		"(address,asset_id,transaction_count,total_received,total_sent,balance,utxo_count) "+
		"values (?,?,?,"+avmAggregate.TotalReceived+","+avmAggregate.TotalSent+","+avmAggregate.Balance+",?)",
		avmAggregate.Address,
		avmAggregate.AssetID,
		avmAggregate.TransactionCount,
		avmAggregate.UtxoCount)
}

func SelectFeeBurn(ctx context.Context, sess *dbr.Session) ([]*AggregateTxFee, error) {
	resuls := make([]*AggregateTxFee, 0, 2)

	_, err := sess.Select("aggregate_ts", "tx_fee").
		From("aggregate_txfee").
		LoadContext(ctx, &resuls)

	return resuls, err
}

func UpdateFeeBurn(ctx context.Context, sess *dbr.Session, feeBurn AggregateTxFee) (sql.Result, error) {
	return sess.ExecContext(ctx, "update aggregate_txfee "+
		"set "+
		"tx_fee="+feeBurn.TxFee+" "+
		"where aggregate_ts = ? ",
		feeBurn.AggregateTS)
}

func InsertFeeBurn(ctx context.Context, sess *dbr.Session, feeBurn AggregateTxFee) (sql.Result, error) {
	return sess.ExecContext(ctx, "insert into aggregate_txfee "+
		"(aggregate_ts, tx_fee) "+
		"values (?,"+feeBurn.TxFee+")",
		feeBurn.AggregateTS)
}

func UpdateAvmAssetAggregationLiveStateTimestamp(ctx context.Context, sess dbr.SessionRunner, time time.Time) (sql.Result, error) {
	return sess.
		Update("avm_asset_aggregation_state").
		Set("created_at", time).
		Where("id = ? and created_at > ?", StateLiveID, time).
		ExecContext(ctx)
}

func SelectAvmAssetAggregationState(ctx context.Context, sess dbr.SessionRunner, id uint64) (AvmAssetAggregateState, error) {
	var avmAssetAggregateState AvmAssetAggregateState
	err := sess.
		Select("id", "created_at", "current_created_at").
		From("avm_asset_aggregation_state").
		Where("id = ?", id).
		LoadOneContext(ctx, &avmAssetAggregateState)
	return avmAssetAggregateState, err
}

func UpdateAvmAssetAggregationState(ctx context.Context, sess *dbr.Session, avmAssetAggregationState AvmAssetAggregateState) (sql.Result, error) {
	return sess.
		Update("avm_asset_aggregation_state").
		Set("created_at", avmAssetAggregationState.CreatedAt).
		Set("current_created_at", avmAssetAggregationState.CurrentCreatedAt).
		Where("id=?", avmAssetAggregationState.ID).
		ExecContext(ctx)
}

func InsertAvmAssetAggregationState(ctx context.Context, sess dbr.SessionRunner, avmAssetAggregationState AvmAssetAggregateState) (sql.Result, error) {
	return sess.
		InsertInto("avm_asset_aggregation_state").
		Pair("id", avmAssetAggregationState.ID).
		Pair("created_at", avmAssetAggregationState.CreatedAt).
		Pair("current_created_at", avmAssetAggregationState.CurrentCreatedAt).
		ExecContext(ctx)
}

func DeleteAvmAssetAggregationState(ctx context.Context, sess *dbr.Session, id uint64) (sql.Result, error) {
	return sess.
		DeleteFrom("avm_asset_aggregation_state").
		Where("id = ?", id).
		ExecContext(ctx)
}
