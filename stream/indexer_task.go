package stream

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

var (
	contextDuration = 5 * time.Minute

	aggregationTick      = 20 * time.Second
	aggregateDeleteFrame = (-1 * 24 * 366) * time.Hour

	// rollup all aggregates to 1 minute.
	timestampRollup = 1 * time.Minute

	// == timestampRollup.Seconds() but not a float...
	timestampRollupSecs = 60

	aggregateColumns = []string{
		fmt.Sprintf("FROM_UNIXTIME(floor(UNIX_TIMESTAMP(avm_outputs.created_at) / %d) * %d) as aggregate_ts", timestampRollupSecs, timestampRollupSecs),
		"avm_outputs.asset_id",
		"CAST(COALESCE(SUM(avm_outputs.amount), 0) AS CHAR) AS transaction_volume",
		"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
		"COUNT(DISTINCT(avm_output_addresses.address)) AS address_count",
		"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
		"COUNT(avm_outputs.id) AS output_count",
	}

	additionalHours = (365 * 24) * time.Hour
	blank           = models.AvmAssetAggregateState{}
)

type ProducerTasker struct {
	initlock                sync.RWMutex
	connections             *services.Connections
	log                     logging.Logger
	plock                   sync.Mutex
	avmOutputsCursor        func(ctx context.Context, sess *dbr.Session, aggregateTs time.Time) (*sql.Rows, error)
	insertAvmAggregate      func(ctx context.Context, sess *dbr.Session, avmAggregate models.AvmAggregate) (sql.Result, error)
	updateAvmAggregate      func(ctx context.Context, sess *dbr.Session, avmAggregate models.AvmAggregate) (sql.Result, error)
	insertAvmAggregateCount func(ctx context.Context, sess *dbr.Session, avmAggregate models.AvmAggregateCount) (sql.Result, error)
	updateAvmAggregateCount func(ctx context.Context, sess *dbr.Session, avmAggregate models.AvmAggregateCount) (sql.Result, error)
	timeStampProducer       func() time.Time
}

var producerTaskerInstance = ProducerTasker{
	avmOutputsCursor:        AvmOutputsAggregateCursor,
	insertAvmAggregate:      models.InsertAvmAssetAggregation,
	updateAvmAggregate:      models.UpdateAvmAssetAggregation,
	insertAvmAggregateCount: models.InsertAvmAssetAggregationCount,
	updateAvmAggregateCount: models.UpdateAvmAssetAggregationCount,
	timeStampProducer:       time.Now,
}

func initializeConsumerTasker(conf cfg.Config, log logging.Logger) error {
	producerTaskerInstance.initlock.Lock()
	defer producerTaskerInstance.initlock.Unlock()

	if producerTaskerInstance.connections != nil {
		return nil
	}

	connections, err := services.NewConnectionsFromConfig(conf.Services)
	if err != nil {
		return err
	}

	producerTaskerInstance.connections = connections
	producerTaskerInstance.log = log
	producerTaskerInstance.Start()
	return nil
}

// under lock control.  update live state, and copy into backup state
func (t *ProducerTasker) updateBackupState(ctx context.Context, dbSession *dbr.Session, liveAggregationState models.AvmAssetAggregateState) (models.AvmAssetAggregateState, error) {
	var err error

	var sessTX *dbr.Tx
	sessTX, err = dbSession.Begin()
	if err != nil {
		return blank, err
	}

	defer sessTX.RollbackUnlessCommitted()

	// make a copy of the last created_at, and reset to now + 1 years in the future
	// we are using the db as an atomic swap...
	// current_created_at is set to the newest aggregation timestamp from the message queue.
	// and in the same update we reset created_at to a time in the future.
	// when we get new messages from the queue, they will execute the sql _after_ this update, and set created_at to an earlier date.
	updatedCurrentCreated := t.timeStampProducer().Add(additionalHours)
	_, err = sessTX.ExecContext(ctx, "update avm_asset_aggregation_state "+
		"set current_created_at=created_at, created_at=? "+
		"where id=?", updatedCurrentCreated, models.StateLiveID)
	if err != nil {
		t.log.Error("atomic swap %s", err)
		return blank, err
	}

	backupAggregateState := liveAggregationState
	backupAggregateState.ID = models.StateBackupID

	// id=stateBackupId backup row - for crash recovery
	_, err = models.InsertAvmAssetAggregationState(ctx, sessTX, backupAggregateState)
	if db.ErrIsDuplicateEntryError(err) {
		var sqlResult sql.Result
		sqlResult, err = sessTX.ExecContext(ctx, "update avm_asset_aggregation_state "+
			"set current_created_at=? "+
			"where id=? and current_created_at > ?",
			backupAggregateState.CurrentCreatedAt, backupAggregateState.ID, backupAggregateState.CurrentCreatedAt)
		if err != nil {
			t.log.Error("update backup state %s", err.Error())
			return blank, err
		}

		var rowsAffected int64
		rowsAffected, err = sqlResult.RowsAffected()

		if err != nil {
			t.log.Error("update backup state failed %s", err)
			return blank, err
		}

		if rowsAffected > 0 {
			// if we updated, refresh the backup state
			backupAggregateState, err = models.SelectAvmAssetAggregationState(ctx, sessTX, backupAggregateState.ID)
			if err != nil {
				t.log.Error("refresh backup state %s", err)
				return blank, err
			}
		}
	}

	err = sessTX.Commit()
	if err != nil {
		return blank, err
	}

	return backupAggregateState, nil
}

func (t *ProducerTasker) RefreshAggregates() error {
	t.plock.Lock()
	defer t.plock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), contextDuration)
	defer cancel()

	var err error
	sess, err := t.connections.DB().NewSession("producertasker", contextDuration)
	if err != nil {
		return err
	}

	var liveAggregationState models.AvmAssetAggregateState
	var backupAggregateState models.AvmAssetAggregateState

	// initialize the assset_aggregation_state table with id=stateLiveId row.
	// if the row has not been created..
	// created at and current created at set to time(0), so the first run will re-build aggregates for the entire db.
	_, _ = models.InsertAvmAssetAggregationState(ctx, sess,
		models.AvmAssetAggregateState{
			ID:               models.StateLiveID,
			CreatedAt:        time.Unix(1, 0),
			CurrentCreatedAt: time.Unix(1, 0)},
	)

	liveAggregationState, err = models.SelectAvmAssetAggregationState(ctx, sess, models.StateLiveID)
	// this is really bad, the state live row was not created..  we cannot proceed safely.
	if liveAggregationState.ID != models.StateLiveID {
		t.log.Error("unable to find live state")
		return err
	}

	// check if the backup row exists, if found we crashed from a previous run.
	backupAggregateState, _ = models.SelectAvmAssetAggregationState(ctx, sess, models.StateBackupID)

	if backupAggregateState.ID == uint64(models.StateBackupID) {
		// re-process from backup row..
		liveAggregationState = backupAggregateState
	} else {
		backupAggregateState, err = t.updateBackupState(ctx, sess, liveAggregationState)
		if err != nil {
			t.log.Error("unable to update backup state %s", err)
			return err
		}
	}

	// truncate to earliest minute.
	aggregateTS := liveAggregationState.CurrentCreatedAt.Truncate(timestampRollup)

	baseAggregateTS := aggregateTS

	aggregateTS, err = t.processAvmOutputs(ctx, sess, baseAggregateTS)
	if err != nil {
		return err
	}

	err = t.processAvmOutputAddressesCounts(ctx, sess, baseAggregateTS)
	if err != nil {
		return err
	}

	// everything worked, so we can wipe id=stateBackupId backup row
	// lets make sure our run created this row ..  so check for current_created_at match..
	// if we didn't create the row, the creator would delete it..  (some other producer running this code)
	// if things go really bad, then when the process restarts the row will be re-selected and deleted then..
	_, _ = sess.
		DeleteFrom("avm_asset_aggregation_state").
		Where("id = ? and current_created_at = ?", models.StateBackupID, backupAggregateState.CurrentCreatedAt).
		ExecContext(ctx)

	// delete aggregate data before aggregateDeleteFrame
	// *disable* _, _ = models.PurgeOldAvmAssetAggregation(ctx, sess, aggregateTS.Add(aggregateDeleteFrame))

	t.log.Info("processed up to %s", aggregateTS.String())

	return nil
}

func (t *ProducerTasker) processAvmOutputs(ctx context.Context, sess *dbr.Session, aggregateTS time.Time) (time.Time, error) {
	var err error
	var rows *sql.Rows
	rows, err = t.avmOutputsCursor(ctx, sess, aggregateTS)
	if err != nil {
		t.log.Error("error query %s", err)
		return time.Time{}, err
	}
	if rows.Err() != nil {
		t.log.Error("error query %s", rows.Err())
		return time.Time{}, rows.Err()
	}

	for ok := rows.Next(); ok; ok = rows.Next() {
		var avmAggregate models.AvmAggregate
		err = rows.Scan(&avmAggregate.AggregateTS,
			&avmAggregate.AssetID,
			&avmAggregate.TransactionVolume,
			&avmAggregate.TransactionCount,
			&avmAggregate.AddressCount,
			&avmAggregate.AssetCount,
			&avmAggregate.OutputCount)
		if err != nil {
			t.log.Error("row fetch %s", err)
			return time.Time{}, err
		}

		// aggregateTS would be update to the most recent timestamp we processed...
		// we use it later to prune old aggregates from the db.
		if avmAggregate.AggregateTS.After(aggregateTS) {
			aggregateTS = avmAggregate.AggregateTS
		}

		err = t.replaceAvmAggregate(ctx, sess, avmAggregate)
		if err != nil {
			t.log.Error("replace avm aggregate %s", err)
			return time.Time{}, err
		}
	}
	return aggregateTS, nil
}

func (t *ProducerTasker) processAvmOutputAddressesCounts(ctx context.Context, sess *dbr.Session, aggregateTS time.Time) error {
	var err error
	var rows *sql.Rows

	subquery := sess.Select("avm_output_addresses.address").
		Distinct().
		From("avm_output_addresses").
		Where("avm_output_addresses.created_at >= ?", aggregateTS)

	rows, err = sess.
		Select(
			"avm_output_addresses.address",
			"avm_outputs.asset_id",
			"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
			"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
			"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
			"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
			"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN 1 ELSE 0 END), 0) AS utxo_count",
		).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id").
		Where("avm_output_addresses.address IN ?", subquery).
		GroupBy("avm_output_addresses.address", "avm_outputs.asset_id").
		RowsContext(ctx)
	if err != nil {
		t.log.Error("error query %s", err)
		return err
	}
	if rows.Err() != nil {
		t.log.Error("error query %s", rows.Err())
		return rows.Err()
	}

	for ok := rows.Next(); ok; ok = rows.Next() {
		var avmAggregateCount models.AvmAggregateCount
		err = rows.Scan(&avmAggregateCount.Address,
			&avmAggregateCount.AssetID,
			&avmAggregateCount.TransactionCount,
			&avmAggregateCount.TotalReceived,
			&avmAggregateCount.TotalSent,
			&avmAggregateCount.Balance,
			&avmAggregateCount.UtxoCount)
		if err != nil {
			t.log.Error("row fetch %s", err)
			return err
		}

		err = t.replaceAvmAggregateCount(ctx, sess, avmAggregateCount)
		if err != nil {
			t.log.Error("replace avm aggregate count %s", err)
			return err
		}
	}
	return nil
}

func (t *ProducerTasker) replaceAvmAggregate(ctx context.Context, sess *dbr.Session, avmAggregates models.AvmAggregate) error {
	_, err := t.insertAvmAggregate(ctx, sess, avmAggregates)
	if db.ErrIsDuplicateEntryError(err) {
		_, err := t.updateAvmAggregate(ctx, sess, avmAggregates)
		// the update failed.  (could be truncation?)... Punt..
		if err != nil {
			return err
		}
	} else
	// the insert failed, not a duplicate.  (could be truncation?)... Punt..
	if err != nil {
		return err
	}
	return nil
}

func (t *ProducerTasker) replaceAvmAggregateCount(ctx context.Context, sess *dbr.Session, avmAggregates models.AvmAggregateCount) error {
	_, err := t.insertAvmAggregateCount(ctx, sess, avmAggregates)
	if db.ErrIsDuplicateEntryError(err) {
		_, err := t.updateAvmAggregateCount(ctx, sess, avmAggregates)
		// the update failed.  (could be truncation?)... Punt..
		if err != nil {
			return err
		}
	} else
	// the insert failed, not a duplicate.  (could be truncation?)... Punt..
	if err != nil {
		return err
	}
	return nil
}

func AvmOutputsAggregateCursor(ctx context.Context, sess *dbr.Session, aggregateTS time.Time) (*sql.Rows, error) {
	rows, err := sess.
		Select(aggregateColumns...).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		GroupBy("aggregate_ts", "avm_outputs.asset_id").
		Where("avm_outputs.created_at >= ?", aggregateTS).
		RowsContext(ctx)
	return rows, err
}

func (t *ProducerTasker) Start() {
	go initRefreshAggregatesTick(t)
}

func initRefreshAggregatesTick(t *ProducerTasker) {
	timer := time.NewTicker(aggregationTick)
	defer timer.Stop()

	_ = t.RefreshAggregates()
	for range timer.C {
		_ = t.RefreshAggregates()
	}
}
