package stream

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/ortelius/utils"

	avalancheGoUtils "github.com/ava-labs/avalanchego/utils"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/gocraft/dbr/v2"
)

var (
	stateContextDuration = time.Minute
	contextDuration      = time.Minute

	queryContextDuration = 10 * time.Minute

	aggregationTick      = 20 * time.Second
	aggregateDeleteFrame = (-1 * 24 * 366) * time.Hour

	// rollup all aggregates to 1 minute.
	timestampRollup = 1 * time.Minute

	// == timestampRollup.Seconds() but not a float...
	timestampRollupSecs = 60

	additionalHours = (365 * 24) * time.Hour
	blank           = models.AvmAssetAggregateState{}

	// queue for updates
	updateChannelSize = 10000

	// number of updaters
	updatesCount = 4
)

type ProducerTasker struct {
	initlock          sync.RWMutex
	connections       *services.Connections
	plock             sync.Mutex
	timeStampProducer func() time.Time

	// metrics
	metricSuccessCountKey        string
	metricFailureCountKey        string
	metricProcessMillisCountKey  string
	metricAssetAggregateCountKey string
	metricCountAggregateCountKey string
}

var producerTaskerInstance = ProducerTasker{
	timeStampProducer: time.Now,
}

func initializeConsumerTasker(conns *services.Connections) {
	producerTaskerInstance.initlock.Lock()
	defer producerTaskerInstance.initlock.Unlock()

	if producerTaskerInstance.connections != nil {
		return
	}

	producerTaskerInstance.connections = conns
	producerTaskerInstance.Start()
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
	// current_created_at is set to the newest aggregation timestamp from the kafka message queue.
	// and in the same update we reset created_at to a time in the future.
	// when we get new messages from the queue, they will execute the sql _after_ this update, and set created_at to an earlier date.
	updatedCurrentCreated := t.timeStampProducer().Add(additionalHours)
	_, err = sessTX.ExecContext(ctx, "update avm_asset_aggregation_state "+
		"set current_created_at=created_at, created_at=? "+
		"where id=?", updatedCurrentCreated, models.StateLiveID)
	if err != nil {
		t.connections.Logger().Error("atomic swap %s", err)
		return blank, err
	}

	backupAggregateState := liveAggregationState
	backupAggregateState.ID = models.StateBackupID

	// id=stateBackupId backup row - for crash recovery
	_, err = models.InsertAvmAssetAggregationState(ctx, sessTX, backupAggregateState)
	if db.ErrIsDuplicateEntryError(err) {
		var sqlResult sql.Result
		sqlResult, err = sessTX.Update("avm_asset_aggregation_state").
			Set("current_created_at", backupAggregateState.CurrentCreatedAt).
			Where("id=? and current_created_at > ?", backupAggregateState.ID, backupAggregateState.CurrentCreatedAt).
			ExecContext(ctx)
		if err != nil {
			t.connections.Logger().Error("update backup state %s", err.Error())
			return blank, err
		}

		var rowsAffected int64
		rowsAffected, err = sqlResult.RowsAffected()

		if err != nil {
			t.connections.Logger().Error("update backup state failed %s", err)
			return blank, err
		}

		if rowsAffected > 0 {
			// if we updated, refresh the backup state
			backupAggregateState, err = models.SelectAvmAssetAggregationState(ctx, sessTX, backupAggregateState.ID)
			if err != nil {
				t.connections.Logger().Error("refresh backup state %s", err)
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

type updateJob struct {
	avmAggregate      *models.AvmAggregate
	avmAggregateCount *models.AvmAggregateCount
}

func (t *ProducerTasker) RefreshAggregates() error {
	t.plock.Lock()
	defer t.plock.Unlock()

	liveAggregationState, backupAggregateState, err := t.fetchState()
	if err != nil {
		return err
	}

	// truncate to earliest minute.
	aggregateTS := liveAggregationState.CurrentCreatedAt.Truncate(timestampRollup)

	baseAggregateTS := aggregateTS

	aggregateTSUpdate, err := t.processAggregates(baseAggregateTS)
	if err != nil {
		return err
	}

	err = t.cleanupState(backupAggregateState)
	if err != nil {
		return err
	}

	if aggregateTSF, ok := aggregateTSUpdate.GetValue().(time.Time); ok {
		aggregateTS = aggregateTSF
	}

	t.connections.Logger().Info("processed up to %s", aggregateTS.String())

	return nil
}

func (t *ProducerTasker) processAggregates(baseAggregateTS time.Time) (*avalancheGoUtils.AtomicInterface, error) {
	errs := &avalancheGoUtils.AtomicInterface{}
	aggregateTSUpdate := &avalancheGoUtils.AtomicInterface{}

	pf := func(wn int, i interface{}) {
		update, ok := i.(*updateJob)
		if !ok {
			return
		}

		if update.avmAggregate != nil {
			if err := t.replaceAvmAggregate(*update.avmAggregate); err != nil {
				t.connections.Logger().Error("replace avm aggregate %s", err)
				errs.SetValue(err)
			} else {
				err := metrics.Prometheus.CounterInc(t.metricAssetAggregateCountKey)
				if err != nil {
					t.connections.Logger().Error("prometheus.CounterInc: %s", err)
				}
			}
		}

		if update.avmAggregateCount != nil {
			if err := t.replaceAvmAggregateCount(*update.avmAggregateCount); err != nil {
				t.connections.Logger().Error("replace avm aggregate count %s", err)
				errs.SetValue(err)
			} else {
				err := metrics.Prometheus.CounterInc(t.metricCountAggregateCountKey)
				if err != nil {
					t.connections.Logger().Error("prometheus.CounterInc: %s", err)
				}
			}
		}
	}
	worker := utils.NewWorker(updateChannelSize, updatesCount, pf)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		aggregateTSUpdateRes, err := t.processAvmOutputs(baseAggregateTS, worker, errs)
		if err != nil {
			errs.SetValue(err)
		}
		aggregateTSUpdate.SetValue(aggregateTSUpdateRes)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := t.processAvmOutputAddressesCounts(baseAggregateTS, worker, errs)
		if err != nil {
			errs.SetValue(err)
		}
	}()
	wg.Wait()
	worker.Finish(100 * time.Millisecond)

	if err, ok := errs.GetValue().(error); ok {
		return nil, err
	}

	return aggregateTSUpdate, nil
}

func (t *ProducerTasker) cleanupState(backupAggregateState models.AvmAssetAggregateState) error {
	ctx, cancel := context.WithTimeout(context.Background(), stateContextDuration)
	defer cancel()

	sess, err := t.connections.DB().NewSession("producertasker_cleanstate", stateContextDuration)
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

	return nil
}

func (t *ProducerTasker) fetchState() (models.AvmAssetAggregateState, models.AvmAssetAggregateState, error) {
	var liveAggregationState models.AvmAssetAggregateState
	var backupAggregateState models.AvmAssetAggregateState

	ctx, cancel := context.WithTimeout(context.Background(), stateContextDuration)
	defer cancel()

	sess, err := t.connections.DB().NewSession("producertasker_fetchstate", stateContextDuration)
	if err != nil {
		return liveAggregationState, backupAggregateState, err
	}

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
		t.connections.Logger().Error("unable to find live state")
		return liveAggregationState, backupAggregateState, err
	}

	// check if the backup row exists, if found we crashed from a previous run.
	backupAggregateState, _ = models.SelectAvmAssetAggregationState(ctx, sess, models.StateBackupID)

	if backupAggregateState.ID == uint64(models.StateBackupID) {
		// re-process from backup row..
		liveAggregationState = backupAggregateState
	} else {
		backupAggregateState, err = t.updateBackupState(ctx, sess, liveAggregationState)
		if err != nil {
			t.connections.Logger().Error("unable to update backup state %s", err)
			return liveAggregationState, backupAggregateState, err
		}
	}

	return liveAggregationState, backupAggregateState, nil
}

func (t *ProducerTasker) processAvmOutputs(aggregateTS time.Time, updateChannel utils.Worker, errs *avalancheGoUtils.AtomicInterface) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), queryContextDuration)
	defer cancel()

	sess, err := t.connections.DB().NewSession("producertasker_outputs", queryContextDuration)
	if err != nil {
		return time.Time{}, err
	}

	var rows *sql.Rows
	rows, err = sess.
		Select(fmt.Sprintf("FROM_UNIXTIME(floor(UNIX_TIMESTAMP(avm_outputs.created_at) / %d) * %d) as aggregate_ts", timestampRollupSecs, timestampRollupSecs),
			"avm_outputs.asset_id",
			"avm_outputs.chain_id",
			"CAST(COALESCE(SUM(avm_outputs.amount), 0) AS CHAR) AS transaction_volume",
			"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
			"COUNT(DISTINCT(avm_output_addresses.address)) AS address_count",
			"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
			"COUNT(avm_outputs.id) AS output_count").
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		GroupBy("aggregate_ts", "avm_outputs.chain_id", "avm_outputs.asset_id").
		Where("avm_outputs.created_at >= ?", aggregateTS).
		RowsContext(ctx)
	if err != nil {
		t.connections.Logger().Error("error query %s", err)
		return time.Time{}, err
	}
	if rows.Err() != nil {
		t.connections.Logger().Error("error query %s", rows.Err())
		return time.Time{}, rows.Err()
	}

	for ok := rows.Next(); ok && errs.GetValue() == nil; ok = rows.Next() {
		var avmAggregate models.AvmAggregate
		err = rows.Scan(&avmAggregate.AggregateTS,
			&avmAggregate.AssetID,
			&avmAggregate.ChainID,
			&avmAggregate.TransactionVolume,
			&avmAggregate.TransactionCount,
			&avmAggregate.AddressCount,
			&avmAggregate.AssetCount,
			&avmAggregate.OutputCount)
		if err != nil {
			t.connections.Logger().Error("row fetch %s", err)
			return time.Time{}, err
		}

		// aggregateTS would be update to the most recent timestamp we processed...
		// we use it later to prune old aggregates from the db.
		if avmAggregate.AggregateTS.After(aggregateTS) {
			aggregateTS = avmAggregate.AggregateTS
		}

		update := updateJob{
			avmAggregate: &avmAggregate,
		}
		updateChannel.Enque(&update)
	}
	return aggregateTS, nil
}

func (t *ProducerTasker) processAvmOutputAddressesCounts(aggregateTS time.Time, updateChannel utils.Worker, errs *avalancheGoUtils.AtomicInterface) error {
	ctx, cancel := context.WithTimeout(context.Background(), queryContextDuration)
	defer cancel()

	sess, err := t.connections.DB().NewSession("producertasker_addresscounts", queryContextDuration)
	if err != nil {
		return err
	}

	var rows *sql.Rows

	subquery := sess.Select("avm_output_addresses.address").
		Distinct().
		From("avm_output_addresses").
		Where("avm_output_addresses.created_at >= ?", aggregateTS)

	rows, err = sess.
		Select(
			"avm_output_addresses.address",
			"avm_outputs.asset_id",
			"avm_outputs.chain_id",
			"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
			"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
			"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
			"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
			"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN 1 ELSE 0 END), 0) AS utxo_count",
		).
		From("avm_outputs").
		LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		GroupBy("avm_output_addresses.address", "avm_outputs.chain_id", "avm_outputs.asset_id").
		Where("avm_output_addresses.address IN ?", subquery).
		RowsContext(ctx)
	if err != nil {
		t.connections.Logger().Error("error query %s", err)
		return err
	}
	if rows.Err() != nil {
		t.connections.Logger().Error("error query %s", rows.Err())
		return rows.Err()
	}

	for ok := rows.Next(); ok && errs.GetValue() == nil; ok = rows.Next() {
		var avmAggregateCount models.AvmAggregateCount
		err = rows.Scan(&avmAggregateCount.Address,
			&avmAggregateCount.AssetID,
			&avmAggregateCount.ChainID,
			&avmAggregateCount.TransactionCount,
			&avmAggregateCount.TotalReceived,
			&avmAggregateCount.TotalSent,
			&avmAggregateCount.Balance,
			&avmAggregateCount.UtxoCount)
		if err != nil {
			t.connections.Logger().Error("row fetch %s", err)
			return err
		}

		update := updateJob{
			avmAggregateCount: &avmAggregateCount,
		}
		updateChannel.Enque(&update)
	}

	return nil
}

func (t *ProducerTasker) replaceAvmAggregate(avmAggregates models.AvmAggregate) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextDuration)
	defer cancel()

	sess, err := t.connections.DB().NewSession("producertasker_aggregate", contextDuration)
	if err != nil {
		return err
	}

	_, err = models.InsertAvmAssetAggregation(ctx, sess, avmAggregates)
	if !(err != nil && db.ErrIsDuplicateEntryError(err)) {
		return err
	}
	_, err = models.UpdateAvmAssetAggregation(ctx, sess, avmAggregates)
	return err
}

func (t *ProducerTasker) replaceAvmAggregateCount(avmAggregatesCount models.AvmAggregateCount) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextDuration)
	defer cancel()

	sess, err := t.connections.DB().NewSession("producertasker_aggregate_count", contextDuration)
	if err != nil {
		return err
	}

	_, err = models.InsertAvmAssetAggregationCount(ctx, sess, avmAggregatesCount)
	if !(err != nil && db.ErrIsDuplicateEntryError(err)) {
		return err
	}
	_, err = models.UpdateAvmAssetAggregationCount(ctx, sess, avmAggregatesCount)
	return err
}

func (t *ProducerTasker) Start() {
	t.initMetrics()

	go initRefreshAggregatesTick(t)
}

func (t *ProducerTasker) initMetrics() {
	t.metricSuccessCountKey = "indexer_task_records_success"
	t.metricFailureCountKey = "indexer_task_records_failure"
	t.metricProcessMillisCountKey = "indexer_task_records_process_millis"
	t.metricAssetAggregateCountKey = "index_task_records_asset_aggregate_count"
	t.metricCountAggregateCountKey = "index_task_records_count_aggregate_count"

	metrics.Prometheus.CounterInit(t.metricAssetAggregateCountKey, "records asset aggregate count")
	metrics.Prometheus.CounterInit(t.metricCountAggregateCountKey, "records count aggregate count")
	metrics.Prometheus.CounterInit(t.metricSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(t.metricFailureCountKey, "records failed")
	metrics.Prometheus.CounterInit(t.metricProcessMillisCountKey, "records processed millis")
}

func initRefreshAggregatesTick(t *ProducerTasker) {
	timer := time.NewTicker(aggregationTick)
	defer timer.Stop()

	performRefresh(t)
	for range timer.C {
		performRefresh(t)
	}
}

func performRefresh(t *ProducerTasker) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(t.metricProcessMillisCountKey),
		metrics.NewSuccessFailCounterInc(t.metricSuccessCountKey, t.metricFailureCountKey),
	)
	defer func() {
		err := collectors.Collect()
		if err != nil {
			t.connections.Logger().Error("collectors.Collect: %s", err)
		}
	}()

	err := t.RefreshAggregates()
	if err != nil {
		collectors.Error()
		t.connections.Logger().Error("Refresh Aggregates %s", err)
	}
}
