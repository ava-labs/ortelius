package services

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/gocraft/dbr/v2"
)

var RowLimitValueBase = 5
var RowLimitValue = uint64(RowLimitValueBase)
var updTimeout = 10 * time.Minute

type processType uint32

var processTypeIn processType = 1
var processTypeOut processType = 2

type BalanceAccumulatorManager struct {
	handlers []*BalancerAccumulateHandler
}

func (a *BalanceAccumulatorManager) Run(persist Persist, sc *Control, conns *Connections) {
	for _, h := range a.handlers {
		h.Run(persist, sc)
	}
}

type BalancerAccumulateHandler struct {
	runningOutputOuts   int64
	runningOutputIns    int64
	runningTransactions int64
	lock                sync.Mutex
}

func (a *BalancerAccumulateHandler) Run(persist Persist, sc *Control) {
	a.runOutputsOuts(persist, sc)
	a.runOutputsIns(persist, sc)
	a.runTransactions(persist, sc)
}

func (a *BalancerAccumulateHandler) runOutputsOuts(persist Persist, sc *Control) {
	if atomic.LoadInt64(&a.runningOutputOuts) != 0 {
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	if atomic.LoadInt64(&a.runningOutputOuts) != 0 {
		return
	}

	atomic.AddInt64(&a.runningOutputOuts, 1)
	go func() {
		defer func() {
			atomic.AddInt64(&a.runningOutputOuts, -1)
		}()

		conns, err := sc.DatabaseOnly()
		if err != nil {
			sc.Log.Warn("Accumulate conns create %v", err)
			return
		}
		defer func() {
			err := conns.Close()
			if err != nil {
				sc.Log.Warn("Accumulate conns close %v", err)
			}
		}()

		for {
			err = a.accumulateOutputOuts(conns, persist, sc)
			if !db.ErrIsLockError(err) {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		if err != nil {
			sc.Log.Warn("Accumulate %v", err)
		}
	}()
}

func (a *BalancerAccumulateHandler) runOutputsIns(persist Persist, sc *Control) {
	if atomic.LoadInt64(&a.runningOutputIns) != 0 {
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	if atomic.LoadInt64(&a.runningOutputIns) != 0 {
		return
	}

	atomic.AddInt64(&a.runningOutputIns, 1)
	go func() {
		defer func() {
			atomic.AddInt64(&a.runningOutputIns, -1)
		}()

		conns, err := sc.DatabaseOnly()
		if err != nil {
			sc.Log.Warn("Accumulate conns create %v", err)
			return
		}
		defer func() {
			err := conns.Close()
			if err != nil {
				sc.Log.Warn("Accumulate conns close %v", err)
			}
		}()

		for {
			err = a.accumulateOutputIns(conns, persist, sc)
			if !db.ErrIsLockError(err) {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		if err != nil {
			sc.Log.Warn("Accumulate %v", err)
		}
	}()
}

func (a *BalancerAccumulateHandler) runTransactions(persist Persist, sc *Control) {
	if atomic.LoadInt64(&a.runningTransactions) != 0 {
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	if atomic.LoadInt64(&a.runningTransactions) != 0 {
		return
	}

	atomic.AddInt64(&a.runningTransactions, 1)
	go func() {
		defer func() {
			atomic.AddInt64(&a.runningTransactions, -1)
		}()

		conns, err := sc.DatabaseOnly()
		if err != nil {
			sc.Log.Warn("Accumulate conns create %v", err)
			return
		}
		defer func() {
			err := conns.Close()
			if err != nil {
				sc.Log.Warn("Accumulate conns close %v", err)
			}
		}()

		for {
			err = a.accumulateTranactions(conns, persist)
			if !db.ErrIsLockError(err) {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		if err != nil {
			sc.Log.Warn("Accumulate %v", err)
		}
	}()
}

func (a *BalancerAccumulateHandler) accumulateOutputOuts(conns *Connections, persist Persist, sc *Control) error {
	for {
		cnt, err := a.processOutputs(processTypeOut, conns, persist, sc)
		if err != nil {
			return err
		}
		if cnt == 0 {
			break
		}
	}

	return nil
}

func (a *BalancerAccumulateHandler) accumulateOutputIns(conns *Connections, persist Persist, sc *Control) error {
	for {
		cnt, err := a.processOutputs(processTypeIn, conns, persist, sc)
		if err != nil {
			return err
		}
		if cnt == 0 {
			break
		}
	}

	return nil
}

func (a *BalancerAccumulateHandler) accumulateTranactions(conns *Connections, persist Persist) error {
	for {
		cnt, err := a.processTransactions(conns, persist)
		if err != nil {
			return err
		}
		if cnt == 0 {
			break
		}
	}

	return nil
}

type OutputAddressAccumulateWithTrID struct {
	OutputAddressAccumulate
	TransactionID string
}

func (a *BalancerAccumulateHandler) processOutputs(typ processType, conns *Connections, persist Persist, sc *Control) (int, error) {
	job := conns.Stream().NewJob("accumulate")
	session := conns.DB().NewSessionForEventReceiver(job)

	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var dbTx *dbr.Tx
	dbTx, err = session.Begin()
	if err != nil {
		return 0, err
	}
	defer dbTx.RollbackUnlessCommitted()

	var rowdata []*OutputAddressAccumulateWithTrID

	switch typ {
	case processTypeOut:
		_, err = dbTx.Select(
			"output_addresses_accumulate.id",
			"output_addresses_accumulate.address",
			"avm_outputs.transaction_id",
		).
			From("output_addresses_accumulate").
			Join("avm_outputs", "output_addresses_accumulate.id = avm_outputs.id").
			Where("output_addresses_accumulate.processed_out = ?", 0).
			Limit(RowLimitValue).
			Suffix("for update").
			LoadContext(ctx, &rowdata)
		if err != nil {
			return 0, err
		}
	case processTypeIn:
		_, err = dbTx.Select(
			"output_addresses_accumulate.id",
			"output_addresses_accumulate.address",
			"avm_outputs.transaction_id",
		).
			From("output_addresses_accumulate").
			Join("avm_outputs", "output_addresses_accumulate.id = avm_outputs.id").
			Join("avm_outputs_redeeming", "output_addresses_accumulate.id = avm_outputs_redeeming.id ").
			Where("output_addresses_accumulate.processed_in = ?", 0).
			Limit(RowLimitValue).
			Suffix("for update").
			LoadContext(ctx, &rowdata)
		if err != nil {
			return 0, err
		}
	}

	if len(rowdata) == 0 {
		return 0, nil
	}

	for _, row := range rowdata {
		err := a.processOutputsBase(ctx, typ, dbTx, persist, row, sc)
		if err != nil {
			return 0, err
		}
	}

	return len(rowdata), dbTx.Commit()
}

func (a *BalancerAccumulateHandler) processOutputsBase(
	ctx context.Context,
	typ processType,
	dbTx *dbr.Tx,
	persist Persist,
	row *OutputAddressAccumulateWithTrID,
	sc *Control,
) error {
	var err error

	upd := ""
	switch typ {
	case processTypeOut:
		upd = "processed_out"
	case processTypeIn:
		upd = "processed_in"
	}

	_, err = dbTx.Update("output_addresses_accumulate").
		Set(upd, 1).
		Where("id=? and address=?", row.ID, row.Address).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	var balances []*AccumulateBalances

	_, err = dbTx.Select(
		"avm_outputs.chain_id",
		"avm_output_addresses.address",
		"avm_outputs.asset_id",
		"count(distinct(avm_outputs.transaction_id)) as transaction_count",
		"sum(avm_outputs.amount) as total_received",
		"sum(avm_outputs.amount) as total_sent",
	).From("avm_outputs").
		Join("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
		Where("avm_outputs.id=? and avm_output_addresses.address=?", row.ID, row.Address).
		GroupBy("chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
		LoadContext(ctx, &balances)
	if err != nil {
		return err
	}

	if len(balances) == 0 {
		sc.Log.Info("invalid balance %s %s on %d", row.ID, row.Address, typ)
		return nil
	}

	for _, b := range balances {
		// add any missing transaction rows.
		outputsTxsAccumulate := &OutputTxsAccumulate{
			ChainID:       b.ChainID,
			AssetID:       b.AssetID,
			Address:       b.Address,
			TransactionID: row.TransactionID,
		}
		err = outputsTxsAccumulate.ComputeID()
		if err != nil {
			return err
		}
		err = persist.InsertOutputTxsAccumulate(ctx, dbTx, outputsTxsAccumulate)
		if err != nil {
			return err
		}

		err = b.ComputeID()
		if err != nil {
			return err
		}

		err = persist.InsertAccumulateBalances(ctx, dbTx, b)
		if err != nil {
			return err
		}

		var balancesLocked []*AccumulateBalances
		_, err = dbTx.Select("id").
			From("accumulate_balances").
			Where("id = ?", b.ID).
			Suffix("for update").
			LoadContext(ctx, &balancesLocked)
		if err != nil {
			return err
		}
		// we didn't lock the row
		if len(balancesLocked) == 0 {
			return nil
		}
		brow := balancesLocked[0]
		if brow.ID != b.ID {
			return nil
		}

		switch typ {
		case processTypeOut:
			_, err = dbTx.UpdateBySql("update accumulate_balances "+
				"set "+
				"utxo_count = utxo_count+1, "+
				"total_received = total_received+"+b.TotalReceived+" "+
				"where id=? "+
				"", b.ID).
				ExecContext(ctx)
			if err != nil {
				return err
			}
		case processTypeIn:
			_, err = dbTx.UpdateBySql("update accumulate_balances "+
				"set "+
				"utxo_count = utxo_count-1, "+
				"total_sent = total_sent+"+b.TotalSent+" "+
				"where id=? "+
				"", b.ID).
				ExecContext(ctx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *BalancerAccumulateHandler) processTransactions(conns *Connections, persist Persist) (int, error) {
	job := conns.Stream().NewJob("accumulate")
	session := conns.DB().NewSessionForEventReceiver(job)

	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var dbTx *dbr.Tx
	dbTx, err = session.Begin()
	if err != nil {
		return 0, err
	}
	defer dbTx.RollbackUnlessCommitted()

	var rowdata []*OutputTxsAccumulate

	_, err = dbTx.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"transaction_id",
		"processed",
	).
		From("output_txs_accumulate").
		Where("processed = ?", 0).
		Limit(RowLimitValue).
		Suffix("for update").
		LoadContext(ctx, &rowdata)
	if err != nil {
		return 0, err
	}

	if len(rowdata) == 0 {
		return 0, nil
	}

	for _, row := range rowdata {
		err := a.processTransactionsBase(ctx, dbTx, persist, row)
		if err != nil {
			return 0, err
		}
	}

	return len(rowdata), dbTx.Commit()
}

func (a *BalancerAccumulateHandler) processTransactionsBase(
	ctx context.Context,
	dbTx *dbr.Tx,
	persist Persist,
	row *OutputTxsAccumulate,
) error {
	var err error

	_, err = dbTx.Update("output_txs_accumulate").
		Set("processed", 1).
		Where("id = ?", row.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	b := &AccumulateBalances{
		ChainID: row.ChainID,
		AssetID: row.AssetID,
		Address: row.Address,
	}
	err = b.ComputeID()
	if err != nil {
		return err
	}
	err = persist.InsertAccumulateBalances(ctx, dbTx, b)
	if err != nil {
		return err
	}

	var balancesLocked []*AccumulateBalances
	_, err = dbTx.Select("id").
		From("accumulate_balances").
		Where("id = ?", b.ID).
		Suffix("for update").
		LoadContext(ctx, &balancesLocked)
	if err != nil {
		return err
	}

	// we didn't lock the row
	if len(balancesLocked) == 0 {
		return nil
	}
	brow := balancesLocked[0]
	if brow.ID != b.ID {
		return nil
	}

	_, err = dbTx.UpdateBySql("update accumulate_balances "+
		"set "+
		"transaction_count = transaction_count+1 "+
		"where id=? "+
		"", b.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	return nil
}
