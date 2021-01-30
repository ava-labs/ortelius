package services

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/gocraft/dbr/v2"
)

var RowLimitValueBase = 1000
var RowLimitValue = uint64(RowLimitValueBase)

var updTimeout = 10 * time.Minute

type processType uint32

var processTypeIn processType = 1
var processTypeOut processType = 2

type BalanceAccumulatorManager struct {
	handlers []*BalancerAccumulateHandler
}

func (a *BalanceAccumulatorManager) Run(persist Persist, sc *Control, _ *Connections) {
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
			err = a.accumulateOutputOuts(conns, persist)
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
			err = a.accumulateOutputIns(conns, persist)
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

func (a *BalancerAccumulateHandler) accumulateOutputOuts(conns *Connections, persist Persist) error {
	for {
		cnt, err := a.processOutputs(processTypeOut, conns, persist)
		if err != nil {
			return err
		}
		if cnt == 0 {
			break
		}
	}

	return nil
}

func (a *BalancerAccumulateHandler) accumulateOutputIns(conns *Connections, persist Persist) error {
	for {
		cnt, err := a.processOutputs(processTypeIn, conns, persist)
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

func (a *BalancerAccumulateHandler) processOutputsPre(typ processType, session *dbr.Session) ([]*OutputAddressAccumulateWithTrID, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var rowdata []*OutputAddressAccumulateWithTrID
	var err error

	b := session.Select(
		"output_addresses_accumulate.id",
		"output_addresses_accumulate.output_id",
		"output_addresses_accumulate.address",
		"avm_outputs.transaction_id",
	).
		From("output_addresses_accumulate").
		Join("avm_outputs", "output_addresses_accumulate.output_id = avm_outputs.id")

	switch typ {
	case processTypeOut:
		b = b.
			Where("output_addresses_accumulate.processed_out = ?", 0).
			OrderAsc("output_addresses_accumulate.processed_out")
	case processTypeIn:
		b = b.
			Join("avm_outputs_redeeming", "output_addresses_accumulate.output_id = avm_outputs_redeeming.id ").
			Where("output_addresses_accumulate.processed_in = ?", 0).
			OrderAsc("output_addresses_accumulate.processed_in")
	}

	_, err = b.
		OrderAsc("output_addresses_accumulate.created_at").
		Limit(RowLimitValue).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return nil, err
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(rowdata), func(i, j int) { rowdata[i], rowdata[j] = rowdata[j], rowdata[i] })

	return rowdata, nil
}

func (a *BalancerAccumulateHandler) processOutputs(typ processType, conns *Connections, persist Persist) (int, error) {
	job := conns.Stream().NewJob("accumulate")
	session := conns.DB().NewSessionForEventReceiver(job)

	var err error
	var rowdataAvail []*OutputAddressAccumulateWithTrID
	rowdataAvail, err = a.processOutputsPre(typ, session)
	if err != nil {
		return 0, err
	}

	if len(rowdataAvail) > 0 {
		trowsID := make([]string, 0, 5)
		trows := make(map[string]*OutputAddressAccumulateWithTrID)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > 5 {
				err = a.processOutputsPost(trows, trowsID, typ, session, persist)
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*OutputAddressAccumulateWithTrID)
				trowsID = make([]string, 0, 5)
			}
		}

		if len(trowsID) > 0 {
			err = a.processOutputsPost(trows, trowsID, typ, session, persist)
			if err != nil {
				return 0, err
			}
		}
	}

	return len(rowdataAvail), nil
}

func (a *BalancerAccumulateHandler) processOutputsPost(
	workRows map[string]*OutputAddressAccumulateWithTrID,
	workRowsID []string,
	typ processType,
	session *dbr.Session,
	persist Persist,
) error {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var dbTx *dbr.Tx
	dbTx, err = session.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	var rowdata []*OutputAddressAccumulateWithTrID

	b := dbTx.Select(
		"id",
	).
		From("output_addresses_accumulate")

	switch typ {
	case processTypeOut:
		b = b.Where("processed_out = ? and id in ?", 0, workRowsID)
	case processTypeIn:
		b = b.Where("processed_in = ? and id in ?", 0, workRowsID)
	}

	_, err = b.
		Suffix("for update").
		LoadContext(ctx, &rowdata)
	if err != nil {
		return err
	}

	if len(rowdata) == 0 {
		return nil
	}

	for _, row := range rowdata {
		err := a.processOutputsBase(ctx, typ, dbTx, persist, workRows[row.ID])
		if err != nil {
			return err
		}
	}

	return dbTx.Commit()
}

func (a *BalancerAccumulateHandler) processOutputsBase(
	ctx context.Context,
	typ processType,
	dbTx *dbr.Tx,
	persist Persist,
	row *OutputAddressAccumulateWithTrID,
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
		Where("id=?", row.ID).
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
		Where("avm_outputs.id=? and avm_output_addresses.address=?", row.OutputID, row.Address).
		GroupBy("chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
		LoadContext(ctx, &balances)
	if err != nil {
		return err
	}

	if len(balances) == 0 {
		return fmt.Errorf("invalid balance")
	}

	for _, b := range balances {
		// add any missing transaction rows.
		outputsTxsAccumulate := &OutputTxsAccumulate{
			ChainID:       b.ChainID,
			AssetID:       b.AssetID,
			Address:       b.Address,
			TransactionID: row.TransactionID,
			CreatedAt:     time.Now(),
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
			return fmt.Errorf("balancesLocked failed")
		}
		brow := balancesLocked[0]
		if brow.ID != b.ID {
			return fmt.Errorf("balancesLocked failed")
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

func (a *BalancerAccumulateHandler) processTransactionsPre(session *dbr.Session) ([]*OutputTxsAccumulate, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var rowdata []*OutputTxsAccumulate

	_, err = session.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"transaction_id",
		"processed",
	).
		From("output_txs_accumulate").
		Where("processed = ?", 0).
		OrderAsc("processed").
		OrderAsc("created_at").
		Limit(RowLimitValue).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return nil, err
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(rowdata), func(i, j int) { rowdata[i], rowdata[j] = rowdata[j], rowdata[i] })

	return rowdata, nil
}

func (a *BalancerAccumulateHandler) processTransactions(conns *Connections, persist Persist) (int, error) {
	job := conns.Stream().NewJob("accumulate")
	session := conns.DB().NewSessionForEventReceiver(job)

	var err error
	var rowdataAvail []*OutputTxsAccumulate
	rowdataAvail, err = a.processTransactionsPre(session)
	if err != nil {
		return 0, err
	}
	if len(rowdataAvail) > 0 {
		trowsID := make([]string, 0, 5)
		trows := make(map[string]*OutputTxsAccumulate)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > 5 {
				err = a.processTransactionsPost(trows, trowsID, session, persist)
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*OutputTxsAccumulate)
				trowsID = make([]string, 0, 5)
			}
		}

		if len(trowsID) > 0 {
			err = a.processTransactionsPost(trows, trowsID, session, persist)
			if err != nil {
				return 0, err
			}
		}
	}

	return len(rowdataAvail), nil
}

func (a *BalancerAccumulateHandler) processTransactionsPost(
	workRows map[string]*OutputTxsAccumulate,
	workRowsID []string,
	session *dbr.Session,
	persist Persist,
) error {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var dbTx *dbr.Tx
	dbTx, err = session.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	var rowdata []*OutputTxsAccumulate

	_, err = dbTx.Select(
		"id",
	).
		From("output_txs_accumulate").
		Where("processed = ? and id in ?", 0, workRowsID).
		Suffix("for update").
		LoadContext(ctx, &rowdata)
	if err != nil {
		return err
	}

	if len(rowdata) == 0 {
		return nil
	}

	for _, row := range rowdata {
		err := a.processTransactionsBase(ctx, dbTx, persist, workRows[row.ID])
		if err != nil {
			return err
		}
	}

	return dbTx.Commit()
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
		return fmt.Errorf("balancesLocked failed")
	}
	brow := balancesLocked[0]
	if brow.ID != b.ID {
		return fmt.Errorf("balancesLocked failed")
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
