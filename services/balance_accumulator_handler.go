package services

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/gocraft/dbr/v2"
)

var RowLimitValueBase = 1000
var RowLimitValue = uint64(RowLimitValueBase)
var LockSize = 5
var Threads = int64(4)

var updTimeout = 10 * time.Minute

type processType uint32

var processTypeIn processType = 1
var processTypeOut processType = 2

type BalanceAccumulatorManager struct {
	handler BalancerAccumulateHandler
}

func (a *BalanceAccumulatorManager) Run(persist Persist, sc *Control, conns *Connections) {
	a.handler.Run(persist, sc, conns)
}

type BalancerAccumulateHandler struct {
	running int64
	lock    sync.Mutex
}

func (a *BalancerAccumulateHandler) Run(persist Persist, sc *Control, _ *Connections) {
	if atomic.LoadInt64(&a.running) > Threads {
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	if atomic.LoadInt64(&a.running) > Threads {
		return
	}

	atomic.AddInt64(&a.running, 1)

	go func() {
		var conns *Connections
		var err error
		defer func() {
			atomic.AddInt64(&a.running, -1)

			if conns != nil {
				_ = conns.Close()
			}
		}()

		conns, err = sc.DatabaseOnly()
		if err != nil {
			return
		}
		for icnt := 0; icnt < 10; icnt++ {
			_, err = a.runInternal(sc, conns, persist)
			if err != nil {
				sc.Log.Info("accumulate balance error %v", err)
			}
		}
	}()
}

func (a *BalancerAccumulateHandler) runInternal(_ *Control, conns *Connections, persist Persist) (uint64, error) {
	errs := avlancheGoUtils.AtomicInterface{}
	var totalcnt uint64

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		var cnt uint64
		for {
			cnt, err = a.accumulateOutputOuts(conns, persist)
			if err == nil {
				atomic.AddUint64(&totalcnt, cnt)
				break
			}
			if !db.ErrIsLockError(err) {
				break
			}
		}
		if err != nil {
			errs.SetValue(err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		var cnt uint64
		for {
			cnt, err = a.accumulateOutputIns(conns, persist)
			if err == nil {
				atomic.AddUint64(&totalcnt, cnt)
				break
			}
			if !db.ErrIsLockError(err) {
				break
			}
		}
		if err != nil {
			errs.SetValue(err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		var cnt uint64
		for {
			cnt, err = a.accumulateTranactions(conns, persist)
			if err == nil {
				atomic.AddUint64(&totalcnt, cnt)
				break
			}
			if !db.ErrIsLockError(err) {
				break
			}
		}
		if err != nil {
			errs.SetValue(err)
		}
	}()
	wg.Wait()

	if errs.GetValue() != nil {
		return totalcnt, errs.GetValue().(error)
	}

	return totalcnt, nil
}

func (a *BalancerAccumulateHandler) accumulateOutputOuts(conns *Connections, persist Persist) (uint64, error) {
	var totalcnt uint64
	for {
		cnt, err := a.processOutputs(processTypeOut, conns, persist)
		if err != nil {
			return totalcnt, err
		}
		totalcnt += uint64(cnt)
		if cnt < RowLimitValueBase {
			break
		}
	}

	return totalcnt, nil
}

func (a *BalancerAccumulateHandler) accumulateOutputIns(conns *Connections, persist Persist) (uint64, error) {
	var totalcnt uint64
	for {
		cnt, err := a.processOutputs(processTypeIn, conns, persist)
		if err != nil {
			return totalcnt, err
		}
		totalcnt += uint64(cnt)
		if cnt < RowLimitValueBase {
			break
		}
	}

	return totalcnt, nil
}

func (a *BalancerAccumulateHandler) accumulateTranactions(conns *Connections, persist Persist) (uint64, error) {
	var totalcnt uint64
	for {
		cnt, err := a.processTransactions(conns, persist)
		if err != nil {
			return totalcnt, err
		}
		totalcnt += uint64(cnt)
		if cnt < RowLimitValueBase {
			break
		}
	}

	return totalcnt, nil
}

type OutputAddressAccumulateWithTrID struct {
	OutputAddressAccumulate
	TransactionID string
}

func outTable(typ processType) string {
	tbl := ""
	switch typ {
	case processTypeOut:
		tbl = TableOutputAddressAccumulateOut
	case processTypeIn:
		tbl = TableOutputAddressAccumulateIn
	}
	return tbl
}

func balanceTable(typ processType) string {
	tbl := ""
	switch typ {
	case processTypeOut:
		tbl = TableAccumulateBalancesReceived
	case processTypeIn:
		tbl = TableAccumulateBalancesSent
	}
	return tbl
}

func (a *BalancerAccumulateHandler) processOutputsPre(typ processType, session *dbr.Session) ([]*OutputAddressAccumulateWithTrID, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var rowdata []*OutputAddressAccumulateWithTrID
	var err error

	tbl := outTable(typ)

	b := session.Select(
		tbl+".id",
		tbl+".output_id",
		tbl+".address",
		"avm_outputs.transaction_id",
	).
		From(tbl).
		Join("avm_outputs", tbl+".output_id = avm_outputs.id")

	switch typ {
	case processTypeOut:
	case processTypeIn:
		b = b.
			Join("avm_outputs_redeeming", tbl+".output_id = avm_outputs_redeeming.id ")
	}

	_, err = b.
		Where(tbl+".processed = ?", 0).
		OrderAsc(tbl+".processed").
		OrderAsc(tbl+".created_at").
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
		trowsID := make([]string, 0, LockSize)
		trows := make(map[string]*OutputAddressAccumulateWithTrID)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > LockSize {
				err = a.processOutputsPost(trows, trowsID, typ, session, persist)
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*OutputAddressAccumulateWithTrID)
				trowsID = make([]string, 0, LockSize)
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

	tbl := outTable(typ)

	var err error
	var dbTx *dbr.Tx
	dbTx, err = session.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	var rowdata []*OutputAddressAccumulateWithTrID

	_, err = dbTx.Select(
		"id",
	).
		From(tbl).
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

	tbl := outTable(typ)
	balancetbl := balanceTable(typ)

	_, err = dbTx.Update(tbl).
		Set("processed", 1).
		Where("id=?", row.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	var balances []*AccumulateBalancesAmount

	_, err = dbTx.Select(
		"avm_outputs.chain_id",
		"avm_output_addresses.address",
		"avm_outputs.asset_id",
		"sum(avm_outputs.amount) as total_amount",
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
		// add any missing txs rows.
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

		b.UpdatedAt = time.Unix(1, 0)
		err = b.ComputeID()
		if err != nil {
			return err
		}

		switch typ {
		case processTypeOut:
			err = persist.InsertAccumulateBalancesReceived(ctx, dbTx, b)
		case processTypeIn:
			err = persist.InsertAccumulateBalancesSent(ctx, dbTx, b)
		}
		if err != nil {
			return err
		}

		var balancesLocked []*AccumulateBalancesAmount

		_, err = dbTx.Select("id").
			From(balancetbl).
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

		_, err = dbTx.UpdateBySql("update "+balancetbl+" "+
			"set "+
			"utxo_count = utxo_count+1, "+
			"total_amount = total_amount+"+b.TotalAmount+", "+
			"updated_at = ? "+
			"where id=? "+
			"", time.Now().UTC(), b.ID).
			ExecContext(ctx)
		if err != nil {
			return err
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
		From(TableOutputTxsAccumulate).
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
		trowsID := make([]string, 0, LockSize)
		trows := make(map[string]*OutputTxsAccumulate)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > LockSize {
				err = a.processTransactionsPost(trows, trowsID, session, persist)
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*OutputTxsAccumulate)
				trowsID = make([]string, 0, LockSize)
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
		From(TableOutputTxsAccumulate).
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

	_, err = dbTx.Update(TableOutputTxsAccumulate).
		Set("processed", 1).
		Where("id = ?", row.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	b := &AccumulateBalancesTransactions{
		ChainID:   row.ChainID,
		AssetID:   row.AssetID,
		Address:   row.Address,
		UpdatedAt: time.Unix(1, 0),
	}
	err = b.ComputeID()
	if err != nil {
		return err
	}
	err = persist.InsertAccumulateBalancesTransactions(ctx, dbTx, b)
	if err != nil {
		return err
	}

	var balancesLocked []*AccumulateBalancesTransactions
	_, err = dbTx.Select("id").
		From(TableAccumulateBalancesTransactions).
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

	_, err = dbTx.UpdateBySql("update "+TableAccumulateBalancesTransactions+" "+
		"set "+
		"transaction_count = transaction_count+1, "+
		"updated_at = ? "+
		"where id=? "+
		"", time.Now().UTC(), b.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	return nil
}
