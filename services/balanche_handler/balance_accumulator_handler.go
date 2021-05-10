package balanche_handler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ava-labs/ortelius/services/idb"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/utils/controlwrap"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/gocraft/dbr/v2"
)

var RowLimitValueBase = 5000
var RowLimitValue = uint64(RowLimitValueBase)
var LockSize = 5

var retryProcessing = 3

var updTimeout = 1 * time.Minute

type processType uint32

var processTypeIn processType = 1
var processTypeOut processType = 2

type managerChannels struct {
	ins  chan struct{}
	outs chan struct{}
	txs  chan struct{}
}

func newManagerChannels() *managerChannels {
	return &managerChannels{
		ins:  make(chan struct{}, 1),
		outs: make(chan struct{}, 1),
		txs:  make(chan struct{}, 1),
	}
}

type BalanceAccumulatorManager struct {
	handler *BalancerAccumulateHandler
	sc      controlwrap.ControlWrap
	persist idb.Persist

	managerChannelsList []*managerChannels

	doneCh chan struct{}
}

func NewBalanceAccumulatorManager(persist idb.Persist, sc controlwrap.ControlWrap) (*BalanceAccumulatorManager, error) {
	bmanager := &BalanceAccumulatorManager{
		handler: &BalancerAccumulateHandler{},
		sc:      sc,
		persist: persist,
		doneCh:  make(chan struct{}),
	}
	return bmanager, nil
}

func (a *BalanceAccumulatorManager) Close() {
	close(a.doneCh)
}

func (a *BalanceAccumulatorManager) Start() error {
	processingFunc := func(id string, managerChannel *managerChannels) error {
		connsOuts, err := a.sc.DatabaseOnly()
		if err != nil {
			return err
		}
		a.runProcessing("out_"+id, connsOuts, func(conns *servicesconn.Connections) (uint64, error) {
			return a.handler.processOutputs(true, processTypeOut, conns, a.persist)
		}, managerChannel.ins)

		connsIns, err := a.sc.DatabaseOnly()
		if err != nil {
			return err
		}
		a.runProcessing("in_"+id, connsIns, func(conns *servicesconn.Connections) (uint64, error) {
			return a.handler.processOutputs(true, processTypeIn, conns, a.persist)
		}, managerChannel.outs)

		connsTxs, err := a.sc.DatabaseOnly()
		if err != nil {
			return err
		}
		a.runProcessing("tx_"+id, connsTxs, func(conns *servicesconn.Connections) (uint64, error) {
			return a.handler.processTransactions(conns, a.persist)
		}, managerChannel.txs)
		return nil
	}

	for ipos := 0; ipos < 2; ipos++ {
		managerChannels := newManagerChannels()
		a.managerChannelsList = append(a.managerChannelsList, managerChannels)
		err := processingFunc(fmt.Sprintf("%d", ipos), managerChannels)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *BalanceAccumulatorManager) runProcessing(id string, conns *servicesconn.Connections, f func(conns *servicesconn.Connections) (uint64, error), trigger chan struct{}) {
	a.sc.Logger().Info("start processing %v", id)
	go func() {
		runEvent := func(conns *servicesconn.Connections) {
			icnt := 0
			for ; icnt < retryProcessing; icnt++ {
				cnt, err := f(conns)
				if db.ErrIsLockError(err) {
					icnt = 0
					continue
				}
				if err != nil {
					a.sc.Logger().Error("accumulate error %s %v", id, err)
					return
				}
				if cnt < RowLimitValue {
					break
				}
				icnt = 0
			}
		}

		defer func() {
			err := conns.Close()
			if err != nil {
				a.sc.Logger().Warn("connection close %v", err)
			}
			a.sc.Logger().Info("stop processing %v", id)
		}()

		for {
			select {
			case <-trigger:
				runEvent(conns)
			case <-a.doneCh:
				return
			}
		}
	}()
}

func (a *BalanceAccumulatorManager) Run() {
	for _, managerChannels := range a.managerChannelsList {
		select {
		case managerChannels.ins <- struct{}{}:
		default:
		}
		select {
		case managerChannels.outs <- struct{}{}:
		default:
		}
		select {
		case managerChannels.txs <- struct{}{}:
		default:
		}
	}
}

type BalancerAccumulateHandler struct {
}

func outTable(typ processType) string {
	tbl := ""
	switch typ {
	case processTypeOut:
		tbl = idb.TableOutputAddressAccumulateOut
	case processTypeIn:
		tbl = idb.TableOutputAddressAccumulateIn
	}
	return tbl
}

func balanceTable(typ processType) string {
	tbl := ""
	switch typ {
	case processTypeOut:
		tbl = idb.TableAccumulateBalancesReceived
	case processTypeIn:
		tbl = idb.TableAccumulateBalancesSent
	}
	return tbl
}

func (a *BalancerAccumulateHandler) processOutputsPre(outputProcessed bool, typ processType, session *dbr.Session) ([]*idb.OutputAddressAccumulate, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var rowdata []*idb.OutputAddressAccumulate
	var err error

	tbl := outTable(typ)

	b := session.Select(
		tbl+".id",
		tbl+".output_id",
		tbl+".address",
	).
		From(tbl).
		Join("avm_outputs", tbl+".output_id = avm_outputs.id")

	b.Where(tbl+".processed = ?", 0)

	switch typ {
	case processTypeOut:
	case processTypeIn:
		if outputProcessed {
			b = b.
				Where(tbl+".output_processed = ?", 1).
				OrderAsc(tbl + ".output_processed")
		} else {
			b = b.
				Join("avm_outputs_redeeming", tbl+".output_id = avm_outputs_redeeming.id ")
		}
	}

	sb := b.
		OrderAsc(tbl + ".processed").
		OrderAsc(tbl + ".created_at").
		Limit(RowLimitValue)

	scq := session.Select(
		"a.id",
		"a.output_id",
		"a.address",
		"avm_outputs.transaction_id",
	).From(sb.As("a")).
		Join("avm_outputs", "a.output_id = avm_outputs.id")

	switch typ {
	case processTypeOut:
	case processTypeIn:
		scq = scq.
			Join("avm_outputs_redeeming", "a.output_id = avm_outputs_redeeming.id ")
	}

	_, err = scq.
		LoadContext(ctx, &rowdata)
	if err != nil {
		return nil, err
	}

	rand.Shuffle(len(rowdata), func(i, j int) { rowdata[i], rowdata[j] = rowdata[j], rowdata[i] })

	return rowdata, nil
}

func (a *BalancerAccumulateHandler) processOutputs(outputProcessed bool, typ processType, conns *servicesconn.Connections, persist idb.Persist) (uint64, error) {
	job := conns.QuietStream().NewJob("accumulate-poll")
	session := conns.DB().NewSessionForEventReceiver(job)

	var err error
	var rowdataAvail []*idb.OutputAddressAccumulate
	rowdataAvail, err = a.processOutputsPre(outputProcessed, typ, session)
	if err != nil {
		return 0, err
	}

	job = conns.StreamDBDedup().NewJob("accumulate")
	session = conns.DB().NewSessionForEventReceiver(job)

	if len(rowdataAvail) > 0 {
		trowsID := make([]string, 0, LockSize)
		trows := make(map[string]*idb.OutputAddressAccumulate)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > LockSize {
				err := RepeatForLock(func() error { return a.processOutputsPost(trows, trowsID, typ, session, persist) })
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*idb.OutputAddressAccumulate)
				trowsID = make([]string, 0, LockSize)
			}
		}

		if len(trowsID) > 0 {
			err := RepeatForLock(func() error { return a.processOutputsPost(trows, trowsID, typ, session, persist) })
			if err != nil {
				return 0, err
			}
		}
	}

	return uint64(len(rowdataAvail)), nil
}

func (a *BalancerAccumulateHandler) processOutputsPost(
	workRows map[string]*idb.OutputAddressAccumulate,
	workRowsID []string,
	typ processType,
	session *dbr.Session,
	persist idb.Persist,
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

	var rowdata []*idb.OutputAddressAccumulate

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
	persist idb.Persist,
	row *idb.OutputAddressAccumulate,
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

	var balances []*idb.AccumulateBalancesAmount

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
		outputsTxsAccumulate := &idb.OutputTxsAccumulate{
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

		var balancesLocked []*idb.AccumulateBalancesAmount

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

func (a *BalancerAccumulateHandler) processTransactionsPre(session *dbr.Session) ([]*idb.OutputTxsAccumulate, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var rowdata []*idb.OutputTxsAccumulate

	b := session.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"transaction_id",
		"processed",
	).
		From(idb.TableOutputTxsAccumulate)

	_, err = b.
		Where("processed = ?", 0).
		OrderAsc("processed").
		OrderAsc("created_at").
		Limit(RowLimitValue).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return nil, err
	}

	rand.Shuffle(len(rowdata), func(i, j int) { rowdata[i], rowdata[j] = rowdata[j], rowdata[i] })

	return rowdata, nil
}

func (a *BalancerAccumulateHandler) processTransactions(conns *servicesconn.Connections, persist idb.Persist) (uint64, error) {
	job := conns.QuietStream().NewJob("accumulate-poll")
	session := conns.DB().NewSessionForEventReceiver(job)

	var err error
	var rowdataAvail []*idb.OutputTxsAccumulate
	rowdataAvail, err = a.processTransactionsPre(session)
	if err != nil {
		return 0, err
	}

	job = conns.StreamDBDedup().NewJob("accumulate")
	session = conns.DB().NewSessionForEventReceiver(job)

	if len(rowdataAvail) > 0 {
		trowsID := make([]string, 0, LockSize)
		trows := make(map[string]*idb.OutputTxsAccumulate)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > LockSize {
				err := RepeatForLock(func() error { return a.processTransactionsPost(trows, trowsID, session, persist) })
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*idb.OutputTxsAccumulate)
				trowsID = make([]string, 0, LockSize)
			}
		}

		if len(trowsID) > 0 {
			err := RepeatForLock(func() error { return a.processTransactionsPost(trows, trowsID, session, persist) })
			if err != nil {
				return 0, err
			}
		}
	}

	return uint64(len(rowdataAvail)), nil
}

func (a *BalancerAccumulateHandler) processTransactionsPost(
	workRows map[string]*idb.OutputTxsAccumulate,
	workRowsID []string,
	session *dbr.Session,
	persist idb.Persist,
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

	var rowdata []*idb.OutputTxsAccumulate

	_, err = dbTx.Select(
		"id",
	).
		From(idb.TableOutputTxsAccumulate).
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
	persist idb.Persist,
	row *idb.OutputTxsAccumulate,
) error {
	var err error

	_, err = dbTx.Update(idb.TableOutputTxsAccumulate).
		Set("processed", 1).
		Where("id = ?", row.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	b := &idb.AccumulateBalancesTransactions{
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

	var balancesLocked []*idb.AccumulateBalancesTransactions
	_, err = dbTx.Select("id").
		From(idb.TableAccumulateBalancesTransactions).
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

	_, err = dbTx.UpdateBySql("update "+idb.TableAccumulateBalancesTransactions+" "+
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

func RepeatForLock(f func() error) error {
	var err error
	for {
		err = f()
		if err == nil {
			return nil
		}
		if db.ErrIsLockError(err) {
			continue
		}
		return err
	}
}
