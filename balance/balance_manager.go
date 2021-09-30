// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package balance

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
	"github.com/gocraft/dbr/v2"
)

var RowLimitValue = uint64(5000)
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

type Manager struct {
	handler *Handler
	sc      *servicesctrl.Control
	persist db.Persist

	managerChannelsList []*managerChannels

	doneCh  chan struct{}
	enabled bool
}

func NewManager(persist db.Persist, sc *servicesctrl.Control) *Manager {
	return &Manager{
		handler: &Handler{},
		sc:      sc,
		persist: persist,
		doneCh:  make(chan struct{}),
	}
}

func (a *Manager) Close() {
	close(a.doneCh)
}

func (a *Manager) Start(enabled bool) error {
	a.enabled = enabled
	if !a.enabled {
		return nil
	}

	connsTicker, err := a.sc.Database()
	if err != nil {
		return err
	}

	a.runTicker(connsTicker)

	processingFunc := func(id string, managerChannel *managerChannels) error {
		connsOuts, err := a.sc.Database()
		if err != nil {
			return err
		}
		a.runProcessing("out_"+id, connsOuts, func(conns *utils.Connections) (uint64, error) {
			return a.handler.processOutputs(true, processTypeOut, conns, a.persist)
		}, managerChannel.ins)

		connsIns, err := a.sc.Database()
		if err != nil {
			return err
		}
		a.runProcessing("in_"+id, connsIns, func(conns *utils.Connections) (uint64, error) {
			return a.handler.processOutputs(true, processTypeIn, conns, a.persist)
		}, managerChannel.outs)

		connsTxs, err := a.sc.Database()
		if err != nil {
			return err
		}
		a.runProcessing("tx_"+id, connsTxs, func(conns *utils.Connections) (uint64, error) {
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

func (a *Manager) runTicker(conns *utils.Connections) {
	a.sc.Logger().Info("start")
	go func() {
		defer func() {
			a.sc.Logger().Info("stop")
		}()

		ticker := time.NewTicker(30 * time.Second)

		runEvent := func(conns *utils.Connections) {
			icnt := 0
			for ; icnt < retryProcessing; icnt++ {
				cnt, err := a.handler.processOutputs(false, processTypeIn, conns, a.persist)
				if utils.ErrIsLockError(err) {
					icnt = 0
					continue
				}
				if err != nil {
					a.sc.Logger().Error("accumulate ticker error %v", err)
					return
				}
				if cnt < RowLimitValue {
					break
				}
				icnt = 0
			}
		}

		defer func() {
			ticker.Stop()
			err := conns.Close()
			if err != nil {
				a.sc.Logger().Warn("connection close %v", err)
			}
			a.sc.Logger().Info("stop ticker")
		}()

		for {
			select {
			case <-ticker.C:
				runEvent(conns)
			case <-a.doneCh:
				return
			}
		}
	}()
}

func (a *Manager) runProcessing(id string, conns *utils.Connections, f func(conns *utils.Connections) (uint64, error), trigger chan struct{}) {
	a.sc.Logger().Info("start processing %v", id)
	go func() {
		defer func() {
			a.sc.Logger().Info("stop processing %v", id)
		}()
		runEvent := func(conns *utils.Connections) {
			icnt := 0
			for ; icnt < retryProcessing; icnt++ {
				cnt, err := f(conns)
				if utils.ErrIsLockError(err) {
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

func (a *Manager) Exec() {
	if !a.enabled {
		return
	}

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

type Handler struct {
}

func outTable(typ processType) string {
	tbl := ""
	switch typ {
	case processTypeOut:
		tbl = db.TableOutputAddressAccumulateOut
	case processTypeIn:
		tbl = db.TableOutputAddressAccumulateIn
	}
	return tbl
}

func balanceTable(typ processType) string {
	tbl := ""
	switch typ {
	case processTypeOut:
		tbl = db.TableAccumulateBalancesReceived
	case processTypeIn:
		tbl = db.TableAccumulateBalancesSent
	}
	return tbl
}

func (a *Handler) processOutputsPre(outputProcessed bool, typ processType, session *dbr.Session) ([]*db.OutputAddressAccumulate, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var rowdata []*db.OutputAddressAccumulate
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

func (a *Handler) processOutputs(outputProcessed bool, typ processType, conns *utils.Connections, persist db.Persist) (uint64, error) {
	job := conns.Stream().NewJob("accumulate-poll")
	session := conns.DB().NewSessionForEventReceiver(job)

	var err error
	var rowdataAvail []*db.OutputAddressAccumulate
	rowdataAvail, err = a.processOutputsPre(outputProcessed, typ, session)
	if err != nil {
		return 0, err
	}

	job = conns.Stream().NewJob("accumulate")
	session = conns.DB().NewSessionForEventReceiver(job)

	if len(rowdataAvail) > 0 {
		trowsID := make([]string, 0, LockSize)
		trows := make(map[string]*db.OutputAddressAccumulate)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > LockSize {
				err := RepeatForLock(func() error { return a.processOutputsPost(trows, trowsID, typ, session, persist) })
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*db.OutputAddressAccumulate)
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

func (a *Handler) processOutputsPost(
	workRows map[string]*db.OutputAddressAccumulate,
	workRowsID []string,
	typ processType,
	session *dbr.Session,
	persist db.Persist,
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

	var rowdata []*db.OutputAddressAccumulate

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

func (a *Handler) processOutputsBase(
	ctx context.Context,
	typ processType,
	dbTx *dbr.Tx,
	persist db.Persist,
	row *db.OutputAddressAccumulate,
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

	var balances []*db.AccumulateBalancesAmount

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
		outputsTxsAccumulate := &db.OutputTxsAccumulate{
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

		var balancesLocked []*db.AccumulateBalancesAmount

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

func (a *Handler) processTransactionsPre(session *dbr.Session) ([]*db.OutputTxsAccumulate, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var rowdata []*db.OutputTxsAccumulate

	b := session.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"transaction_id",
		"processed",
	).
		From(db.TableOutputTxsAccumulate)

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

func (a *Handler) processTransactions(conns *utils.Connections, persist db.Persist) (uint64, error) {
	job := conns.Stream().NewJob("accumulate-poll")
	session := conns.DB().NewSessionForEventReceiver(job)

	var err error
	var rowdataAvail []*db.OutputTxsAccumulate
	rowdataAvail, err = a.processTransactionsPre(session)
	if err != nil {
		return 0, err
	}

	job = conns.Stream().NewJob("accumulate")
	session = conns.DB().NewSessionForEventReceiver(job)

	if len(rowdataAvail) > 0 {
		trowsID := make([]string, 0, LockSize)
		trows := make(map[string]*db.OutputTxsAccumulate)
		for _, row := range rowdataAvail {
			trows[row.ID] = row
			trowsID = append(trowsID, row.ID)
			if len(trowsID) > LockSize {
				err := RepeatForLock(func() error { return a.processTransactionsPost(trows, trowsID, session, persist) })
				if err != nil {
					return 0, err
				}
				trows = make(map[string]*db.OutputTxsAccumulate)
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

func (a *Handler) processTransactionsPost(
	workRows map[string]*db.OutputTxsAccumulate,
	workRowsID []string,
	session *dbr.Session,
	persist db.Persist,
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

	var rowdata []*db.OutputTxsAccumulate

	_, err = dbTx.Select(
		"id",
	).
		From(db.TableOutputTxsAccumulate).
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

func (a *Handler) processTransactionsBase(
	ctx context.Context,
	dbTx *dbr.Tx,
	persist db.Persist,
	row *db.OutputTxsAccumulate,
) error {
	var err error

	_, err = dbTx.Update(db.TableOutputTxsAccumulate).
		Set("processed", 1).
		Where("id = ?", row.ID).
		ExecContext(ctx)
	if err != nil {
		return err
	}

	b := &db.AccumulateBalancesTransactions{
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

	var balancesLocked []*db.AccumulateBalancesTransactions
	_, err = dbTx.Select("id").
		From(db.TableAccumulateBalancesTransactions).
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

	_, err = dbTx.UpdateBySql("update "+db.TableAccumulateBalancesTransactions+" "+
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
		if utils.ErrIsLockError(err) {
			continue
		}
		return err
	}
}
