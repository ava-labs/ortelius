package services

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/gocraft/dbr/v2"
)

var RowLintValue = uint64(100)
var updTimeout = 10 * time.Second

type processType uint32

var processTypeIn processType = 1
var processTypeOut processType = 2

type BalancerAccumulateHandler struct {
	running int64
	lock    sync.Mutex
	Log     logging.Logger
}

func (a *BalancerAccumulateHandler) Run(conns *Connections, persist Persist, sc *Control) {
	if atomic.LoadInt64(&a.running) != 0 {
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	if atomic.LoadInt64(&a.running) != 0 {
		return
	}

	atomic.AddInt64(&a.running, 1)
	go func() {
		defer func() {
			atomic.AddInt64(&a.running, -1)
		}()

		// delay a bit..
		time.Sleep(1 * time.Millisecond)

		var err error
		for {
			err = a.Accumulate(conns, persist)
			if err == nil || !strings.Contains(err.Error(), db.DeadlockDBErrorMessage) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if err != nil {
			sc.Log.Warn("Accumulate %v", err)
		}
	}()
}

func (a *BalancerAccumulateHandler) Accumulate(conns *Connections, persist Persist) error {
	icnt := 0
	for ; icnt < 10; icnt++ {
		job := conns.Stream().NewJob("accumulate")
		sess := conns.DB().NewSessionForEventReceiver(job)

		cnt, err := a.processOutputs(processTypeOut, sess, persist)
		if err != nil {
			return err
		}
		if cnt > 0 {
			icnt = 0
		}
		cnt, err = a.processOutputs(processTypeIn, sess, persist)
		if err != nil {
			return err
		}
		if cnt > 0 {
			icnt = 0
		}
		cnt, err = a.processTransactions(sess, persist)
		if err != nil {
			return err
		}
		if cnt > 0 {
			icnt = 0
		}
	}

	return nil
}

func (a *BalancerAccumulateHandler) processOutputs(typ processType, sess *dbr.Session, persist Persist) (int, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var rowdata []*OutputAddressAccumulate

	switch typ {
	case processTypeOut:
		_, err = sess.Select(
			"output_addresses_accumulate.id",
			"output_addresses_accumulate.address",
		).
			From("output_addresses_accumulate").
			Join("avm_outputs", "output_addresses_accumulate.id = avm_outputs.id").
			Where("output_addresses_accumulate.processed_out", 0).
			Limit(RowLintValue).
			LoadContext(ctx, &rowdata)
		if err != nil {
			return 0, err
		}
	case processTypeIn:
		_, err = sess.Select(
			"output_addresses_accumulate.id",
			"output_addresses_accumulate.address",
		).
			From("output_addresses_accumulate").
			Join("avm_outputs", "output_addresses_accumulate.id = avm_outputs.id").
			Join("avm_outputs_redeeming", "output_addresses_accumulate.id = avm_outputs_redeeming.id ").
			Where("output_addresses_accumulate.processed_in", 0).
			Limit(RowLintValue).
			LoadContext(ctx, &rowdata)
		if err != nil {
			return 0, err
		}
	}

	if len(rowdata) == 0 {
		return 0, nil
	}

	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return 0, err
	}
	defer dbTx.RollbackUnlessCommitted()

	for _, row := range rowdata {
		var rowdataLock []*OutputAddressAccumulate

		upd := ""
		switch typ {
		case processTypeOut:
			upd = "processed_out"
		case processTypeIn:
			upd = "processed_in"
		}
		_, err := dbTx.Select(
			"id",
			"address",
			"processed_out",
			"processed_in",
		).
			From("output_addresses_accumulate").
			Where(upd+" = ? and id=? and address=?", 0, row.ID, row.Address).
			Suffix("for update").
			LoadContext(ctx, &rowdataLock)
		if err != nil {
			return 0, err
		}

		// it was already processed
		if len(rowdataLock) == 0 {
			continue
		}

		// There will be only 1 record.
		row := rowdataLock[0]

		// This would be unexpected..
		switch typ {
		case processTypeOut:
			if row.ProcessedOut != 0 {
				continue
			}
		case processTypeIn:
			if row.ProcessedIn != 0 {
				continue
			}
		}

		balances := []*AccumulateBalances{}

		_, err = dbTx.Select("avm_outputs.chain_id",
			"avm_output_addresses.address",
			"avm_outputs.asset_id",
			"count(distinct(avm_outputs.transaction_id)) as transaction_count",
			"sum(avm_outputs.amount) as total_received",
			"sum(avm_outputs.amount) as total_sent",
		).From("avm_outputs").
			Join("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_outputs.id=? and avm_output_addresses.address=? ", row.ID, row.Address).
			GroupBy("avm_outputs.chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
			LoadContext(ctx, &balances)
		if err != nil {
			return 0, err
		}

		if len(balances) == 0 {
			a.Log.Info("invalid balance %s %s on %d", row.ID, row.Address, typ)
			continue
		}

		accumulateBalanceIds := []string{}
		for _, b := range balances {
			err = b.ComputeID()
			if err != nil {
				return 0, err
			}
			accumulateBalanceIds = append(accumulateBalanceIds, b.ID)

			err = persist.InsertAccumulateBalances(ctx, dbTx, b)
			if err != nil {
				return 0, err
			}
		}

		balancesLocked := []*AccumulateBalances{}
		_, err = dbTx.Select("id").
			From("accumulate_balances").
			Where("id in ?", accumulateBalanceIds).
			Suffix("for update").
			LoadContext(ctx, &balancesLocked)
		if err != nil {
			return 0, err
		}

		for _, b := range balances {
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
					return 0, err
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
					return 0, err
				}
			}
		}

		_, err = dbTx.Update("output_addresses_accumulate").
			Set(upd, 1).
			Where("id=? and address=?", row.ID, row.Address).
			ExecContext(ctx)
		if err != nil {
			return 0, err
		}
	}

	if err = dbTx.Commit(); err != nil {
		return 0, err
	}

	return len(rowdata), nil
}

func (a *BalancerAccumulateHandler) processTransactions(sess *dbr.Session, persist Persist) (int, error) {
	ctx, cancelCTX := context.WithTimeout(context.Background(), updTimeout)
	defer cancelCTX()

	var err error
	var rowdata []*OutputTxsAccumulate

	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return 0, err
	}
	defer dbTx.RollbackUnlessCommitted()

	_, err = dbTx.Select(
		"id",
		"chain_id",
		"asset_id",
		"address",
		"transaction_id",
	).
		From("output_txs_accumulate").
		Where("processed = 0").
		Limit(RowLintValue).
		Suffix("for update").
		LoadContext(ctx, &rowdata)
	if err != nil {
		return 0, err
	}

	if len(rowdata) == 0 {
		return 0, nil
	}

	balances := []*AccumulateBalances{}
	accumulateBalanceIds := []string{}
	for _, row := range rowdata {
		b := &AccumulateBalances{
			ChainID: row.ChainID,
			AssetID: row.AssetID,
			Address: row.Address,
		}
		err = b.ComputeID()
		if err != nil {
			return 0, err
		}
		accumulateBalanceIds = append(accumulateBalanceIds, b.ID)
		err = persist.InsertAccumulateBalances(ctx, dbTx, b)
		if err != nil {
			return 0, err
		}
		balances = append(balances, b)
	}

	balancesLocked := []*AccumulateBalances{}
	_, err = dbTx.SelectBySql("select id "+
		"from accumulate_balances "+
		"where id in ? "+
		"for update", accumulateBalanceIds).
		LoadContext(ctx, &balancesLocked)
	if err != nil {
		return 0, err
	}

	for _, b := range balances {
		_, err = dbTx.UpdateBySql("update accumulate_balances "+
			"set "+
			"transaction_count = transaction_count+1 "+
			"where id=? "+
			"", b.ID).
			ExecContext(ctx)
		if err != nil {
			return 0, err
		}
	}

	for _, row := range rowdata {
		_, err = dbTx.Update("output_txs_accumulate").
			Set("processed", 1).
			Where("id=?", row.ID).
			ExecContext(ctx)
		if err != nil {
			return 0, err
		}
	}

	if err = dbTx.Commit(); err != nil {
		return 0, err
	}

	return len(rowdata), nil
}
