package avax

import (
	"context"
	"fmt"
	"math/big"
	"os"

	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/dbr/v2"
)

var (
	RowLimit = fmt.Sprintf("%d", 500)
)

func BalanceAccumulatorHandlerAccumulate(conns *services.Connections) error {
	job := conns.Stream().NewJob("accumulate")
	sess := conns.DB().NewSessionForEventReceiver(job)

	for {
		cnt, err := processDataOut(sess)
		if err != nil {
			return err
		}
		if cnt == 0 {
			break
		}
	}
	for {
		cnt, err := processDataIn(sess)
		if err != nil {
			return err
		}
		if cnt == 0 {
			break
		}
	}

	return nil
}

func processDataOut(sess *dbr.Session) (int, error) {
	ctx := context.Background()
	type Row struct {
		OutputID string
		Address  string
	}
	var rowdata []*Row

	_, err := sess.SelectBySql("select output_id, address "+
		"from output_addresses_accumulate "+
		"where processed = 0 and type = ? "+
		"limit 1 "+
		" ", services.OutputAddressAccumulateTypeOut).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return 0, err
	}

	if len(rowdata) == 0 {
		return 0, nil
	}

	rowdata = nil

	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return 0, err
	}
	defer dbTx.RollbackUnlessCommitted()

	_, err = dbTx.SelectBySql("select output_id, address "+
		"from output_addresses_accumulate "+
		"where processed = 0 and type = ? "+
		"limit "+RowLimit+" "+
		"for update", services.OutputAddressAccumulateTypeOut).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return 0, err
	}

	lenrowdata := len(rowdata)
	if lenrowdata == 0 {
		return 0, nil
	}

	for _, row := range rowdata {
		balances := []*services.AccumulateBalances{}

		_, err = dbTx.Select("avm_outputs.chain_id",
			"avm_output_addresses.address",
			"avm_outputs.asset_id",
			"count(distinct(avm_outputs.transaction_id)) as transaction_count",
			"sum(avm_outputs.amount) as balance",
		).From("avm_outputs").
			Join("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_outputs.id=? and avm_output_addresses.address=?", row.OutputID, row.Address).
			GroupBy("avm_outputs.chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
			LoadContext(ctx, &balances)
		if err != nil {
			return 0, err
		}
		for _, b := range balances {
			err = b.ComputeID()
			if err != nil {
				return 0, err
			}

			bi := new(big.Int)
			bi.SetString(b.TransactionCount, 10)
			if bi.Int64() > 1 {
				fmt.Fprintf(os.Stderr, "here\n")
			}
			// fmt.Fprintf(os.Stderr, "%v\n", b)

			_, err = dbTx.UpdateBySql("update accumulate_balances "+
				"set "+
				"utxo_count = utxo_count+1, "+
				"balance = balance+"+b.Balance+", "+
				"total_received = total_received+"+b.Balance+", "+
				"transaction_count = transaction_count+"+b.TransactionCount+" "+
				"where id=? "+
				"", b.ID).
				ExecContext(ctx)
			if err != nil {
				return 0, err
			}

			/*
						"avm_outputs.chain_id",
				"avm_output_addresses.address",
				"avm_outputs.asset_id",
				"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
				"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN 1 ELSE 0 END), 0) AS utxo_count",
			*/
		}

		_, err = dbTx.UpdateBySql("update output_addresses_accumulate "+
			"set processed = 1 "+
			"where type = ? and output_id=? and address=? "+
			"", services.OutputAddressAccumulateTypeOut, row.OutputID, row.Address).
			ExecContext(ctx)
		if err != nil {
			return 0, err
		}
	}

	if err = dbTx.Commit(); err != nil {
		return 0, err
	}

	return lenrowdata, nil
}

func processDataIn(sess *dbr.Session) (int, error) {
	ctx := context.Background()

	type Row struct {
		OutputID string
		Address  string
	}
	var rowdata []*Row

	_, err := sess.SelectBySql("select output_id, address "+
		"from output_addresses_accumulate "+
		"where "+
		"processed = 0 and out = 1 and type = ? "+
		"limit 1 "+
		" ", services.OutputAddressAccumulateTypeIn, services.OutputAddressAccumulateTypeOut).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return 0, err
	}
	if len(rowdata) == 0 {
		return 0, nil
	}

	rowdata = nil

	var dbTx *dbr.Tx
	dbTx, err = sess.Begin()
	if err != nil {
		return 0, err
	}
	defer dbTx.RollbackUnlessCommitted()

	_, err = dbTx.SelectBySql("select output_id, address "+
		"from output_addresses_accumulate "+
		"where "+
		"processed = 0 and out = 1 and type = ? "+
		"limit "+RowLimit+" "+
		"for update ", services.OutputAddressAccumulateTypeIn).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return 0, err
	}

	lenrowdata := len(rowdata)
	if lenrowdata == 0 {
		return 0, nil
	}

	for _, row := range rowdata {
		balances := []*services.AccumulateBalances{}

		_, err = dbTx.Select("avm_outputs.chain_id",
			"avm_output_addresses.address",
			"avm_outputs.asset_id",
			"sum(avm_outputs.amount) as balance",
		).From("avm_outputs").
			Join("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			Where("avm_outputs.id=? and avm_output_addresses.address=?", row.OutputID, row.Address).
			GroupBy("avm_outputs.chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
			LoadContext(ctx, &balances)
		if err != nil {
			return 0, err
		}
		for _, b := range balances {
			err = b.ComputeID()
			if err != nil {
				return 0, err
			}

			bi := new(big.Int)
			bi.SetString(b.TransactionCount, 10)
			if bi.Int64() > 1 {
				fmt.Fprintf(os.Stderr, "here\n")
			}
			// fmt.Fprintf(os.Stderr, "%v\n", b)

			_, err = dbTx.UpdateBySql("update accumulate_balances "+
				"set "+
				"utxo_count = utxo_count-1, "+
				"total_sent = total_sent+"+b.Balance+", "+
				"balance = balance-"+b.Balance+" "+
				"where id=? "+
				"", b.ID).
				ExecContext(ctx)
			if err != nil {
				return 0, err
			}

			/*
						"avm_outputs.chain_id",
				"avm_output_addresses.address",
				"avm_outputs.asset_id",
				"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
				"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN 1 ELSE 0 END), 0) AS utxo_count",
			*/
		}

		_, err = dbTx.UpdateBySql("update output_addresses_accumulate "+
			"set processed = 1 "+
			"where type = ? and output_id=? and address=? "+
			"", services.OutputAddressAccumulateTypeIn, row.OutputID, row.Address).
			ExecContext(ctx)
		if err != nil {
			return 0, err
		}
	}

	if err = dbTx.Commit(); err != nil {
		return 0, err
	}

	return lenrowdata, nil
}