package avax

import (
	"context"

	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/dbr/v2"
	"github.com/palantir/stacktrace"
)

type BalanceAccumulatorHandler struct {
}

func (a *BalanceAccumulatorHandler) Accumulate(conns *services.Connections) error {
	job := conns.Stream().NewJob("accumulate")
	sess := conns.DB().NewSessionForEventReceiver(job)

	ctx := context.Background()

	var dbTx *dbr.Tx
	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	type Row struct {
		OutputID string
		Address  string
		In       int
		Out      int
	}
	var rows []*Row

	q := dbTx.SelectBySql("select output_id, address " +
		"from output_addresses_accumulate " +
		"where processed = 0 " +
		"limit 1000 " +
		"for update")
	_, err = q.LoadContext(ctx, &rows)
	if err != nil {
		return err
	}

	// // Ingest the tx and commit
	// err = w.insertTx(services.NewConsumerContext(ctx, job, dbTx, i.Timestamp(), persist), i.Body())
	// if err != nil {
	// 	return stacktrace.Propagate(err, "Failed to insert tx")
	// }

	if err = dbTx.Commit(); err != nil {
		return stacktrace.Propagate(err, "Failed to commit database tx")
	}

	return nil
}