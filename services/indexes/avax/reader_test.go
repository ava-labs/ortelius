// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avax

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

func TestCollectInsAndOuts(t *testing.T) {
	reader, closeFn := newTestIndex(t)
	defer closeFn()

	ctx := newTestContext()
	session, _ := reader.conns.DB().NewSession("test_tx", cfg.RequestTimeout)

	_, _ = session.DeleteFrom("avm_outputs").ExecContext(ctx)
	_, _ = session.DeleteFrom("avm_output_addresses").ExecContext(ctx)
	_, _ = session.DeleteFrom("avm_outputs_redeeming").ExecContext(ctx)

	inputID := "in1"
	outputID := "out1"
	chainID := "ch1"
	txID := "tx1"
	intxID := "tx0"
	address := "addr1"
	idx := uint32(0)
	assetID := "assid1"
	outputType := models.OutputTypesSECP2556K1Transfer
	amount := uint64(1)
	locktime := uint64(0)
	thresholD := uint32(0)
	groupID := uint32(0)
	payload := []byte("")
	stakeLocktime := uint64(99991)
	time := time.Now().Truncate(1 * time.Hour)

	inputIDUnmatched := "inu"

	persist := services.NewPersist()

	outputs := &services.Outputs{
		ID:            outputID,
		ChainID:       chainID,
		TransactionID: txID,
		OutputIndex:   idx,
		AssetID:       assetID,
		OutputType:    outputType,
		Amount:        amount,
		Locktime:      locktime,
		Threshold:     thresholD,
		GroupID:       groupID,
		Payload:       payload,
		StakeLocktime: stakeLocktime,
		CreatedAt:     time,
	}
	_ = persist.InsertOutputs(ctx, session, outputs, false)

	outputAddresses := &services.OutputAddresses{
		OutputID:  outputID,
		Address:   address,
		CreatedAt: time,
	}
	_ = persist.InsertOutputAddresses(ctx, session, outputAddresses, false)

	outputsRedeeming := &services.OutputsRedeeming{
		ID:                     inputID,
		RedeemedAt:             time,
		RedeemingTransactionID: txID,
		Amount:                 amount,
		OutputIndex:            idx,
		Intx:                   intxID,
		AssetID:                assetID,
		CreatedAt:              time,
	}
	_ = persist.InsertOutputsRedeeming(ctx, session, outputsRedeeming, false)

	outputsRedeeming = &services.OutputsRedeeming{
		ID:                     inputIDUnmatched,
		RedeemedAt:             time,
		RedeemingTransactionID: txID,
		Amount:                 amount,
		OutputIndex:            idx,
		Intx:                   intxID,
		AssetID:                assetID,
		CreatedAt:              time,
	}
	_ = persist.InsertOutputsRedeeming(ctx, session, outputsRedeeming, false)

	records, _ := reader.collectInsAndOuts(ctx, session, []models.StringID{models.StringID(txID)})

	if len(records) != 3 {
		t.Error("invalid input/outputs")
	}

	if records[0].Output.ID != models.StringID(outputID) &&
		records[1].ID != models.StringID(inputID) &&
		records[2].ID != models.StringID(inputIDUnmatched) {
		t.Error("invalid input/outputs")
	}

	if records[0].Output.OutputType != models.OutputTypesSECP2556K1Transfer &&
		records[1].Output.OutputType != 0 &&
		records[2].Output.OutputType != 0 {
		t.Error("invalid output type")
	}

	if records[0].Output.StakeLocktime != stakeLocktime &&
		records[1].Output.StakeLocktime != 0 &&
		records[2].Output.StakeLocktime != 0 {
		t.Error("invalid stake locktime")
	}
}

func TestAggregateTxfee(t *testing.T) {
	reader, closeFn := newTestIndex(t)
	defer closeFn()

	ctx := newTestContext()

	persist := services.NewPersist()

	sess, _ := reader.conns.DB().NewSession("test_aggregate_tx_fee", cfg.RequestTimeout)
	_, _ = sess.DeleteFrom("avm_transactions").ExecContext(ctx)

	tnow := time.Now().UTC().Truncate(1 * time.Second).Add(-1 * time.Hour)

	transaction := &services.Transactions{
		ID:        "id1",
		ChainID:   "cid",
		Type:      "type",
		Txfee:     10,
		CreatedAt: tnow,
	}
	_ = persist.InsertTransaction(ctx, sess, transaction, false)

	transaction = &services.Transactions{
		ID:        "id2",
		ChainID:   "cid",
		Type:      "type",
		Txfee:     15,
		CreatedAt: tnow.Add(-1 * time.Hour),
	}
	_ = persist.InsertTransaction(ctx, sess, transaction, false)

	starttime := tnow.Add(-2 * time.Hour)
	endtime := tnow.Add(1 * time.Second)
	p := params.TxfeeAggregateParams{ListParams: params.ListParams{StartTime: starttime, EndTime: endtime}}
	agg, err := reader.TxfeeAggregate(ctx, &p)
	if err != nil {
		t.Error("error", err)
	}
	if agg.TxfeeAggregates.Txfee != models.TokenAmount("25") {
		t.Error("aggregate tx invalid expected ", agg.TxfeeAggregates.Txfee)
	}
	if agg.StartTime != starttime || agg.EndTime != endtime {
		t.Error("aggregate tx invalid")
	}

	p = params.TxfeeAggregateParams{ListParams: params.ListParams{StartTime: tnow.Add(-50 * time.Minute), EndTime: tnow.Add(1 * time.Second)}}
	agg, _ = reader.TxfeeAggregate(ctx, &p)

	if agg.TxfeeAggregates.Txfee != models.TokenAmount("10") {
		t.Error("aggregate tx invalid expected ", agg.TxfeeAggregates.Txfee)
	}
}

func newTestIndex(t *testing.T) (*Reader, func()) {
	// Start test redis
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("Failed to create miniredis server:", err.Error())
	}

	logConf, err := logging.DefaultConfig()
	if err != nil {
		t.Fatal("Failed to create logging config:", err.Error())
	}

	conf := cfg.Services{
		Logging: logConf,
		DB: &cfg.DB{
			TXDB:   true,
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true",
		},
		Redis: &cfg.Redis{
			Addr: s.Addr(),
		},
	}

	sc := &services.Control{Log: logging.NoLog{}, Services: conf}
	conns, err := sc.Database()
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	reader := NewReader(conns, nil)
	return reader, func() {
		s.Close()
		_ = conns.Close()
	}
}

func newTestContext() context.Context {
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	time.AfterFunc(5*time.Second, cancelFn)
	return ctx
}
