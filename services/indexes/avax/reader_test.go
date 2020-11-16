package avax

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

func TestCollectInsAndOuts(t *testing.T) {
	reader, closeFn := newTestIndex(t)
	defer closeFn()

	ctx := newTestContext()
	session, _ := reader.conns.DB().NewSession("test_tx", services.RequestTimeout)

	_, _ = session.DeleteFrom("avm_outputs").ExecContext(ctx)
	_, _ = session.DeleteFrom("avm_output_addresses").ExecContext(ctx)
	_, _ = session.DeleteFrom("avm_outputs_redeeming").ExecContext(ctx)

	inputID := "in1"
	outputID := "out1"
	chainID := "ch1"
	txID := "tx1"
	intxID := "tx0"
	address := "addr1"
	idx := 0
	assetID := "assid1"
	outputType := models.OutputTypesSECP2556K1Transfer
	amount := 1
	locktime := 0
	thresholD := 0
	groupID := 0
	payload := []byte("")
	stakeLocktime := uint64(99991)
	time := time.Now().Truncate(1 * time.Hour)

	inputIDUnmatched := "inu"

	_, _ = session.InsertInto("avm_outputs").
		Pair("id", outputID).
		Pair("chain_id", chainID).
		Pair("transaction_id", txID).
		Pair("output_index", idx).
		Pair("asset_id", assetID).
		Pair("output_type", outputType).
		Pair("amount", amount).
		Pair("locktime", locktime).
		Pair("threshold", thresholD).
		Pair("group_id", groupID).
		Pair("payload", payload).
		Pair("stake_locktime", stakeLocktime).
		Pair("created_at", time).
		ExecContext(ctx)

	_, _ = session.InsertInto("avm_output_addresses").
		Pair("output_id", outputID).
		Pair("address", address).
		Pair("created_at", time).
		Exec()

	_, _ = session.InsertInto("avm_outputs_redeeming").
		Pair("id", inputID).
		Pair("redeemed_at", time).
		Pair("redeeming_transaction_id", txID).
		Pair("amount", amount).
		Pair("output_index", idx).
		Pair("intx", intxID).
		Pair("asset_id", assetID).
		Pair("created_at", time).
		ExecContext(ctx)

	_, _ = session.InsertInto("avm_outputs_redeeming").
		Pair("id", inputIDUnmatched).
		Pair("redeemed_at", time).
		Pair("redeeming_transaction_id", txID).
		Pair("amount", amount).
		Pair("output_index", idx).
		Pair("intx", intxID).
		Pair("asset_id", assetID).
		Pair("created_at", time).
		ExecContext(ctx)

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

	conf.Log = logging.NoLog{}
	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	reader := NewReader(conns)
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
