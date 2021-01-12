package pvm

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/vms/components/core"

	"github.com/ava-labs/ortelius/services/indexes/models"

	"github.com/ava-labs/avalanchego/vms/platformvm"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

var (
	testXChainID = ids.ID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})
)

func TestBootstrap(t *testing.T) {
	w, r, closeFn := newTestIndex(t, 12345, ChainID)
	defer closeFn()

	persist := services.NewPersist()
	if err := w.Bootstrap(context.Background(), persist); err != nil {
		t.Fatal(err)
	}

	txList, err := r.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs: []string{ChainID.String()},
	}, ids.Empty)
	if err != nil {
		t.Fatal("Failed to list transactions:", err.Error())
	}

	if txList == nil || *txList.Count < 1 {
		if txList.Count == nil {
			t.Fatal("Incorrect number of transactions:", txList.Count)
		} else {
			t.Fatal("Incorrect number of transactions:", *txList.Count)
		}
	}
}

func newTestIndex(t *testing.T, networkID uint32, chainID ids.ID) (*Writer, *avax.Reader, func()) {
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

	// Create index
	writer, err := NewWriter(conns, networkID, chainID.String())
	if err != nil {
		t.Fatal("Failed to create writer:", err.Error())
	}

	reader := avax.NewReader(conns)
	return writer, reader, func() {
		s.Close()
		_ = conns.Close()
	}
}

func TestInsertTxInternal(t *testing.T) {
	writer, _, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := platformvm.Tx{}
	validatorTx := &platformvm.UnsignedAddValidatorTx{}
	tx.UnsignedTx = validatorTx

	persist := services.NewPersistMock()
	session, _ := writer.conns.DB().NewSession("test_tx", cfg.RequestTimeout)
	job := writer.conns.Stream().NewJob("")
	cCtx := services.NewConsumerContext(ctx, job, session, time.Now().Unix(), persist)
	err := writer.indexTransaction(cCtx, tx.ID(), tx, false)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.Transactions) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.TransactionsBlock) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.TransactionsValidator) != 1 {
		t.Fatal("insert failed")
	}
}

func TestInsertTxInternalRewards(t *testing.T) {
	writer, _, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := platformvm.Tx{}
	validatorTx := &platformvm.UnsignedRewardValidatorTx{}
	tx.UnsignedTx = validatorTx

	persist := services.NewPersistMock()
	session, _ := writer.conns.DB().NewSession("test_tx", cfg.RequestTimeout)
	job := writer.conns.Stream().NewJob("")
	cCtx := services.NewConsumerContext(ctx, job, session, time.Now().Unix(), persist)
	err := writer.indexTransaction(cCtx, tx.ID(), tx, false)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.Transactions) != 0 {
		t.Fatal("insert failed")
	}
	if len(persist.TransactionsBlock) != 0 {
		t.Fatal("insert failed")
	}
	if len(persist.TransactionsValidator) != 0 {
		t.Fatal("insert failed")
	}
	if len(persist.Rewards) != 1 {
		t.Fatal("insert failed")
	}
}

func TestCommonBlock(t *testing.T) {
	writer, _, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := platformvm.CommonBlock{Block: &core.Block{}}
	blkid := ids.ID{}

	persist := services.NewPersistMock()
	session, _ := writer.conns.DB().NewSession("test_tx", cfg.RequestTimeout)
	job := writer.conns.Stream().NewJob("")
	cCtx := services.NewConsumerContext(ctx, job, session, time.Now().Unix(), persist)
	err := writer.indexCommonBlock(cCtx, blkid, models.BlockTypeCommit, tx, []byte(""))
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.PvmBlocks) != 1 {
		t.Fatal("insert failed")
	}
}
