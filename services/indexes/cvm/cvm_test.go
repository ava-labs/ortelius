// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cvm

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	avalancheGoAvax "github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/coreth/core/types"
	"github.com/ava-labs/coreth/plugin/evm"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/modelsc"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
)

var (
	testXChainID = ids.ID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})
)

func newTestIndex(t *testing.T, networkID uint32, chainID ids.ID) (*utils.Connections, *Writer, func()) {
	logConf, err := logging.DefaultConfig()
	if err != nil {
		t.Fatal("Failed to create logging config:", err.Error())
	}

	conf := cfg.Services{
		Logging: logConf,
		DB: &cfg.DB{
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true",
		},
	}

	sc := &servicesctrl.Control{Log: logging.NoLog{}, Services: conf}
	conns, err := sc.Database()
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	// Create index
	writer, err := NewWriter(networkID, chainID.String())
	if err != nil {
		t.Fatal("Failed to create writer:", err.Error())
	}

	return conns, writer, func() {
		_ = conns.Close()
	}
}

func TestInsertTxInternalExport(t *testing.T) {
	conns, writer, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := &evm.Tx{}

	extx := &evm.UnsignedExportTx{}
	extxIn := evm.EVMInput{}
	extx.Ins = []evm.EVMInput{extxIn}
	transferableOut := &avalancheGoAvax.TransferableOutput{}
	transferableOut.Out = &secp256k1fx.TransferOutput{}
	extx.ExportedOutputs = []*avalancheGoAvax.TransferableOutput{transferableOut}

	tx.UnsignedAtomicTx = extx
	header := types.Header{}
	block := &modelsc.Block{Header: header}

	persist := db.NewPersistMock()
	session := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("test_tx"))
	cCtx := services.NewConsumerContext(ctx, session, time.Now().Unix(), 0, persist)
	err := writer.indexBlockInternal(cCtx, []*evm.Tx{tx}, tx.Bytes(), block)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.CvmTransactions) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Outputs) != 1 {
		t.Fatal("insert failed")
	}
}

func TestInsertTxInternalImport(t *testing.T) {
	conns, writer, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()
	tx := &evm.Tx{}

	extx := &evm.UnsignedImportTx{}
	evtxOut := evm.EVMOutput{}
	extx.Outs = []evm.EVMOutput{evtxOut}
	transferableIn := &avalancheGoAvax.TransferableInput{}
	transferableIn.In = &secp256k1fx.TransferInput{}
	extx.ImportedInputs = []*avalancheGoAvax.TransferableInput{transferableIn}

	tx.UnsignedAtomicTx = extx
	header := types.Header{}
	block := &modelsc.Block{Header: header}

	persist := db.NewPersistMock()
	session := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("test_tx"))
	cCtx := services.NewConsumerContext(ctx, session, time.Now().Unix(), 0, persist)
	err := writer.indexBlockInternal(cCtx, []*evm.Tx{tx}, tx.Bytes(), block)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.CvmTransactions) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.CvmAddresses) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputsRedeeming) != 1 {
		t.Fatal("insert failed")
	}
}
