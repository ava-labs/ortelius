package pvm

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

func TestBootstrap(t *testing.T) {
	w, r, closeFn := newTestIndex(t, 12345, ChainID)
	defer closeFn()

	if err := w.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}

	txList, err := r.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs: []string{ChainID.String()},
	}, ids.Empty)
	if err != nil {
		t.Fatal("Failed to list transactions:", err.Error())
	}

	if txList == nil || *txList.Count != 7 {
		t.Fatal("Incorrect number of transactions:", txList.Count)
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

	sc := &services.ServicesControl{Log: logging.NoLog{}, Services: conf}
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
