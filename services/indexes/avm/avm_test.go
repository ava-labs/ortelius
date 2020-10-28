// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

var (
	testXChainID = ids.NewID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})
)

func TestIndexBootstrap(t *testing.T) {
	writer, reader, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()

	err := writer.Bootstrap(newTestContext())
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	txList, err := reader.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs: []string{testXChainID.String()},
	})
	if err != nil {
		t.Fatal("Failed to list transactions:", err.Error())
	}

	if txList.Count != 1 {
		t.Fatal("Incorrect number of transactions:", txList.Count)
	}

	addr, _ := ids.ToShortID([]byte("addr"))

	sess, _ := writer.conns.DB().NewSession("address_chain", api.RequestTimeout)
	_, _ = sess.InsertInto("address_chain").
		Pair("address", addr.String()).
		Pair("chain_id", "ch1").
		Pair("created_at", time.Now()).
		ExecContext(context.Background())

	addressChains, err := reader.AddressChains(context.Background(), &params.AddressChainsParams{
		Addresses: []ids.ShortID{addr},
	})
	if err != nil {
		t.Fatal("Failed to get address chains:", err.Error())
	}
	if len(addressChains.AddressChains) != 1 {
		t.Fatal("Incorrect number of address chains:", len(addressChains.AddressChains))
	}
	addrf, _ := models.Address(addr.String()).MarshalString()
	if addressChains.AddressChains[string(addrf)][0] != "ch1" {
		t.Fatal("Incorrect chain id")
	}

	// invoke the addrss and asset logic to test the db.
	txList, err = reader.ListTransactions(context.Background(), &params.ListTransactionsParams{
		ChainIDs:  []string{testXChainID.String()},
		Addresses: []ids.ShortID{ids.ShortEmpty},
		AssetID:   &ids.Empty,
	})

	if err != nil {
		t.Fatal("Failed to list transactions:", err.Error())
	}

	if txList.Count != 0 {
		t.Fatal("Incorrect number of transactions:", txList.Count)
	}
}

func newTestIndex(t *testing.T, networkID uint32, chainID ids.ID) (*Writer, *Reader, func()) {
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

	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	// Create index
	writer, err := NewWriter(conns, networkID, chainID.String())
	if err != nil {
		t.Fatal("Failed to create writer:", err.Error())
	}

	reader := NewReader(conns, chainID.String())
	return writer, reader, func() {
		s.Close()
		conns.Close()
	}
}

func newTestContext() context.Context {
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	time.AfterFunc(5*time.Second, cancelFn)
	return ctx
}
