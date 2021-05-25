package avax

import (
	"testing"

	"github.com/ava-labs/ortelius/models"
)

func TestReaderAggregate(t *testing.T) {
	reader := &ReaderAggregateTxList{}
	var txs []*models.Transaction
	txs = append(txs, &models.Transaction{ID: "1"})
	reader.Set(txs)
	ftxs := reader.FindTxs(nil, 1)
	if len(ftxs) != 1 {
		t.Fatal("found len(txs) != 1")
	}
	if ftxs[0].ID != "1" {
		t.Fatal("found ftxs[0].ID != '1'")
	}
	ftx := reader.First()
	if ftx == nil {
		t.Fatal("first tx is nil")
	}
	if ftx.ID != "1" {
		t.Fatal("first tx.ID != '1'")
	}
	ftx, _ = reader.Get("1")
	if ftx == nil {
		t.Fatal("first tx is nil")
	}
	if ftx.ID != "1" {
		t.Fatal("first tx.ID != '1'")
	}
}
