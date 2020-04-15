package pvm_index

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/ava-labs/gecko/utils/hashing"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/ortelius/cfg"
)

type message struct {
	id        ids.ID
	chainID   ids.ID
	body      []byte
	timestamp uint64
}

func (m *message) ID() ids.ID        { return m.id }
func (m *message) ChainID() ids.ID   { return m.chainID }
func (m *message) Body() []byte      { return m.body }
func (m *message) Timestamp() uint64 { return m.timestamp }

func newTestIndex(t *testing.T) (*Index, func()) {
	// Get default config
	conf, err := cfg.NewAPIConfig("")
	if err != nil {
		t.Fatal("Failed to get config:", err.Error())
	}

	// Configure test db
	conf.DB.TXDB = true
	conf.DB.DSN = "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true"

	// Configure test redis
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("Failed to create miniredis server:", err.Error())
	}
	conf.Redis.Addr = s.Addr()

	// Create index
	idx, err := New(conf.ServiceConfig, testPChainID)
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	return idx, func() {
		s.Close()
		idx.db.db.Close()
	}
}

func TestPVMIndex(t *testing.T) {
	idx, closeFn := newTestIndex(t)
	defer closeFn()

	err := idx.Bootstrap()
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}
}

func TestPVMIngestAtomicBlock(t *testing.T) {
	idx, closeFn := newTestIndex(t)
	defer closeFn()

	err := idx.Add(&message{
		id:        ids.NewID(hashing.ComputeHash256Array(testAtomicBlock)),
		chainID:   ids.Empty,
		body:      testAtomicBlock,
		timestamp: 123,
	})
	if err != nil {
		t.Fatal("Failed to add block to index:", err.Error())
	}
}
