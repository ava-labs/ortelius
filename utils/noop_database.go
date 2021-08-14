package utils

import (
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/database/manager"
	"github.com/ava-labs/avalanchego/version"
	"github.com/prometheus/client_golang/prometheus"
)

type Vers struct {
}

func (*Vers) Major() int {
	return 0
}

func (*Vers) Minor() int {
	return 0
}
func (*Vers) Patch() int {
	return 0
}
func (*Vers) Compare(v version.Version) int {
	return 1
}

func (*Vers) String() string {
	return ""
}

type NoopManager struct {
	Databases []*manager.VersionedDatabase
}

func NewNoopManager(db database.Database) *NoopManager {
	vers := &Vers{}

	return &NoopManager{
		Databases: []*manager.VersionedDatabase{
			{
				Database: db,
				Version:  vers,
			},
		},
	}
}

func (n *NoopManager) NewCompleteMeterDBManager(namespace string, registerer prometheus.Registerer) (manager.Manager, error) {
	panic("undefined")
}

func (n *NoopManager) Current() *manager.VersionedDatabase {
	return n.Databases[0]
}

func (n *NoopManager) Previous() (*manager.VersionedDatabase, bool) {
	return n.Databases[0], true
}

func (n *NoopManager) GetDatabases() []*manager.VersionedDatabase {
	return n.Databases
}

func (n *NoopManager) Close() error {
	return nil
}

func (n *NoopManager) NewPrefixDBManager(prefix []byte) manager.Manager {
	return n
}

func (n *NoopManager) NewNestedPrefixDBManager(prefix []byte) manager.Manager {
	return n
}

func (n *NoopManager) NewMeterDBManager(namespace string, registerer prometheus.Registerer) (manager.Manager, error) {
	return n, nil
}

// NoopDatabase is a lightning fast key value store with probabilistic operations.
type NoopDatabase struct{}

// Has returns false, nil
func (*NoopDatabase) Has([]byte) (bool, error) {
	return true, nil
}

// Get returns nil, error
func (*NoopDatabase) Get([]byte) ([]byte, error) {
	return nil, database.ErrNotFound
}

// Put returns nil
func (*NoopDatabase) Put(_, _ []byte) error { return nil }

// Delete returns nil
func (*NoopDatabase) Delete([]byte) error { return nil }

// NewBatch returns a new batch
func (*NoopDatabase) NewBatch() database.Batch { return &NoopBatch{} }

// NewIterator returns a new empty iterator
func (*NoopDatabase) NewIterator() database.Iterator { return &Iterator{} }

// NewIteratorWithStart returns a new empty iterator
func (*NoopDatabase) NewIteratorWithStart([]byte) database.Iterator { return &Iterator{} }

// NewIteratorWithPrefix returns a new empty iterator
func (*NoopDatabase) NewIteratorWithPrefix([]byte) database.Iterator { return &Iterator{} }

// NewIteratorWithStartAndPrefix returns a new empty iterator
func (db *NoopDatabase) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	return &Iterator{}
}

// Stat returns an error
func (*NoopDatabase) Stat(string) (string, error) { return "", nil }

// Compact returns nil
func (*NoopDatabase) Compact(_, _ []byte) error { return nil }

// Close returns nil
func (*NoopDatabase) Close() error { return nil }

// NoopBatch does nothing
type NoopBatch struct{}

func (b *NoopBatch) Size() int { return 0 }

// Put returns nil
func (*NoopBatch) Put(_, _ []byte) error { return nil }

// Delete returns nil
func (*NoopBatch) Delete([]byte) error { return nil }

// ValueSize returns 0
func (*NoopBatch) ValueSize() int { return 0 }

// Write returns nil
func (*NoopBatch) Write() error { return nil }

// Reset does nothing
func (*NoopBatch) Reset() {}

// Replay does nothing
func (*NoopBatch) Replay(database.KeyValueWriter) error { return nil }

// Inner returns itself
func (b *NoopBatch) Inner() database.Batch { return b }

// Iterator does nothing
type Iterator struct{ Err error }

// Next returns false
func (*Iterator) Next() bool { return false }

// Error returns any errors
func (it *Iterator) Error() error { return it.Err }

// Key returns nil
func (*Iterator) Key() []byte { return nil }

// Value returns nil
func (*Iterator) Value() []byte { return nil }

// Release does nothing
func (*Iterator) Release() {}
