package services

import (
	"context"
	"sync"

	"github.com/gocraft/dbr/v2"
)

type MockPersist struct {
	lock                  sync.RWMutex
	Transactions          map[string]*Transactions
	Outputs               map[string]*Outputs
	OutputsRedeeming      map[string]*OutputsRedeeming
	CvmTransactions       map[string]*CvmTransactions
	CvmAddresses          map[string]*CvmAddresses
	TransactionsValidator map[string]*TransactionsValidator
	TransactionsBlock     map[string]*TransactionsBlock
	Rewards               map[string]*Rewards
}

func NewPersistMock() *MockPersist {
	return &MockPersist{
		Transactions:          make(map[string]*Transactions),
		Outputs:               make(map[string]*Outputs),
		OutputsRedeeming:      make(map[string]*OutputsRedeeming),
		CvmTransactions:       make(map[string]*CvmTransactions),
		CvmAddresses:          make(map[string]*CvmAddresses),
		TransactionsValidator: make(map[string]*TransactionsValidator),
		TransactionsBlock:     make(map[string]*TransactionsBlock),
		Rewards:               make(map[string]*Rewards),
	}
}

func (m *MockPersist) QueryTransactions(ctx context.Context, runner dbr.SessionRunner, v *Transactions) (*Transactions, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Transactions[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTransaction(ctx context.Context, runner dbr.SessionRunner, v *Transactions, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Transactions{}
	*nv = *v
	m.Transactions[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryOutputsRedeeming(ctx context.Context, runner dbr.SessionRunner, v *OutputsRedeeming) (*OutputsRedeeming, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.OutputsRedeeming[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputsRedeeming(ctx context.Context, runner dbr.SessionRunner, v *OutputsRedeeming, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &OutputsRedeeming{}
	*nv = *v
	m.OutputsRedeeming[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryOutputs(ctx context.Context, runner dbr.SessionRunner, v *Outputs) (*Outputs, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Outputs[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputs(ctx context.Context, runner dbr.SessionRunner, v *Outputs, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Outputs{}
	*nv = *v
	m.Outputs[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAssets(ctx context.Context, runner dbr.SessionRunner, assets *Assets) (*Assets, error) {
	panic("implement me")
}

func (m *MockPersist) InsertAssets(ctx context.Context, runner dbr.SessionRunner, assets *Assets, b bool) error {
	panic("implement me")
}

func (m *MockPersist) QueryAddresses(ctx context.Context, runner dbr.SessionRunner, addresses *Addresses) (*Addresses, error) {
	panic("implement me")
}

func (m *MockPersist) InsertAddresses(ctx context.Context, runner dbr.SessionRunner, addresses *Addresses, b bool) error {
	panic("implement me")
}

func (m *MockPersist) QueryAddressChain(ctx context.Context, runner dbr.SessionRunner, chain *AddressChain) (*AddressChain, error) {
	panic("implement me")
}

func (m *MockPersist) InsertAddressChain(ctx context.Context, runner dbr.SessionRunner, chain *AddressChain, b bool) error {
	panic("implement me")
}

func (m *MockPersist) QueryOutputAddresses(ctx context.Context, runner dbr.SessionRunner, addresses *OutputAddresses) (*OutputAddresses, error) {
	panic("implement me")
}

func (m *MockPersist) InsertOutputAddresses(ctx context.Context, runner dbr.SessionRunner, addresses *OutputAddresses, b bool) error {
	panic("implement me")
}

func (m *MockPersist) UpdateOutputAddresses(ctx context.Context, runner dbr.SessionRunner, addresses *OutputAddresses) error {
	panic("implement me")
}

func (m *MockPersist) QueryTransactionsEpoch(ctx context.Context, runner dbr.SessionRunner, epoch *TransactionsEpoch) (*TransactionsEpoch, error) {
	panic("implement me")
}

func (m *MockPersist) InsertTransactionsEpoch(ctx context.Context, runner dbr.SessionRunner, epoch *TransactionsEpoch, b bool) error {
	panic("implement me")
}

func (m *MockPersist) QueryCvmAddresses(ctx context.Context, runner dbr.SessionRunner, v *CvmAddresses) (*CvmAddresses, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmAddresses[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmAddresses(ctx context.Context, runner dbr.SessionRunner, v *CvmAddresses, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmAddresses{}
	*nv = *v
	m.CvmAddresses[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryCvmTransactions(ctx context.Context, runner dbr.SessionRunner, v *CvmTransactions) (*CvmTransactions, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmTransactions[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmTransactions(ctx context.Context, runner dbr.SessionRunner, v *CvmTransactions, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmTransactions{}
	*nv = *v
	m.CvmTransactions[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryPvmBlocks(ctx context.Context, runner dbr.SessionRunner, blocks *PvmBlocks) (*PvmBlocks, error) {
	panic("implement me")
}

func (m *MockPersist) InsertPvmBlocks(ctx context.Context, runner dbr.SessionRunner, blocks *PvmBlocks, b bool) error {
	panic("implement me")
}

func (m *MockPersist) QueryRewards(ctx context.Context, runner dbr.SessionRunner, v *Rewards) (*Rewards, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Rewards[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertRewards(ctx context.Context, runner dbr.SessionRunner, v *Rewards, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Rewards{}
	*nv = *v
	m.Rewards[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryTransactionsValidator(ctx context.Context, runner dbr.SessionRunner, v *TransactionsValidator) (*TransactionsValidator, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.TransactionsValidator[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTransactionsValidator(ctx context.Context, runner dbr.SessionRunner, v *TransactionsValidator, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &TransactionsValidator{}
	*nv = *v
	m.TransactionsValidator[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryTransactionsBlock(ctx context.Context, runner dbr.SessionRunner, v *TransactionsBlock) (*TransactionsBlock, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.TransactionsBlock[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTransactionsBlock(ctx context.Context, runner dbr.SessionRunner, v *TransactionsBlock, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &TransactionsBlock{}
	*nv = *v
	m.TransactionsBlock[v.ID] = nv
	return nil
}
