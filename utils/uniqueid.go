package utils

import "sync"

type UniqueID interface {
	Get(id string) (bool, error)
	Put(id string) error
}

type uniqueID struct {
	m    map[string]int
	lock sync.Mutex
}

func NewMemoryUniqueID() UniqueID {
	return &uniqueID{m: make(map[string]int)}
}

func (uniqueId *uniqueID) Get(id string) (bool, error) {
	uniqueId.lock.Lock()
	defer uniqueId.lock.Unlock()

	if res, ok := uniqueId.m[id]; ok {
		return res != 0, nil
	}
	return false, nil
}

func (uniqueId *uniqueID) Put(id string) error {
	uniqueId.lock.Lock()
	defer uniqueId.lock.Unlock()

	uniqueId.m[id] = 1
	return nil
}
