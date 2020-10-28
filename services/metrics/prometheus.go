package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	Prometheus Metrics
)

func init() {
	Prometheus.Init()
}

type Metrics struct {
	counters    map[string]*prometheus.Counter
	counterLock sync.RWMutex
}

func (m *Metrics) Init() {
	m.counterLock.Lock()
	defer m.counterLock.Unlock()
	if m.counters == nil {
		m.counters = make(map[string]*prometheus.Counter)
	}
}

func (m *Metrics) CounterInit(name string, help string) {
	m.Init()
	counter := promauto.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})
	m.counterLock.Lock()
	defer m.counterLock.Unlock()
	m.counters[name] = &counter
}

func (m *Metrics) CounterInc(name string) {
	m.counterLock.RLock()
	defer m.counterLock.RUnlock()
	if counter, ok := m.counters[name]; ok {
		(*counter).Inc()
	}
}

func (m *Metrics) CounterAdd(name string, v float64) {
	m.counterLock.RLock()
	defer m.counterLock.RUnlock()
	if counter, ok := m.counters[name]; ok {
		(*counter).Add(v)
	}
}
