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
	histograms  map[string]*prometheus.Histogram
	metricsLock sync.RWMutex
}

func (m *Metrics) Init() {
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()
	if m.counters == nil {
		m.counters = make(map[string]*prometheus.Counter)
	}
	if m.histograms == nil {
		m.histograms = make(map[string]*prometheus.Histogram)
	}
}

func (m *Metrics) CounterInit(name string, help string) {
	m.Init()
	counter := promauto.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()
	m.counters[name] = &counter
}

func (m *Metrics) CounterInc(name string) {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()
	if counter, ok := m.counters[name]; ok {
		(*counter).Inc()
	}
}

func (m *Metrics) CounterAdd(name string, v float64) {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()
	if counter, ok := m.counters[name]; ok {
		(*counter).Add(v)
	}
}

func (m *Metrics) HistogramInit(name string, help string) {
	m.Init()
	histogram := promauto.NewHistogram(prometheus.HistogramOpts{
		Name: name,
		Help: help,
	})
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()
	m.histograms[name] = &histogram
}

func (m *Metrics) HistogramObserve(name string, v float64) {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()
	if histogram, ok := m.histograms[name]; ok {
		(*histogram).Observe(v)
	}
}
