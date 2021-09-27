// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()
	if _, ok := m.counters[name]; ok {
		return
	}
	counter := promauto.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})
	m.counters[name] = &counter
}

func (m *Metrics) CounterInc(name string) error {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()
	if counter, ok := m.counters[name]; ok {
		(*counter).Inc()
		return nil
	}
	return fmt.Errorf("metric not found: %s", name)
}

func (m *Metrics) CounterAdd(name string, v float64) error {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()
	if counter, ok := m.counters[name]; ok {
		(*counter).Add(v)
		return nil
	}
	return fmt.Errorf("metric not found: %s", name)
}

func (m *Metrics) HistogramInit(name string, help string, buckets []float64) {
	m.Init()
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()
	if _, ok := m.histograms[name]; ok {
		return
	}
	histogram := promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	})
	m.histograms[name] = &histogram
}

func (m *Metrics) HistogramObserve(name string, v float64) error {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()
	if histogram, ok := m.histograms[name]; ok {
		(*histogram).Observe(v)
		return nil
	}
	return fmt.Errorf("metric not found: %s", name)
}

type Collector interface {
	Error()
	Collect() error
}

// Increment to keySuccess or keyFail.
type successFailCounterInc struct {
	success    bool
	keySuccess string
	keyFail    string
}

func NewSuccessFailCounterInc(keySuccess string, keyFail string) Collector {
	c := successFailCounterInc{
		success:    true,
		keySuccess: keySuccess,
		keyFail:    keyFail,
	}
	return &c
}

func (hc *successFailCounterInc) Error() {
	hc.success = false
}

func (hc *successFailCounterInc) Collect() error {
	if hc.success {
		return Prometheus.CounterInc(hc.keySuccess)
	}
	return Prometheus.CounterInc(hc.keyFail)
}

// Add count to keySuccess or keyFail
type successFailCounterAdd struct {
	success    bool
	keySuccess string
	keyFail    string
	count      float64
}

func NewSuccessFailCounterAdd(keySuccess string, keyFail string, count float64) Collector {
	c := successFailCounterAdd{
		success:    true,
		keySuccess: keySuccess,
		keyFail:    keyFail,
		count:      count,
	}
	return &c
}

func (hc *successFailCounterAdd) Error() {
	hc.success = false
}

func (hc *successFailCounterAdd) Collect() error {
	if hc.success {
		return Prometheus.CounterAdd(hc.keySuccess, hc.count)
	}
	return Prometheus.CounterAdd(hc.keyFail, hc.count)
}

// a histogram observer.  Observers time delta.
type histogramObserveMillis struct {
	collect bool
	timeNow time.Time
	key     string
}

func NewHistogramCollect(key string) Collector {
	hc := histogramObserveMillis{
		collect: true,
		timeNow: time.Now(),
		key:     key,
	}
	return &hc
}

func (hc *histogramObserveMillis) Error() {
	hc.collect = false
}

func (hc *histogramObserveMillis) Collect() error {
	if !hc.collect {
		return nil
	}
	return Prometheus.HistogramObserve(hc.key, float64(time.Since(hc.timeNow).Milliseconds()))
}

// a counter incrementer.  Increments by delta time
type counterObserveMillisCollect struct {
	collect bool
	timeNow time.Time
	key     string
}

func NewCounterObserveMillisCollect(key string) Collector {
	c := counterObserveMillisCollect{
		collect: true,
		timeNow: time.Now(),
		key:     key,
	}
	return &c
}

func (c *counterObserveMillisCollect) Error() {
	c.collect = false
}

func (c *counterObserveMillisCollect) Collect() error {
	if !c.collect {
		return nil
	}
	return Prometheus.CounterAdd(c.key, float64(time.Since(c.timeNow).Milliseconds()))
}

// a counter incrementer.  Increments by 1
type counterIncCollect struct {
	collect bool
	key     string
}

func NewCounterIncCollect(key string) Collector {
	hc := counterIncCollect{
		collect: true,
		key:     key,
	}
	return &hc
}

func (hc *counterIncCollect) Error() {
	hc.collect = false
}

func (hc *counterIncCollect) Collect() error {
	if !hc.collect {
		return nil
	}
	return Prometheus.CounterInc(hc.key)
}

// A list of collectors
type collectorsContainer struct {
	collectors []Collector
}

func NewCollectors(collectors ...Collector) Collector {
	cs := collectorsContainer{
		collectors: make([]Collector, 0, len(collectors)),
	}
	cs.collectors = append(cs.collectors, collectors...)
	return &cs
}

func (cs *collectorsContainer) Error() {
	for _, c := range cs.collectors {
		c.Error()
	}
}

func (cs *collectorsContainer) Collect() error {
	var err error
	for _, c := range cs.collectors {
		errc := c.Collect()
		if errc != nil {
			err = errc
		}
	}
	return err
}
