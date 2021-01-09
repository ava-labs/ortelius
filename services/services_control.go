package services

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

const (
	MetricProduceProcessedCountKey = "produce_records_processed"
	MetricProduceSuccessCountKey   = "produce_records_success"
	MetricProduceFailureCountKey   = "produce_records_failure"

	MetricConsumeProcessedCountKey       = "consume_records_processed"
	MetricConsumeProcessMillisCounterKey = "consume_records_process_millis"
	MetricConsumeSuccessCountKey         = "consume_records_success"
	MetricConsumeFailureCountKey         = "consume_records_failure"

	TopicCheckInteval  = 10 * time.Second
	TopicOffsetTimeout = 10 * time.Second
)

type TopicGroup struct {
	Topic string
	Group string
}

func (tg *TopicGroup) String() string {
	return fmt.Sprintf("%s:%s", tg.Topic, tg.Group)
}

type TopicGroupPart struct {
	TopicGroup TopicGroup
	Partition  int
}

func (tg *TopicGroupPart) String() string {
	return fmt.Sprintf("%s:%d", tg.TopicGroup, tg.Partition)
}

type Offsets struct {
	Offset       *int64
	CommitOffset *int64
}

type Control struct {
	Services           cfg.Services
	Kafka              cfg.Kafka
	Log                logging.Logger `json:"log"`
	dbLock             sync.Mutex
	connections        *Connections
	connectionsRO      *Connections
	Persist            Persist
	topicLock          sync.RWMutex
	topicOnce          sync.Once
	topicGroups        map[string]TopicGroup
	topicGroupPartLock sync.RWMutex
	topicGroupParts    map[string]*Offsets
}

func (s *Control) Init() {
	s.topicLock.Lock()
	if s.topicGroups == nil {
		s.topicGroups = make(map[string]TopicGroup)
	}
	s.topicLock.Unlock()

	s.topicGroupPartLock.Lock()
	if s.topicGroupParts == nil {
		s.topicGroupParts = make(map[string]*Offsets)
	}
	s.topicGroupPartLock.Unlock()
}

func (s *Control) TopicMonitor(tg TopicGroup) {
	s.topicLock.Lock()
	s.topicGroups[tg.String()] = tg
	s.topicLock.Unlock()

	s.topicOnce.Do(func() {
		go func() {
			tick := time.NewTicker(TopicCheckInteval)

			for range tick.C {
				topicGroups := []TopicGroup{}
				s.topicLock.RLock()
				for _, topicGroup := range s.topicGroups {
					topicGroups = append(topicGroups, topicGroup)
				}
				s.topicLock.RUnlock()

				addr, err := net.ResolveTCPAddr("tcp", s.Kafka.Brokers[0])
				if err != nil {
					s.Log.Error("connect broker %s %v", s.Kafka.Brokers[0], err)
					return
				}
				for _, topicGroup := range topicGroups {
					tu := NewTopicUtil(addr, time.Duration(0), topicGroup.Topic)
					s.tc(tu, topicGroup)
				}

				s.TgC()
			}
		}()
	})
}

func (s *Control) TgC() {
	s.topicGroupPartLock.RLock()
	defer s.topicGroupPartLock.RUnlock()
	for k, v := range s.topicGroupParts {
		off := atomic.LoadInt64(v.Offset)
		coff := atomic.LoadInt64(v.CommitOffset)
		if off != coff {
			s.Log.Error("offset diff %s %d %d", k, off, coff)
		}
	}
}

func (s *Control) tc(tu TopicUtil, topicGroup TopicGroup) {
	ctx, cancelFn := context.WithTimeout(context.Background(), TopicOffsetTimeout)
	defer cancelFn()
	resp, err := tu.CommittedOffets(ctx, topicGroup.Group)
	if err != nil {
		s.Log.Error("req offset %s %s %v", s.Kafka.Brokers[0], topicGroup, err)
		return
	}
	if resp.Error != nil {
		s.Log.Error("resp offset %s %s %v", s.Kafka.Brokers[0], topicGroup, resp.Error)
		return
	}
	for topic, offsetParts := range resp.Topics {
		for _, offsetPart := range offsetParts {
			if offsetPart.Error != nil {
				s.Log.Error("resp offset %s %s %s %v", s.Kafka.Brokers[0], topicGroup, topic, offsetPart.Error)
				continue
			}
			s.Log.Info("topic: %s %s %d %d %s", topicGroup, topic, offsetPart.Partition, offsetPart.CommittedOffset, offsetPart.Metadata)

			tgp := TopicGroupPart{TopicGroup: topicGroup, Partition: offsetPart.Partition}
			s.UpdCom(&tgp, offsetPart.CommittedOffset)
		}
	}
}

func (s *Control) InitProduceMetrics() {
	metrics.Prometheus.CounterInit(MetricProduceProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(MetricProduceSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(MetricProduceFailureCountKey, "records failure")
}

func (s *Control) InitConsumeMetrics() {
	metrics.Prometheus.CounterInit(MetricConsumeProcessedCountKey, "records processed")
	metrics.Prometheus.CounterInit(MetricConsumeProcessMillisCounterKey, "records processed millis")
	metrics.Prometheus.CounterInit(MetricConsumeSuccessCountKey, "records success")
	metrics.Prometheus.CounterInit(MetricConsumeFailureCountKey, "records failure")
}

func (s *Control) Database() (*Connections, error) {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()
	if s.connections != nil {
		return s.connections, nil
	}
	c, err := NewConnectionsFromConfig(s.Services, false)
	if err != nil {
		return nil, err
	}
	s.connections = c
	s.connections.DB().SetMaxIdleConns(32)
	s.connections.DB().SetConnMaxIdleTime(5 * time.Minute)
	s.connections.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, err
}

func (s *Control) DatabaseRO() (*Connections, error) {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()
	if s.connectionsRO != nil {
		return s.connectionsRO, nil
	}
	c, err := NewConnectionsFromConfig(s.Services, true)
	if err != nil {
		return nil, err
	}
	s.connectionsRO = c
	s.connectionsRO.DB().SetMaxIdleConns(32)
	s.connectionsRO.DB().SetConnMaxIdleTime(5 * time.Minute)
	s.connectionsRO.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, err
}

func (s *Control) TopicMessage(topicGroup string, message *kafka.Message) {
	tgp := TopicGroupPart{TopicGroup: TopicGroup{Topic: message.Topic, Group: topicGroup}, Partition: message.Partition}
	s.UpdOff(&tgp, message.Offset)
}

func (s *Control) UpdOff(tgp *TopicGroupPart, offset int64) {
	tpgs := tgp.String()
	ok := false
	s.topicGroupPartLock.RLock()
	if x, present := s.topicGroupParts[tpgs]; present {
		atomic.StoreInt64(x.Offset, offset)
		ok = true
	}
	s.topicGroupPartLock.RUnlock()
	if ok {
		return
	}
	s.topicGroupPartLock.Lock()
	defer s.topicGroupPartLock.Unlock()

	if x, present := s.topicGroupParts[tpgs]; present {
		atomic.StoreInt64(x.Offset, offset)
		return
	}
	offsets := &Offsets{
		Offset:       new(int64),
		CommitOffset: new(int64),
	}
	atomic.StoreInt64(offsets.Offset, offset)
	atomic.StoreInt64(offsets.CommitOffset, offset)
	s.topicGroupParts[tpgs] = offsets
}

func (s *Control) UpdCom(tgp *TopicGroupPart, commitoffset int64) {
	tpgs := tgp.String()
	ok := false
	s.topicGroupPartLock.RLock()
	if x, present := s.topicGroupParts[tpgs]; present {
		atomic.StoreInt64(x.CommitOffset, commitoffset)
		ok = true
	}
	s.topicGroupPartLock.RUnlock()
	if ok {
		return
	}
	s.topicGroupPartLock.Lock()
	defer s.topicGroupPartLock.Unlock()

	if x, present := s.topicGroupParts[tpgs]; present {
		atomic.StoreInt64(x.CommitOffset, commitoffset)
		return
	}
	offsets := &Offsets{
		Offset:       new(int64),
		CommitOffset: new(int64),
	}
	atomic.StoreInt64(offsets.Offset, commitoffset)
	atomic.StoreInt64(offsets.CommitOffset, commitoffset)
	s.topicGroupParts[tpgs] = offsets
}
