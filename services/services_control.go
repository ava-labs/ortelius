package services

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

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

type Control struct {
	Services      cfg.Services
	Kafka         cfg.Kafka
	Log           logging.Logger `json:"log"`
	dbLock        sync.Mutex
	connections   *Connections
	connectionsRO *Connections
	Persist       Persist
	topicLock     sync.RWMutex
	topicOnce     sync.Once
	topicGroups   map[string]TopicGroup
}

func (s *Control) TopicMonitor(tg TopicGroup) {
	s.topicLock.Lock()
	if s.topicGroups == nil {
		s.topicGroups = make(map[string]TopicGroup)
	}
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
					topicUtil := NewTopicUtil(addr, time.Duration(0), topicGroup.Topic)
					ctx, cancelFn := context.WithTimeout(context.Background(), TopicOffsetTimeout)
					defer cancelFn()
					resp, err := topicUtil.CommittedOffets(ctx, topicGroup.Group)
					if err != nil {
						s.Log.Error("req offset %s %s %v", s.Kafka.Brokers[0], topicGroup, err)
						continue
					}
					if resp.Error != nil {
						s.Log.Error("resp offset %s %s %v", s.Kafka.Brokers[0], topicGroup, resp.Error)
						continue
					}
					for topic, offsetParts := range resp.Topics {
						for _, offsetPart := range offsetParts {
							if offsetPart.Error != nil {
								s.Log.Error("resp offset %s %s %s %v", s.Kafka.Brokers[0], topicGroup, topic, offsetPart.Error)
								continue
							}
							s.Log.Info("topic: %s %s %d %d %s", topicGroup, topic, offsetPart.Partition, offsetPart.CommittedOffset, offsetPart.Metadata)
						}
					}
				}
			}
		}()
	})
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
