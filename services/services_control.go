package services

import (
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

type Control struct {
	Services      cfg.Services
	Log           logging.Logger `json:"log"`
	lock          sync.Mutex
	connections   *Connections
	connectionsRO *Connections
}

func (s *Control) Database() (*Connections, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.connections != nil {
		return s.connections, nil
	}
	c, err := NewConnectionsFromConfig(s.Log, s.Services, false)
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
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.connectionsRO != nil {
		return s.connectionsRO, nil
	}
	c, err := NewConnectionsFromConfig(s.Log, s.Services, true)
	if err != nil {
		return nil, err
	}
	s.connectionsRO = c
	s.connectionsRO.DB().SetMaxIdleConns(32)
	s.connectionsRO.DB().SetConnMaxIdleTime(5 * time.Minute)
	s.connectionsRO.DB().SetConnMaxLifetime(5 * time.Minute)
	return c, err
}
