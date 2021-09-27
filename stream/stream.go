// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"errors"
	"fmt"

	"github.com/ava-labs/ortelius/services"
)

var (
	ErrUnknownVM = errors.New("unknown VM")

	ErrInvalidTopicName    = errors.New("invalid topic name")
	ErrWrongTopicEventType = errors.New("wrong topic event type")
	ErrWrongTopicNetworkID = errors.New("wrong topic networkID")
)

const (
	EventTypeConsensus EventType = "consensus"
	EventTypeDecisions EventType = "decisions"
)

type EventType string

// Message is a message on the event stream
type Message struct {
	id         string
	chainID    string
	body       []byte
	timestamp  int64
	nanosecond int64
}

func (m *Message) ID() string        { return m.id }
func (m *Message) ChainID() string   { return m.chainID }
func (m *Message) Body() []byte      { return m.body }
func (m *Message) Timestamp() int64  { return m.timestamp }
func (m *Message) Nanosecond() int64 { return m.nanosecond }

func NewMessage(id string,
	chainID string,
	body []byte,
	timestamp int64,
	nanosecond int64,
) services.Consumable {
	return &Message{id: id, chainID: chainID, body: body, timestamp: timestamp, nanosecond: nanosecond}
}

func GetTopicName(networkID uint32, chainID string, eventType EventType) string {
	return fmt.Sprintf("%d-%s-%s", networkID, chainID, eventType)
}
