// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"

	"github.com/ava-labs/gecko/ids"
)

// Processor handles writing and reading to/from the event stream
type Processor interface {
	ProcessNextMessage(context.Context) (*Message, error)
	Close() error
}

// Message is a message on the event stream
type Message struct {
	id        ids.ID
	chainID   ids.ID
	body      []byte
	timestamp uint64
}

func (m *Message) ID() ids.ID        { return m.id }
func (m *Message) ChainID() ids.ID   { return m.chainID }
func (m *Message) Body() []byte      { return m.body }
func (m *Message) Timestamp() uint64 { return m.timestamp }
