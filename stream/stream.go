// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"errors"
)

var (
	ErrUnknownVM = errors.New("Unknown VM")
)

// Message is a message on the event stream
type Message struct {
	id        string
	chainID   string
	body      []byte
	timestamp int64
}

func (m *Message) ID() string       { return m.id }
func (m *Message) ChainID() string  { return m.chainID }
func (m *Message) Body() []byte     { return m.body }
func (m *Message) Timestamp() int64 { return m.timestamp }
