package avm

import "github.com/ava-labs/gecko/utils/logging"

// AVM consumes for the AVM
type AVM struct{}

// Initialize inits the consumer
func (c *AVM) Initialize(logging.Logger) error { return nil }

// Close closes the consumer
func (c *AVM) Close() {}

// ReadMessage consumers a message
func (c *AVM) ReadMessage(msg []byte) error { return nil }
