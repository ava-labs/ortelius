package avm

import (
	"encoding/json"

	"github.com/ava-labs/ortelius/utils"
)

// Filter the variables for filtering the data as pulled from the filter
type Filter struct {
	unparsed string
	Tx       *utils.BinFilter
}

// Initialize the filter
func (af *Filter) Initialize(unparsedJSON string) error {
	af.unparsed = unparsedJSON
	if err := json.Unmarshal([]byte(unparsedJSON), af.Tx); err != nil {
		return err
	}
	return nil
}

// Filter returns true if it should be filtered out
func (af *Filter) Filter(input []byte) bool {
	return af.Tx.Filter(input)
}
