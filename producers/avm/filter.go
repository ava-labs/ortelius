package avm

import (
	"fmt"

	"github.com/ava-labs/ortelius/utils"
)

// Filter the variables for filtering the data as pulled from the filter
type Filter struct {
	src   map[string]interface{} // good debug practice considering possible multiple filts in other filter structs
	filt1 utils.BinFilter
}

// Initialize the filter
func (af *Filter) Initialize(filt map[string]interface{}) error {
	af.filt1 = utils.BinFilter{}
	af.filt1.Initialize()
	fmt.Printf("unparsed %s", filt)
	if _, ok := filt["min"]; !ok {
		return fmt.Errorf("'min' not found in filter")
	}
	if _, ok := filt["max"]; !ok {
		return fmt.Errorf("'max' not found in filter")
	}
	af.filt1.Min = uint32(filt["min"].(float64))
	af.filt1.Max = uint32(filt["max"].(float64))
	fmt.Printf("filt: %d, %d\n", af.filt1.Min, af.filt1.Max)
	af.src = filt
	return nil
}

// Filter returns true if it should be filtered out
func (af *Filter) Filter(input []byte) bool {
	return af.filt1.Filter(input)
}
