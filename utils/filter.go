package utils

import "encoding/binary"

// Filter the filter data, calls appropriate filters
type Filter interface {
	Initialize(string)
	Filter([]byte) bool
}

// BinFilter if the binary object is between two values
type BinFilter struct {
	Min uint32 // minimum acceptable value
	Max uint32 // maximum acceptable value
}

// Filter returns false if tx hash is less than min or more than max
func (hf *BinFilter) Filter(input []byte) bool {
	b := input[:4]
	value := binary.LittleEndian.Uint32(b)
	if value < hf.Min || value > hf.Max {
		return false
	}
	return true
}
