// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package record

import "errors"

const (
	// defaultSchema sets the schema to use for new records
	defaultSchema schemaID = rawGeckoCodecV1

	// rawGeckoCodecV1 is a raw binary blob from the initial Gecko Codec
	// format
	rawGeckoCodecV1 schemaID = iota
)

var (
	// ErrRecordTooShort is returned when trying to unmarshal a record that is too
	// short to be valid
	ErrRecordTooShort = errors.New("record is too short")

	// ErrUnknownSchema is returned when trying to unmarshal a record with an
	// unknown schema
	ErrUnknownSchema = errors.New("unknown schema")

	// schemas is a map of all schemas to their name
	schemas = map[schemaID]string{
		rawGeckoCodecV1: "rawgeckocodecv1",
	}
)

// schemaID is the identifier tag used to encode a schema in a Record
type schemaID int8

// Record is a slice of bytes representing a schema ID followed by a payload
type Record []byte

// Marshal creates a new Record with the default schema
func Marshal(b []byte) Record {
	return append(encodeSchemaID(defaultSchema), b...)
}

// Unmarshal parsers a Record, validates it against its schema, and returns its
// payload
func Unmarshal(b Record) ([]byte, error) {
	if len(b) < 1 {
		return nil, ErrRecordTooShort
	}

	if _, ok := schemas[decodeSchemaID(b[0])]; !ok {
		return nil, ErrUnknownSchema
	}

	return b[1:], nil
}

func decodeSchemaID(b byte) schemaID {
	return schemaID(b)
}

func encodeSchemaID(id schemaID) []byte {
	return []byte{byte(id)}
}
