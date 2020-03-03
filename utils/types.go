package utils

import (
	"encoding/json"

	"github.com/ava-labs/gecko/utils/formatting"
)

// Message holds a message
type Message struct {
	Key  []byte // TxID or BlockID
	Raw  []byte // Raw Tx or Block
	Data []byte // Marshalled JSON
}

// MessageJSON turns a message into JSON
type MessageJSON struct {
	Key  string                 `json:"key"`
	Raw  string                 `json:"raw"`
	Data map[string]interface{} `json:"data"`
}

func (m *Message) toJSON() ([]byte, error) {
	mjson := MessageJSON{}
	cb58 := formatting.CB58{}

	cb58.Bytes = m.Raw
	mjson.Raw = cb58.String()

	cb58.Bytes = m.Key
	mjson.Key = cb58.String()

	var data map[string]interface{}
	if err := json.Unmarshal(m.Data, &data); err != nil {
		return nil, err
	}

	mjson.Data = data

	return json.Marshal(mjson)

}
