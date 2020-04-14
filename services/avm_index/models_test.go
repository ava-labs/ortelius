// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm_index

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/gecko/ids"
)

func TestDisplayTxJSONMarshaling(t *testing.T) {
	expectedJSON := `{"credentials":[{"signatures":[[138,237,176,77,16,113,106,27,89,147,143,0,255,86,100,58,25,112,234,229,177,16,86,251,176,149,102,44,140,4,142,80,49,251,9,126,166,201,131,192,202,144,221,229,145,164,226,250,208,16,19,84,54,121,39,68,109,44,21,31,172,101,143,22,1]]}],"id":"11111111111111111111111111111111LpoYY","timestamp":"1970-01-01T00:00:42Z","unsignedTx":{"blockchainID":"4ktRjsAKxgMr2aEzv9SWmrU7Xk5FniHUrVCX4P1TZSfTLZWFM","inputs":[{"Symbol":false,"assetID":"21d7KVtPrubc5fHr6CGNcgbUb4seUjmZKr35ZX7BZb5iP8pXWA","input":{"amount":2448,"signatureIndices":[0]},"outputIndex":1,"txID":"Xaywx8ZyA9Psir1CCtX3YHNLojQYqRUueTu8GzExqJPGqdjeE"}],"networkID":2,"outputs":[{"assetID":"21d7KVtPrubc5fHr6CGNcgbUb4seUjmZKr35ZX7BZb5iP8pXWA","output":{"addresses":["6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV"],"amount":50,"locktime":0,"threshold":1}},{"assetID":"21d7KVtPrubc5fHr6CGNcgbUb4seUjmZKr35ZX7BZb5iP8pXWA","output":{"addresses":["6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV"],"amount":2398,"locktime":0,"threshold":1}}]}}`
	dt := &displayTx{
		RawMessage: json.RawMessage(`{"unsignedTx":{"networkID":2,"blockchainID":"4ktRjsAKxgMr2aEzv9SWmrU7Xk5FniHUrVCX4P1TZSfTLZWFM","outputs":[{"assetID":"21d7KVtPrubc5fHr6CGNcgbUb4seUjmZKr35ZX7BZb5iP8pXWA","output":{"amount":50,"locktime":0,"threshold":1,"addresses":["6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV"]}},{"assetID":"21d7KVtPrubc5fHr6CGNcgbUb4seUjmZKr35ZX7BZb5iP8pXWA","output":{"amount":2398,"locktime":0,"threshold":1,"addresses":["6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV"]}}],"inputs":[{"txID":"Xaywx8ZyA9Psir1CCtX3YHNLojQYqRUueTu8GzExqJPGqdjeE","outputIndex":1,"Symbol":false,"assetID":"21d7KVtPrubc5fHr6CGNcgbUb4seUjmZKr35ZX7BZb5iP8pXWA","input":{"amount":2448,"signatureIndices":[0]}}]},"credentials":[{"signatures":[[138,237,176,77,16,113,106,27,89,147,143,0,255,86,100,58,25,112,234,229,177,16,86,251,176,149,102,44,140,4,142,80,49,251,9,126,166,201,131,192,202,144,221,229,145,164,226,250,208,16,19,84,54,121,39,68,109,44,21,31,172,101,143,22,1]]}]}`),
		Timestamp:  time.Unix(42, 0),
		ID:         ids.Empty.Bytes(),
	}

	dtJSON, err := json.Marshal(dt)
	if err != nil {
		t.Fatal("Failed to marshal displayTx:", err.Error())
	}

	if string(dtJSON) != expectedJSON {
		fmt.Println(expectedJSON)
		fmt.Println(string(dtJSON))
		t.Fatal("Marshalled displayTx was incorrect:", string(dtJSON))
	}
}
