// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func writeJSON(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(200)
	fmt.Fprint(w, string(msg))
}

func writeObject(w http.ResponseWriter, obj interface{}) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	writeJSON(w, bytes)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	errBytes, err := json.Marshal(&struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{
		Code:    code,
		Message: msg,
	})
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprint(w, `{"code": 500, "message": "failed to generate correct error message"}`)
	}

	w.WriteHeader(code)
	fmt.Fprint(w, string(errBytes))
}
