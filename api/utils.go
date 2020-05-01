// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func WriteJSON(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(200)
	fmt.Fprint(w, string(msg))
}

func WriteObject(w http.ResponseWriter, obj interface{}) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		WriteErr(w, 400, err.Error())
		return
	}
	WriteJSON(w, bytes)
}

func WriteErr(w http.ResponseWriter, code int, msg string) {
	errBytes, err := json.Marshal(&ErrorResponse{
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
