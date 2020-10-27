// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ErrorResponse represents an API error to return to the caller
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// WriteJSON writes the given bytes to the http response as JSON
func WriteJSON(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(200)
	w.Write(msg)
}

// WriteObject writes the given object to the http response as JSON
func WriteObject(w http.ResponseWriter, obj interface{}) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		WriteErr(w, 400, err.Error())
		return
	}
	WriteJSON(w, bytes)
}

// WriteErr writes the given error message to the http response
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
	w.Write(errBytes)
}
