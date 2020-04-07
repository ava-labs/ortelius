// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"fmt"
	"net/http"
)

func writeJSON(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(200)
	fmt.Fprint(w, string(msg))
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	fmt.Fprint(w, msg)
}
