// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/ortelius/services"
	"github.com/gorilla/mux"
)

type handlers struct {
	index services.Index
}

func newHandlers(index services.Index) handlers {
	return handlers{index}
}

func (h handlers) overview(w http.ResponseWriter, r *http.Request) {

}

func (h handlers) getTxByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]

	if idStr == "" {
		writeErr(w, 400, "ID is required")
		return
	}

	id, err := ids.FromString(idStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	tx, err := h.index.GetTx(id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeJSON(w, tx)
}

func (h handlers) getRecentTxs(w http.ResponseWriter, r *http.Request) {
	ids, err := h.index.GetRecentTxs(100)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	jsonBytes, err := json.Marshal(ids)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeJSON(w, jsonBytes)
}

func (h handlers) getTxCount(w http.ResponseWriter, r *http.Request) {
	count, err := h.index.GetTxCount()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	jsonBytes, err := json.Marshal(map[string]int64{"tx_count": count})
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeJSON(w, jsonBytes)
}

func writeJSON(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(200)
	fmt.Fprint(w, string(msg))
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	fmt.Fprint(w, msg)
}
