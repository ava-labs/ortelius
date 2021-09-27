// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gocraft/web"
)

// ErrorResponse represents an API error to return to the caller
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// WriteJSON writes the given bytes to the http response as JSON
func WriteJSON(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(200)
	_, _ = w.Write(msg)
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
	_, _ = w.Write(errBytes)
}

func ParseGetJSON(r *web.Request, n int64) (url.Values, error) {
	if r == nil {
		return r.URL.Query(), nil
	}
	pf := r.Body
	if pf == nil {
		return r.URL.Query(), nil
	}
	buf := new(strings.Builder)
	_, err := io.CopyN(buf, pf, n)
	switch err {
	case io.EOF:
	case nil:
	default:
		return r.URL.Query(), err
	}
	if buf.Len() > 0 {
		s := buf.String()
		qdata := make(map[string][]string)
		err := json.Unmarshal([]byte(s), &qdata)
		switch err {
		case nil:
			return mergeValues(qdata, r.URL.Query()), nil
		default:
			return r.URL.Query(), err
		}
	}
	return r.URL.Query(), nil
}

func mergeValues(a url.Values, b url.Values) url.Values {
	for k, v := range b {
		if _, present := a[k]; present {
			a[k] = append(a[k], v...)
		} else {
			a[k] = append([]string{}, v...)
		}
	}
	return a
}
