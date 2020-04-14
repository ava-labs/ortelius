// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"github.com/gocraft/web"

	"github.com/ava-labs/gecko/ids"

	"github.com/ava-labs/ortelius/cfg"
)

func NewCVMRouter(_ *web.Router, _ cfg.ServiceConfig, _ uint32, _ ids.ID, _ string) error {
	return ErrUnimplemented
}
