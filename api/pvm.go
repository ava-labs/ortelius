// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"errors"

	"github.com/gocraft/web"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/ortelius/cfg"
)

func NewPVMRouter(_ *web.Router, _ cfg.ServiceConfig, _ ids.ID, _ string, _ uint32) error {
	return errors.New("PVM not implemented")
}
