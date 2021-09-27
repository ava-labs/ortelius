// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/ortelius/cfg"
)

type Connections struct {
	Eventer *EventRcvr
	db      *Conn
}

func NewDBFromConfig(conf cfg.Services, ro bool) (*Connections, error) {
	var (
		dbConn *Conn
		err    error
	)

	eventer := &EventRcvr{}

	if conf.DB != nil || conf.DB.Driver == DriverNone {
		// Create connection
		dbConn, err = New(eventer, *conf.DB, ro)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("invalid database")
	}

	return &Connections{
		db:      dbConn,
		Eventer: eventer,
	}, nil
}

func (c Connections) Stream() *EventRcvr { return c.Eventer }
func (c Connections) DB() *Conn          { return c.db }

func (c Connections) Close() error {
	errs := wrappers.Errs{}
	errs.Add(c.db.Close(context.Background()))
	return errs.Err
}
