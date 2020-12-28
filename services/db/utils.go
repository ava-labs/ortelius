// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/ava-labs/ortelius/cfg"
)

const (
	RemovedPassword = "[removed]"

	DuplicateDBErrorMessage = "Deadlock found when trying to get lock; try restarting transaction"
)

func SanitizedDSN(cfg *cfg.DB) (string, string, error) {
	if cfg == nil || cfg.Driver != DriverMysql {
		return "", "", nil
	}

	dsn, err := mysql.ParseDSN(cfg.DSN)
	if err != nil {
		return "", "", err
	}
	dsn.Passwd = RemovedPassword
	rodsn, err := mysql.ParseDSN(cfg.RODSN)
	if err != nil {
		return "", "", err
	}
	rodsn.Passwd = RemovedPassword
	return dsn.FormatDSN(), rodsn.FormatDSN(), nil
}

func ErrIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}

func forceParseTimeParam(dsn string) (string, error) {
	// Parse dsn into a url
	u, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	if u.Params == nil {
		u.Params = make(map[string]string)
	}
	u.Params["parseTime"] = "true"

	// Re-encode as a string
	return u.FormatDSN(), nil
}
