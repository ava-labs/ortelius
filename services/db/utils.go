// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/ava-labs/ortelius/cfg"
)

func SanitizedDSN(cfg *cfg.DB) (string, error) {
	if cfg == nil || cfg.Driver != DriverMysql {
		return "", nil
	}

	dsn, err := mysql.ParseDSN(cfg.DSN)
	if err != nil {
		return "", err
	}
	dsn.Passwd = "[removed]"
	return dsn.FormatDSN(), nil
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
