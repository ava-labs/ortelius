// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"strings"

	"github.com/go-sql-driver/mysql"
)

const (
	DeadlockDBErrorMessage = "Deadlock found when trying to get lock; try restarting transaction"
	TimeoutDBErrorMessage  = "Lock wait timeout exceeded; try restarting transaction"
)

func ErrIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}

func ErrIsLockError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), DeadlockDBErrorMessage) ||
		strings.Contains(err.Error(), TimeoutDBErrorMessage))
}

func ForceParseTimeParam(dsn string) (string, error) {
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
