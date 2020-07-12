#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Start a test env
make test_env_run &

# Lint
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.24.0
$GOPATH/bin/golangci-lint run --deadline=1m

# Run tests
go test -v ./...

# Ensure build works
go build cmds/orteliusd/*.go
