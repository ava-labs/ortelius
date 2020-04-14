#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Start a test env
make test_env_run &

# Install deps
go get ./...
go get github.com/alicebob/miniredis
go get -tags 'mysql' github.com/golang-migrate/migrate/cmd/migrate

# Lint
$GOPATH/bin/golangci-lint run --deadline=1m

# Build tests
go test -i ./...

# Migrate DB
$GOPATH/bin/migrate \
  -source "file://services/db/migrations" \
  -database "mysql://root:password@tcp(127.0.0.1:3306)/ortelius_test" \
  up

# Run tests
go test -v ./...
