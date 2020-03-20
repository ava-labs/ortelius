#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

$GOPATH/bin/golangci-lint run --deadline=1m
go test -v ./...
