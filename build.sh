#!/bin/bash

# Playing with a local build
# Extracted from Dockerfile, Makefile

go get ./cmds/...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/orteliusd ./cmds/orteliusd/*.go
$GOPATH/bin/migrate -source file://./services/db/migrations -database 'mysql://root:password@tcp(127.0.0.1:3306)/ortelius' up
