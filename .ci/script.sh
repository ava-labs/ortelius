#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Start a test env
make test_env_run &

# Lint
docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:v1.32.2 golangci-lint run -v --timeout=5m

# Wait for DB to be up and migrated
echo "Waiting for DB to be ready"
for i in `seq 1 60`;
do
  docker run --rm --network ortelius_services --entrypoint=mysql mysql:8.0.22 -uroot -ppassword -h mysql -e 'SELECT version FROM schema_migrations;' ortelius_test
  docker run --rm --network ortelius_services --entrypoint=mysql mysql:8.0.22 -uroot -ppassword -h mysql -e 'SELECT version FROM schema_migrations;' ortelius_test > /dev/null 2>&1 && break
  echo -n . && sleep 1
done

go version

# Run tests
go test -v ./...

# Ensure build works
go build cmds/orteliusd/*.go
