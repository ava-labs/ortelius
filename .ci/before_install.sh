#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Install kafka lib
wget -qO - https://packages.confluent.io/deb/5.4/archive.key | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.4 stable main"
sudo apt-get update && sudo apt-get install -y librdkafka-dev

# Install linter
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.24.0
