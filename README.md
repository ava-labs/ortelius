# Ortelius

[![Build Status](https://travis-ci.com/ava-labs/ortelius.svg?branch=master)](https://travis-ci.com/ava-labs/ortelius)
[![Go Report Card](https://goreportcard.com/badge/github.com/ava-labs/ortelius)](https://goreportcard.com/report/github.com/ava-labs/ortelius)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

A data processing pipeline for the [Avalanche network](https://avax.network).

## Features

- Maintains a persistent log of all consensus events and decisions made on the Avalanche network.
- Indexes Exchange (X), Platform (P), and Contract (C) chain transactions.
- Provides an API allowing easy exploration of the index.

## Quick Start with Standalone Mode

The easiest way to get started to is try out the standalone mode on the Fuji (testnet) network.

Ensure you have Docker and Docker Compose installed:

https://docs.docker.com/engine/install/ubuntu/

https://docs.docker.com/compose/install/

Then run:

```shell script
git clone https://github.com/ava-labs/ortelius.git $GOPATH/github.com/ava-labs/ortelius
cd $GOPATH/github.com/ava-labs/ortelius
make dev_env_start
make standalone_run
```

View the [API](https://docs.avax.network/build/tools/ortelius) for usage.

## Production Deployment

[production deployment](docs/deployment.md)
