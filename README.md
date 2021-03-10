# Ortelius

[![Build Status](https://travis-ci.com/ava-labs/ortelius.svg?branch=master)](https://travis-ci.com/ava-labs/ortelius)
[![Go Report Card](https://goreportcard.com/badge/github.com/ava-labs/ortelius)](https://goreportcard.com/report/github.com/ava-labs/ortelius)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

A data processing pipeline for the [Avalanche network](https://avax.network).

## Features

- Maintains a persistent log of all consensus events and decisions made on the Avalanche network.
- Indexes all Exchange (X) and Platform (P) chain transactions.
- Provides an API allowing easy exploration of the index.
- Engineered from the ground up with reliability at high scale in mind. Ortelius works with best-in-class backing services and is built to scale horizontally as the network grows.

## Quick Start with Standalone Mode

The easiest way to get started to is try out the Docker Compose-based standalone mode. Using the standalone backing services, such as MySQL, is not suitable for large production setups that need to scale out individual clusters, but it allows you to quickly get the pipeline up and working.

Ensure you have Docker and Docker Compose installed and then run:

```shell script
git clone https://github.com/ava-labs/ortelius.git $GOPATH/github.com/ava-labs/ortelius
cd $GOPATH/github.com/ava-labs/ortelius
make dev_env_start
make standalone_run
```

View the [API](https://docs.avax.network/build/tools/ortelius) for usage.

On the first run it will take time for avalanchego to bootstrap and ingest the historical data.

## Production Deployment

[production deployment](docs/deployment.md)
