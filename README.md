# Ortelius

Ortelius stores and maps activity on the Avalanche network. It's primary features are:

- Safely persisting all blocks and transactions to Kafka, which acts as a durable log of all events that have occurred on network

- Indexing all the data in the persistent log into an easy-to-query dataset

- Providing an [HTTP API](docs/api.md) to access the indexed data

## Requirements

Building requires Go 1.13 or higher.

Running the full stack requires Avalanche.go, Kafka, MySQL or PostgreSQL, and Redis.

## Getting Started

### Download

```shell script
git checkout https://github.com/ava-labs/ortelius.git $GOPATH/github.com/ava-labs/ortelius
cd $GOPATH/github.com/ava-labs/ortelius
```

### Start required services

You can run a development service stack in your terminal:

```shell script
make dev_env_run &
make standalone_run
```

