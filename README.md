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
make dev_env_run
```

### Start Gecko

In a new tab or window we start a Gecko instance which acts as our gateway to the AVA network:

```shell script
ava --api-ipcs-enabled=true
```

### Start Ortelius

In a new tab or window we tell Gecko to publish events to an IPC socket and then start up the Ortelius apps to watch and handle those events:

```shell script
curl -X POST --data '{"jsonrpc": "2.0","method": "ipcs.publishBlockchain","params":{"blockchainID":"4R5p2RXDGLqaifZE4hHWH9owe34pfoBULn1DrQTWivjg8o4aH"},"id": 1}' -H 'content-type:application/json;' 127.0.0.1:9650/ext/ipcs
curl -X POST --data '{"jsonrpc": "2.0","method": "ipcs.publishBlockchain","params":{"blockchainID":"11111111111111111111111111111111LpoYY"},"id": 1}' -H 'content-type:application/json;' 127.0.0.1:9650/ext/ipcs

docker-compose -f docker/docker-compose.yml up
```
