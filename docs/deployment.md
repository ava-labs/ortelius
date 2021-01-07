# Ortelius Deployment

This page details the various services and components that make up the Ortelius stack and how they can be configured.

## Required Services

The full Ortelius pipeline requires the following services. This guide will not cover their installation but will discuss key configuration settings.

- **[Avalanche.go](https://github.com/ava-labs/avalanchego)** is the gateway to the Avalanche network
- **[Apache Kafka](https://kafka.apache.org/)** stores the persistent log of events
- **[MySQL](https://www.mysql.com/)** or **[PostgreSQL](https://www.postgresql.org/)** powers the index
- **[Redis](https://redis.io/)** caches index queries for the API so responses can be as fast as possible

## Configuring services

### Avalanche.go

The IPCs for the chains you want to consume must be available. This can be done by starting the Avalanche.go process with the `--ipcs-chain-ids` flag, example:

`./build/avalanchego --ipcs-chain-ids=11111111111111111111111111111111LpoYY,jnUjZSRt16TcRnZzmh5aMhavwVHz3zBrSN8GfFMTQkzUnoBxC`

### Kafka

Kafka should be configured to retain messages forever and to deduplicate messages over time. Because Kafka can't be set to keep messages forever we set it to keep them for 100 years.

```
cleanup.policy=compact
delete.retention.ms=3154000000000
```

### MySQL/PostgreSQL

The indexer requires that a MySQL or Postgres compatible database be available. The migrations can be found in the repo's [services/db/migrations](../services/db/migrations) directory and can be applied with [golang-migrate](https://github.com/golang-migrate/migrate), example:

`migrate -source file://services/db/migrations -database "mysql://root:password@tcp(127.0.0.1:3306)/ortelius" up`

## Ortelius Distribution

Ortelius can be built from source into a single binary or a Docker image. A public Docker image is also available on [Docker Hub](https://hub.docker.com/repository/docker/avaplatform/ortelius).

Example: `docker run --rm avaplatform/ortelius --help`

## Configuring Ortelius

Configuration for Ortelius itself described [here](https://docs.avax.network/build/tools/ortelius#ortelius-configuration).

## Running Ortelius

Ortelius is a collection of services. The full stack consists of the Producer, Indexer, and API which can all be started from the single binary:

```
ortelius stream producer -c path/to/config.json
ortelius stream indexer -c path/to/config.json
ortelius api -c path/to/config.json
```

These processes are separated so they can scale independently as needed. They can start on a single machine or small cluster and be scaled out as the system grows.

As Avalanche.go bootstraps the Producer will send all events to Kafka, the indexer will write them to the RDMS database, and the API will made them available.  You can test your setup [API](https://docs.avax.network/build/tools/ortelius). 
