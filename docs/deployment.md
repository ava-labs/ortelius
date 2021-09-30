# Requirements

ubuntu 18.04+

Minimum _recommended_ specs:

32GB memory + 8 cpu core

Expandable storage to accommodate growth or use an [external db](#external-db-setup).

It is safe to run the ortelius stack on multiple machines with a shared DB.

# Local folders/ports

Docker is configured to write data files into /var/lib/ortelius/

The api listens on port 8080.

Other ports could be exposed on the machine, and appropriate security should be setup to protect your machine.

# Docker

https://docs.docker.com/get-started/

# Setup docker (pre-steps)

https://docs.docker.com/engine/install/ubuntu/

https://docs.docker.com/compose/install/

## Non root docker configuration on linux (optional)
https://docs.docker.com/engine/install/linux-postinstall/

# Install necessary applications
```
# sudo apt install git make jq
```

# Clone the repo
```
# git clone https://github.com/ava-labs/ortelius
# cd ortelius
```

# Start ortelius
*note* docker requires root on linux [see](#non-root-docker-configuration-on-linux-optional)
```
# make production_start
```

# Upgrade ortelius (with a specific tag)
```
# make production_stop
```
```
git fetch --all
git checkout tags/{tag-id}
```
*note* [ortelius tags](https://github.com/ava-labs/ortelius/tags)
```
# make production_start
```

# Test api connectivity
```
# curl 'localhost:8080/' | jq "."
{
  "network_id": 1,
  "chains": {
    "11111111111111111111111111111111LpoYY": {
      "chainID": "11111111111111111111111111111111LpoYY",
      "chainAlias": "p",
      "vm": "pvm",
      "avaxAssetID": "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z",
      "networkID": 1
    },
    "2oYMBNV4eNHyqk2fjjV5nVQLDbtmNJzq5s3qs3Lo6ftnC6FByM": {
      "chainID": "2oYMBNV4eNHyqk2fjjV5nVQLDbtmNJzq5s3qs3Lo6ftnC6FByM",
      "chainAlias": "x",
      "vm": "avm",
      "avaxAssetID": "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z",
      "networkID": 1
    }
  }
}

```

# Test the status of the transaction pool.

{container-id} of the mysql container can be found using `docker ps -a`

```
# docker exec -i -t {container-id}  mysql -uroot -ppassword ortelius -e "select topic,processed,count(*) from tx_pool group by topic,processed"
+---------------------------------------------------+-----------+----------+
| topic                                             | processed | count(*) |
+---------------------------------------------------+-----------+----------+
| 1-11111111111111111111111111111111LpoYY-consensus |         0 |     8607 |
| 1-11111111111111111111111111111111LpoYY-consensus |         1 |   207057 |
| 1-11111111111111111111111111111111LpoYY-decisions |         0 |   120708 |
| 1-11111111111111111111111111111111LpoYY-decisions |         1 |    94998 |
+---------------------------------------------------+-----------+----------+
```

| Processed | Description |
| --- | --- |
| 0 | unprocessed transactions |
| 1 | processed transactions|

As items are consumed into the indexer the count of processed = 0 transactions decreases.
*NOTE* It can take a while for tx_pool records to appear on a fresh install.

# Docker containers

There are 2 ortelius services api and indexer.
There will be an avalanchego, and mysql container.

```
# docker ps -a
CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS                      PORTS                                              NAMES
f9bd3c9d6f74   avaplatform/avalanchego:v1.2.3    "/bin/sh -cx 'exec .…"   19 minutes ago   Up 19 minutes               0.0.0.0:9650->9650/tcp                             production_avalanche_1
70c5b875c07d   avaplatform/ortelius:140ac5c      "/opt/orteliusd api …"   19 minutes ago   Up 19 minutes               0.0.0.0:8080->8080/tcp                             production_api_1
ee28fdea61c2   avaplatform/ortelius:140ac5c      "/opt/orteliusd stre…"   19 minutes ago   Up 19 minutes                                                                  production_indexer_1
ae923d0489f0   mysql:8.0.26                      "docker-entrypoint.s…"   19 minutes ago   Up 19 minutes               0.0.0.0:3306->3306/tcp, 33060/tcp                  production_mysql_1
```

# Stop ortelius

```
# make production_stop
```

# Viewing logs

```
# make production_logs
```

# External DB setup

Ortelius requires an updated mysql compatible DB.  This will work with aurora in mysql mode.

*note* DB needs to be up and migrated before use.

## *optional* mysql docker container -- adjust as necessary
[dockerhub mysql](https://hub.docker.com/_/mysql)
```
docker run --volume .../github.com/ava-labs/ortelius/docker/my.cnf:/etc/mysql/my.cnf --network host -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=ortelius mysql:8.0.26
```
The standard mysql defaults can cause issues, please configure the customizations [here](https://github.com/ava-labs/ortelius/blob/master/docker/my.cnf)

## run migrations -- *required* for all ortelius updates.
[dockerhub migrate](https://hub.docker.com/r/migrate/migrate)
```
docker run --volume .../github.com/ava-labs/ortelius/services/db/migrations:/migrations --network host "migrate/migrate:v4.14.1"  -path=/migrations/ -database "mysql://root:password@tcp(mysql:3306)/ortelius" up
```
Update the docker params as needed to match the user/password/host/database as appropriate.

## update ortelius configs
Update [config](https://github.com/ava-labs/ortelius/blob/master/docker/config.json) with the correct dsn in your local github repo.


# Customized setup

The full Ortelius pipeline requires the following services. This guide will not cover their installation but will discuss key configuration settings.

- **[Avalanche.go](https://github.com/ava-labs/avalanchego)** is the gateway to the Avalanche network
- **[MySQL](https://www.mysql.com/)** powers the index

## Configuring services

### Avalanche.go

The IPCs for the chains you want to consume must be available. This can be done by starting the Avalanche.go process with the `--index-enabled` flag.

see:
[avalanchego configs](https://docs.avax.network/build/references/command-line-interface)

[avalanchego chain configs](https://docs.avax.network/build/references/command-line-interface#chain-configs)

### MySQL

The indexer requires that a MySQL compatible database be available. The migrations can be found in the repo's [services/db/migrations](../services/db/migrations) directory and can be applied with [golang-migrate](https://github.com/golang-migrate/migrate), example:

`migrate -source file://services/db/migrations -database "mysql://root:password@tcp(127.0.0.1:3306)/ortelius" up`

[external db setup](#external-db-setup)

## Ortelius Distribution

Ortelius can be built from source into a single binary or a Docker image. A public Docker image is also available on [Docker Hub](https://hub.docker.com/repository/docker/avaplatform/ortelius).

Example: `docker run --rm avaplatform/ortelius --help`

## Configuring Ortelius

[Configuration for Ortelius](https://github.com/ava-labs/ortelius/blob/master/docker/config.json).

## Running Ortelius

Ortelius is a collection of services. The full stack consists of the Indexer, and API which can all be started from the single binary:

```
ortelius stream indexer -c path/to/config.json
ortelius api -c path/to/config.json
```

As Avalanche.go bootstraps the Producer will send all events to DB, the indexer will index, and the API will make them available. 
You can test your setup [API](https://docs.avax.network/build/tools/ortelius). 

# Ortelius re-indexing

If you performed a standard install the avalanchego database will be located at: /var/lib/ortelius/avalanche/mainnet/.

```
$ ls -altr /var/lib/ortelius/avalanche/mainnet/
total 12
drwxr-xr-x 3 root root 4096 Mar 10 14:29 ..
drwxr-xr-x 3 root root 4096 Mar 10 14:29 .
drwxr-xr-x 2 root root 4096 Mar 10 15:01 v1.4.5
```

## Steps

Stop [ortelius](#stop-ortelius)

Remove the directory /var/lib/ortelius/avalanche/mainnet/

Restart [ortelius](#start-ortelius).


