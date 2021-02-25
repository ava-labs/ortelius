## requirements

ubuntu 20.04+

Minimum recommended specs:

32GB memory + 8 cpu core

Expandable storage to accommodate growth.

### As root

## setup docker

https://docs.docker.com/engine/install/ubuntu/
https://docs.docker.com/compose/install/

## install necessary applications
```
# apt install git make jq
```

## clone the repo
```
# git clone https://github.com/ava-labs/ortelius
```

## start ortelius
```
# cd ortelius
# make production_start
```

## test api connectivity
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


## test the status of the transaction pool.

{container-id} of the mysql container can be found using `docker ps -a`

```
# docker exec -i -t {container-id}}  mysql -uroot -ppassword ortelius -e "select processed,count(*) from tx_pool group by processed"
mysql: [Warning] Using a password on the command line interface can be insecure.
+-----------+----------+
| processed | count(*) |
+-----------+----------+
|         0 |   167188 |
+-----------+----------+

```

## docker containers

There are 3 ortelius services, the api/producer/indexer.
The avalanchego node will be running.
There is a kafka, zookeeper, redis, and mysql service running.

```
# docker ps -a
CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS                      PORTS                                              NAMES
f9bd3c9d6f74   avaplatform/avalanchego:v1.2.0    "/bin/sh -cx 'exec .…"   19 minutes ago   Up 19 minutes               0.0.0.0:9650->9650/tcp                             production_avalanche_1
f5050fca06be   avaplatform/ortelius:140ac5c      "/opt/orteliusd stre…"   19 minutes ago   Up 19 minutes                                                                  production_producer_1
70c5b875c07d   avaplatform/ortelius:140ac5c      "/opt/orteliusd api …"   19 minutes ago   Up 19 minutes               0.0.0.0:8080->8080/tcp                             production_api_1
ee28fdea61c2   avaplatform/ortelius:140ac5c      "/opt/orteliusd stre…"   19 minutes ago   Up 19 minutes                                                                  production_indexer_1
1af896483d18   confluentinc/cp-kafka:5.4.3       "/etc/confluent/dock…"   19 minutes ago   Up 19 minutes               0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp   production_kafka_1
06ed45c21615   redis:6.0.9-alpine3.12            "docker-entrypoint.s…"   19 minutes ago   Up 19 minutes               0.0.0.0:6379->6379/tcp                             production_redis_1
ae923d0489f0   mysql:8.0.22                      "docker-entrypoint.s…"   19 minutes ago   Up 19 minutes               0.0.0.0:3306->3306/tcp, 33060/tcp                  production_mysql_1
13ef9052f18b   confluentinc/cp-zookeeper:5.4.3   "/etc/confluent/dock…"   19 minutes ago   Up 19 minutes               2888/tcp, 3888/tcp, 0.0.0.0:49153->2181/tcp        production_zookeeper_1
```