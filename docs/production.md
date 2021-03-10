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
# cd ortelius
```

## start ortelius
```
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

## docker containers

There are 3 ortelius services, the api/producer/indexer.
The avalanchego node will be running.

```
# docker ps -a
CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS                      PORTS                                              NAMES
f9bd3c9d6f74   avaplatform/avalanchego:v1.2.0    "/bin/sh -cx 'exec .…"   19 minutes ago   Up 19 minutes               0.0.0.0:9650->9650/tcp                             production_avalanche_1
f5050fca06be   avaplatform/ortelius:140ac5c      "/opt/orteliusd stre…"   19 minutes ago   Up 19 minutes                                                                  production_producer_1
70c5b875c07d   avaplatform/ortelius:140ac5c      "/opt/orteliusd api …"   19 minutes ago   Up 19 minutes               0.0.0.0:8080->8080/tcp                             production_api_1
ee28fdea61c2   avaplatform/ortelius:140ac5c      "/opt/orteliusd stre…"   19 minutes ago   Up 19 minutes                                                                  production_indexer_1
06ed45c21615   redis:6.0.9-alpine3.12            "docker-entrypoint.s…"   19 minutes ago   Up 19 minutes               0.0.0.0:6379->6379/tcp                             production_redis_1
ae923d0489f0   mysql:8.0.23                      "docker-entrypoint.s…"   19 minutes ago   Up 19 minutes               0.0.0.0:3306->3306/tcp, 33060/tcp                  production_mysql_1
```

## stop ortelius

```
# make production_stop
```