## requirements

ubuntu 20.04+

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