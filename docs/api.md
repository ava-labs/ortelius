# Ortelius API

## X Chain API

| Name                                                                        | Route                                    |
|---------------------------                                                 | ----------------------------------------|
| [Search](#search---xsearch)                                                 | /x/search                                |
| [List Transactions](#list-transactions---xtransactions)                     | /x/transactions                          |
| [Get Transaction](#get-transaction---xtransactionsid)                       | /x/transactions/:id                      |
| [Aggregate Transactions](#aggregate-transactions---xaggregatetransactions) | /x/transactions/aggregate                 |
| [List Assets](#list-assets---xassets)                                       | /x/assets                                |
| [Get Asset](#get-asset---xassetsalias_or_id)                                | /x/assets/:alias_or_id                   |
| [List Addresses](#list-addresses---xaddresses)                              | /x/addresses                             |
| [Get Address](#get-address---xaddressesid)                                  | /x/addresses/:id                         |

### Search - /x/search

Searches for an indexed item based on it's ID or keywords.

NOTE: Currenly only IDs are supported.

Params:

`query` (Required) - The term(s) to search for

#### Response:

```json
{
  "count": 1,
  "results": [
    {
      "type": "transaction",
      "data": {}
    }
  ]
}
```

### List Transactions - /x/transactions

#### Params:

`sort` - The sorting method to use. Options: timestamp-asc, timestamp-desc. Default: timestamp-asc

#### Response:

Array of transaction objects

### Get Transaction - /x/transactions/:id

#### Params:

`id` - The ID of the transaction to get

#### Response

The transaction object

### Aggregate Transactions - /x/transactions/aggregates

#### Params:

`startTime` - The Time to start calculating from. Defaults to the Time of the first known transaction. Valid values are unix timestamps (in seconds) or RFC3339 datetime strings.

`endTime` - The Time to end calculating to. Defaults to the current Time. Valid values are unix timestamps (in seconds) or RFC3339 datetime strings.

`intervalSize` - If given, a list of intervals of the given size from startTime to endTime will be returned, with the aggregates for each interval. Valid values are `minute`, `hour`, `day`, `week`, `month`, `year`, or a valid Go duration string as described here: https://golang.org/pkg/Time/#ParseDuration 

#### Response:

```json
{
    "startTime": "2019-11-01T00:00:00Z",
    "endTime": "2020-04-23T20:28:21.358567Z",
    "aggregates": {
        "transactionCount": 23,
        "transactionVolume": 719999999992757400,
        "outputCount": 1,
        "addressCount": 16,
        "assetCount": 3
    },
    "intervalSize": 2592000000000000,
    "intervals": [
        {
            "transactionCount": 22,
            "transactionVolume": 719999999992757399,
            "outputCount": 0,
            "addressCount": 15,
            "assetCount": 4
        },
        {
            "transactionCount": 1,
            "transactionVolume": 1,
            "outputCount": 0,
            "addressCount": 1,
            "assetCount": 1
        }
    ]
}
```

### List Assets - /x/assets

#### Response:

Array of asset objects

### Get Asset - /x/assets/:alias_or_id

#### Params:

`alias_or_id` - The alias or ID of the asset to get

#### Response:

Array of asset objects

### List Addresses - /x/addresses

#### Params:

<pagination params>

#### Response:

Array of Address objects

```json
[
  {
    "address": "2poot6VNEurx99o5WZigk2ic3ssj2T5Fz",
    "publicKey": null,
    "transactionCount": 2,
    "balance": 0,
    "lifetimeValue": 26000,
    "utxoCount": 0
  },
  {
    "address": "6cesTteH62Y5mLoDBUASaBvCXuL2AthL",
    "publicKey": null,
    "transactionCount": 186,
    "balance": 0,
    "lifetimeValue": 8369999998480180000,
    "utxoCount": 0
  }
]
```

### Get Address - /x/addresses/:address

#### Params:

`address` - The base58-encoded Address to show.

#### Response:

```json
{
  "address": "6Y3kysjF9jnHnYkdS9yGAuoHyae2eNmeV",
  "publicKey": null,
  "transactionCount": 1,
  "balance": 0,
  "lifetimeValue": 45000000000000000,
  "utxoCount": 0
}
```
## P Chain API (Not yet implemented)

## C Chain API (Not yet implemented)
