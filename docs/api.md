# Ortelius API

## X Chain API

| Name                      | Route                                  |
|---------------------------|----------------------------------------|
| [Search](#search---xsearch)                    | /x/search                              |
| [List Transactions](#list-transactions---xtransactions)         | /x/transactions                        |
| [Get Transaction](#get-transaction---xtransactionsid)           | /x/transactions/:id                    |
| [Aggregate Transactions](#aggregate-transactions---xtransactionsaggregates)    | /x/transactions/aggregates             |
| [List Assets](#list-assets---xassets)               | /x/assets                              |
| [Get Asset](#get-asset---xassetsalias_or_id)                 | /x/assets/:alias_or_id                 |
| [List Address Transactions](#list-address-transactions---xaddressesaddrtransactions) | /x/addresses/:addr/transactions        |
| [List Address Outputs](#list-address-outputs---xaddressesaddrtransaction_outputsspenttrue)      | /x/addresses/:addr/transaction_outputs |

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

`startTime` - The time to start calculating from. Defaults to the time of the first known transaction. Valid values are unix timestamps (in seconds) or RFC3339 datetime strings.

`endTime` - The time to end calculating to. Defaults to the current time. Valid values are unix timestamps (in seconds) or RFC3339 datetime strings.

`intervalSize` - If given, a list of intervals of the given size from startTime to endTime will be returned, with the aggregates for each interval. Valid values are `minute`, `hour`, `day`, `week`, `month`, `year`, or a valid Go duration string as described here: https://golang.org/pkg/time/#ParseDuration 

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

### List Address Transactions - /x/addresses/:addr/transactions

#### Params:

`addr` - The base58-encoded address to show transactions for.

#### Response:

Array of transaction objects

### List Address Outputs - /x/addresses/:addr/transaction_outputs?spent=true

#### Params:

`addr` - The base58-encoded address to show transactions for.

`spent` - Boolean. If supplied it will filter outputs to either spent or unspent.

#### Response:

Array of transaction output objects

## P Chain API (Not yet implemented)

## C Chain API (Not yet implemented)
