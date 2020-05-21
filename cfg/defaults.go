// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

const defaultJSON = `{
  "networkID": 12345,
  "logDirectory": "/tmp/ortelius/logs/producer/avm",
  "chains": {
    "4ktRjsAKxgMr2aEzv9SWmrU7Xk5FniHUrVCX4P1TZSfTLZWFM": {
      "id": "4ktRjsAKxgMr2aEzv9SWmrU7Xk5FniHUrVCX4P1TZSfTLZWFM",
      "alias": "x",
      "vmType": "avm"
    }
  },
  "services": {
    "api": {
			"listenAddr": ":8080"
		},
    "db": {
      "dsn": "root:password@tcp(127.0.0.1:3306)/ortelius_dev",
      "driver": "mysql"
    },
    "redis": {
      "db": 0,
      "addr": "localhost:6379"
    }
  },
  "stream": {
    "kafka": {
      "brokers": [
        "127.0.0.1:29092"
      ]
    },
    "filter": {
      "min": 1073741824,
      "max": 2147483648
    },
    "producer": {
      "ipcRoot": "/tmp"
    },
    "consumer": {
      "groupName": "indexer"
    }
  }
}`
