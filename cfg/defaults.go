// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

const defaultJSON = `{
  "networkID": 1,
  "logDirectory": "/tmp/ortelius/logs/producer/avm",
  "chains": {},
  "services": {
    "api": {
			"listenAddr": ":8080"
		},
    "db": {
      "dsn": "root:password@tcp(127.0.0.1:3306)/ortelius_dev",
      "driver": "mysql"
    }
  },
  "stream": {
    "kafka": {
      "brokers": []
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
