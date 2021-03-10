// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

const defaultJSON = `{
  "networkID": 1,
  "logDirectory": "/tmp/ortelius/logs",
  "listenAddr": ":8080",
  "chains": {},
  "services": {
    "db": {
      "dsn": "root:password@tcp(127.0.0.1:3306)/ortelius_dev",
      "driver": "mysql"
    }
  },
  "stream": {
    "kafka": {
      "brokers": []
    },
    "producer": {
      "ipcRoot": "/tmp"
    },
    "consumer": {
    }
  }
}`
