# Ortelius Configuration

Configuration is done using a JSON file that can be used for each of the Ortelius applications. The configuration defines which network and blockchains Ortelius should index, as well as connection information for the required backing services.

# Example

This configuration is the one used by the standalone Docker Compose setup and illustrates the various available settings. `kafka`, `mysql`, and `redis` are DNS names that resolve to relevant service.
```json
{
  "networkID": 5,
  "logDirectory": "/var/log/ortelius",
  "ipcRoot": "ipc:///tmp",
  "listenAddr": "localhost:8080",
  "chains": {
    "11111111111111111111111111111111LpoYY": {
      "id": "11111111111111111111111111111111LpoYY",
      "alias": "p",
      "vmType": "pvm"
    },
    "2JVSBoinj9C2J33VntvzYtVJNZdN2NKiwwKjcumHUWEb5DbBrm": {
      "id": "2JVSBoinj9C2J33VntvzYtVJNZdN2NKiwwKjcumHUWEb5DbBrm",
      "alias": "x",
      "vmType": "avm"
    }
  },
  "stream": {
    "kafka": {
      "brokers": [
        "kafka:9092"
      ],
      "groupName": "indexer"
    }
  },
  "services": {
    "redis": {
      "addr": "redis:6379"
    },
    "db": {
      "dsn": "root:password@tcp(mysql:3306)/ortelius",
      "driver": "mysql"
    }
  }
}
```