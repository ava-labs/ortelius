# Ortelius Configuration

Configuration is done using a JSON file that can be used for each of the Ortelius applications. The configuration defines which network and blockchains Ortelius should index, as well as connection information for the required backing services.

# Example

This configuration is the one used by the standalone Docker Compose setup and illustrates the various available settings. `kafka`, `mysql`, and `redis` are DNS names that resolve to relevant service.
```json
{
  "networkID": 4,
  "logDirectory": "/var/log/ortelius",
  "ipcRoot": "ipc:///tmp",
  "listenAddr": "localhost:8080",
  "chains": {
    "jnUjZSRt16TcRnZzmh5aMhavwVHz3zBrSN8GfFMTQkzUnoBxC": {
      "id": "jnUjZSRt16TcRnZzmh5aMhavwVHz3zBrSN8GfFMTQkzUnoBxC",
      "alias": "X",
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