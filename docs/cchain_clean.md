# How to clean up c-chain

[docker cheet sheet](https://dockerlabs.collabnix.com/docker/cheatsheet/)

## stop avalanchego
## stop ortelius producers

## Find the mysql container
```
# docker ps -a 
CONTAINER ID   IMAGE                             COMMAND                  CREATED             STATUS             PORTS                                              NAMES
60eafd273f46   mysql:8.0.22                      "docker-entrypoint.s…"   About an hour ago   Up About an hour   0.0.0.0:3306->3306/tcp, 33060/tcp                  dev_env_mysql_1
...
```

Mysql is running on container-id=60eafd273f46 in this example.

## connect to mysql
```
# docker exec -i -t 60eafd273f46 /bin/bash
root@...:/# mysql -uroot -ppassword ortelius
mysql: [Warning] Using a password on the command line interface can be insecure.
...
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

## send mysql the following commands

Each command must be completed in order, a semicolon indicates the end of the command.

```
delete from avm_output_addresses where output_id in (
select id from avm_outputs where transaction_id in (
select id from avm_transactions where type in ('atomic_import_tx', 'atomic_export_tx')
)
union
select id from avm_outputs_redeeming where intx in (
select id from avm_transactions where type in ('atomic_import_tx', 'atomic_export_tx')
)
);

delete from avm_outputs where transaction_id in (
select id from avm_transactions where type in ('atomic_import_tx', 'atomic_export_tx')
);

delete from avm_outputs_redeeming where intx in (
select id from avm_transactions where type in ('atomic_import_tx', 'atomic_export_tx')
);

delete from avm_transactions where type in ('atomic_import_tx', 'atomic_export_tx');

delete from address_chain where address not in (
select address from avm_output_addresses
);

delete from cvm_addresses;
delete from cvm_blocks;
delete from cvm_transactions;
delete from cvm_transactions_txdata;
```

## Exit mysql, leave the docker container.

## Start the producer

## Start avalanchego

# For non standard setup the same delete command can be sent after you connect to your mysql instance.