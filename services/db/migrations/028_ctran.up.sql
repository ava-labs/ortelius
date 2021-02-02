create table `cvm_transactions_txdata`
(
    id             varchar(50)     not null primary key,
    hash           varchar(100)    not null,
    block          decimal(65)     not null,
    serialization  mediumblob,
    created_at                     timestamp       not null default current_timestamp
);

create index cvm_transactions_txdata_hash ON cvm_transactions_txdata (hash);
create index cvm_transactions_txdata_block ON cvm_transactions_txdata (block);
